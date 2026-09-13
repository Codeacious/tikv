// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! UDP sidechannel to the in-path P4 switch — the in-repo half of the "assist"
//! read mode. Ported closely from etcd's
//! `server/etcdserver/api/rafthttp/udp_sidechannel.go`.
//!
//! **Flow 1 — write acceleration (leader <-> switch).** Outgoing `MsgAppend` /
//! `MsgAskAckIndex` are mirrored to the switch by
//! [`UdpSidechannel::process_outgoing_message`] (a tap in `RaftClient::send`).
//! The switch reflects a `MsgAskAckIndexResp` carrying its saved index (the
//! highest *proposed* append index it has observed for the region); we rebuild a
//! `RaftMessage` from a cached routing template and feed it to the local raft
//! group, which consumes it as a fast ack hint.
//!
//! **Flow 2 — follower read gate (client -> switch -> follower).** A client
//! reading from this follower also fires a `MsgReadIndex` UDP packet at this
//! store's `:7700` listener; the switch tags its `value` with the region's saved
//! index in flight. The listener resolves the request's read-gate marker via the
//! shared [`SwitchReadGate`] (defined in `raftstore`, where the read path can
//! reach it). Marker-less reads are gated via the batched query loop instead.
//!
//! Wire format: fixed 43-byte UDP payload
//! ```text
//!   off 0  size 2  magic     (u16 BE, default 0xFEED)
//!   off 2  size 1  MessageType (raft-rs eraftpb::MessageType enum value)
//!   off 3  size 8  to        (store id, BE)
//!   off 11 size 8  from      (store id, BE)
//!   off 19 size 8  marker    (BE; per-request read-gate marker, or batch id)
//!   off 27 size 8  value     (BE; proposed/ack index; 0 is reserved as
//!                             "no saved index" and never served on)
//!   off 35 size 8  region_id (BE; the raft region this message concerns)
//! ```
//! The switch indexes its ack table by `region_id % table_size`, so every packet
//! carries its region and both flows are region-scoped. Encode/decode stays in
//! one [`encode`]/[`decode`] pair.

use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Sender};
use kvproto::{metapb, raft_serverpb::RaftMessage};
use raft::eraftpb::MessageType;
use raftstore::store::SwitchReadGate;
use tikv_kv::RaftExtension;

/// Process-global sidechannel handle. Set once in the store bootstrap when
/// `read_mode == "assist"`; read by the outgoing tap (`RaftClient::send`), the
/// flush hooks, and peer discovery (`KvService::raft`/`batch_raft`). `None` in
/// every other mode, so those hot paths stay untouched.
static GLOBAL_UDP_SIDECHANNEL: OnceLock<Arc<UdpSidechannel>> = OnceLock::new();

/// Publishes the process-global sidechannel handle. Idempotent.
pub fn set_global_udp_sidechannel(sc: Arc<UdpSidechannel>) {
    let _ = GLOBAL_UDP_SIDECHANNEL.set(sc);
}

/// Returns the process-global sidechannel handle, or `None` if disabled.
pub fn global_udp_sidechannel() -> Option<&'static Arc<UdpSidechannel>> {
    GLOBAL_UDP_SIDECHANNEL.get()
}

/// Builds the sidechannel from config, publishing both it and its read gate as
/// process-globals. Returns `None` if the listen socket can't be bound (logged).
pub fn init_global(
    store_id: u64,
    ip: &str,
    port: u16,
    magic: u16,
    feeder: Box<dyn RaftMessageFeeder>,
) -> Option<Arc<UdpSidechannel>> {
    let (gate, gate_rx) = SwitchReadGate::new();
    let sc = UdpSidechannel::new(store_id, ip, port, magic, feeder, gate, gate_rx)?;
    set_global_udp_sidechannel(sc.clone());
    Some(sc)
}

/// Default IP the UDP sidechannel listens on.
pub const DEFAULT_UDP_SIDECHANNEL_IP: &str = "0.0.0.0";
/// Default UDP sidechannel port.
pub const DEFAULT_UDP_SIDECHANNEL_PORT: u16 = 7700;
/// Default 16-bit magic in the sidechannel header.
pub const DEFAULT_UDP_SIDECHANNEL_MAGIC: u16 = 0xFEED;
/// gRPC raft-stream metadata key advertising this store's sidechannel port.
pub const RAFT_SIDECHANNEL_PORT_KEY: &str = "raft-sidechannel-port";
/// gRPC raft-stream metadata key advertising this store's sidechannel magic (hex).
pub const RAFT_SIDECHANNEL_MAGIC_KEY: &str = "raft-sidechannel-magic";
/// Reads the client-minted per-request read-gate marker off a request's
/// `kvrpcpb::Context`, where it rides `Context::txn_source` — a repurposed
/// carrier, NOT that field's real meaning (TiKV reads it only in the txn
/// prewrite path, `storage/txn/commands/prewrite.rs`, never on any read path).
/// Keep in sync with client-go's `rawkv.SetReadGateMarker`. `0` = no client
/// marker; the serve gate then mints a server-side one.
pub fn read_gate_marker_from_ctx(ctx: &kvproto::kvrpcpb::Context) -> u64 {
    ctx.get_txn_source()
}
/// After the first pending marker arrives, wait this long to batch more before
/// issuing a single local switch query (mirrors etcd's `ReadGateAskTimeoutMillis`).
const READ_GATE_ASK_TIMEOUT: Duration = Duration::from_millis(3);
/// If a marker is still pending this long after its last switch query, re-issue
/// it. The query or its reflected `MsgAskAckIndexResp` can be dropped, or no peer
/// may have been attached when it first fired; a server-minted marker has no
/// other resolution path, so without the retry the gated read never resolves.
const READ_GATE_QUERY_RETRY: Duration = Duration::from_millis(250);
/// Read timeout on the sockets so the blocking loops periodically observe the
/// stop flag.
const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(200);
/// Depth of the out channel drained by the send loop. On overflow the switch
/// hint is dropped (best-effort) rather than blocking raft.
const OUT_CHANNEL_DEPTH: usize = 128;
/// The 43-byte fixed wire payload length (35-byte base + 8-byte region_id).
const WIRE_LEN: usize = 43;

/// Feeds a reconstructed inbound `RaftMessage` into the local raft group.
/// Implemented by any `RaftExtension`; boxed so the sidechannel stays
/// non-generic, and held behind a `Mutex` because `RaftExtension` is `Send` but
/// not `Sync`. Feeds from the listener / per-peer read-loop threads serialize on
/// it; feeding is a non-blocking router enqueue, off the write critical path.
pub trait RaftMessageFeeder: Send + 'static {
    fn feed_raft_message(&self, msg: RaftMessage);
}

impl<R: RaftExtension + Send + 'static> RaftMessageFeeder for R {
    fn feed_raft_message(&self, msg: RaftMessage) {
        self.feed(msg, false);
    }
}

/// Routing template captured at tap time, used to reconstruct the inbound
/// `RaftMessage` for a switch-reflected `MsgAskAckIndexResp`. `self_peer` is the
/// local (leader) peer; `remote_peer` is the follower we sent to. The reflected
/// response is injected as if the follower sent it (from/to swapped).
#[derive(Clone)]
struct RaftRoute {
    region_id: u64,
    self_peer: metapb::Peer,
    remote_peer: metapb::Peer,
}

/// Per-peer sidechannel state, keyed by store id. Refcounted because multiple
/// raft gRPC streams can exist per peer (`grpc_raft_conn_num`).
struct PeerEntry {
    store_id: u64,
    remote: SocketAddr,
    magic: u16,
    /// Connected socket dialed to `remote`; sends via it skip the per-call FIB
    /// lookup an unconnected send pays. `None` if the dial failed (callers fall
    /// back to the listener socket). Its `peer_read_loop` consumes the switch's
    /// reflected responses, which return to this socket's ephemeral source port.
    conn: Option<Arc<UdpSocket>>,
    refcount: usize,
    /// Per-region tap state for this peer: the dedupe watermark and the switch
    /// liveness flag. Region-keyed because a store hosts many regions.
    regions: Arc<Mutex<HashMap<u64, RegionTapState>>>,
    /// Latest routing template seen on a tap to this peer, per region, used to
    /// reconstruct the reflected `MsgAskAckIndexResp` for the right region.
    route: Arc<Mutex<HashMap<u64, RaftRoute>>>,
    /// Stops this peer's `peer_read_loop`.
    reader_stop: Arc<AtomicBool>,
    reader_handle: Option<JoinHandle<()>>,
}

/// Per-(peer, region) tap state.
#[derive(Default)]
struct RegionTapState {
    /// Highest `MsgAppend` proposed index emitted to the switch for this
    /// (peer, region). Flow-1's dedupe watermark, and the value the idle refresh
    /// re-sends.
    last_sent_index: u64,
    /// Set by any nonzero switch reflection naming this (peer, region); cleared
    /// by each `refresh_region` pass. A flag rather than a timestamp, so the
    /// receive path pays one store and no clock read.
    switch_updated: bool,
}

/// A message queued for the send loop.
struct OutMsg {
    bytes: [u8; WIRE_LEN],
    conn: Option<Arc<UdpSocket>>,
    remote: SocketAddr,
    store_id: u64,
}

pub struct UdpSidechannel {
    store_id: u64,
    magic: u16,
    listener: Arc<UdpSocket>,
    out_tx: Sender<OutMsg>,
    out_dropped: AtomicU64,
    /// Count of switch answers carrying the reserved invalid index 0, for the
    /// rate-limited warning in `warn_zero_switch_index`.
    zero_stamps: AtomicU64,
    peers: Mutex<HashMap<u64, PeerEntry>>,
    gate: Arc<SwitchReadGate>,
    feeder: Mutex<Box<dyn RaftMessageFeeder>>,
    stopped: Arc<AtomicBool>,
    loop_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl raftstore::store::SwitchRegisterRefresher for UdpSidechannel {
    fn refresh_region(&self, region_id: u64) {
        UdpSidechannel::refresh_region(self, region_id)
    }
}

/// Encodes a 43-byte sidechannel payload. The single wire encoder.
fn encode(
    magic: u16,
    msg_type: MessageType,
    to: u64,
    from: u64,
    marker: u64,
    value: u64,
    region_id: u64,
) -> [u8; WIRE_LEN] {
    let mut buf = [0u8; WIRE_LEN];
    buf[0..2].copy_from_slice(&magic.to_be_bytes());
    buf[2] = msg_type as i32 as u8;
    buf[3..11].copy_from_slice(&to.to_be_bytes());
    buf[11..19].copy_from_slice(&from.to_be_bytes());
    buf[19..27].copy_from_slice(&marker.to_be_bytes());
    buf[27..35].copy_from_slice(&value.to_be_bytes());
    buf[35..43].copy_from_slice(&region_id.to_be_bytes());
    buf
}

struct Decoded {
    magic: u16,
    msg_type: i32,
    // `to` (the destination store) is parsed for wire completeness but the
    // receiver is always this store; routing uses `from`.
    #[allow(dead_code)]
    to: u64,
    from: u64,
    marker: u64,
    value: u64,
    region_id: u64,
}

/// Decodes a 43-byte sidechannel payload. The single wire decoder.
fn decode(buf: &[u8]) -> Decoded {
    Decoded {
        magic: u16::from_be_bytes(buf[0..2].try_into().unwrap()),
        msg_type: buf[2] as i32,
        to: u64::from_be_bytes(buf[3..11].try_into().unwrap()),
        from: u64::from_be_bytes(buf[11..19].try_into().unwrap()),
        marker: u64::from_be_bytes(buf[19..27].try_into().unwrap()),
        value: u64::from_be_bytes(buf[27..35].try_into().unwrap()),
        region_id: u64::from_be_bytes(buf[35..43].try_into().unwrap()),
    }
}

impl UdpSidechannel {
    /// Creates the sidechannel (binds the `:port` listener) and spawns the
    /// listener, send, and batched-query loops. Publishes the shared read gate
    /// as the raftstore process-global so the read path can reach it. Returns
    /// `None` if the listen socket can't be bound.
    pub fn new(
        store_id: u64,
        ip: &str,
        port: u16,
        magic: u16,
        feeder: Box<dyn RaftMessageFeeder>,
        gate: Arc<SwitchReadGate>,
        gate_rx: Receiver<(u64, u64)>,
    ) -> Option<Arc<UdpSidechannel>> {
        let magic = if magic == 0 {
            DEFAULT_UDP_SIDECHANNEL_MAGIC
        } else {
            magic
        };
        let listen_addr: SocketAddr = match format!("{}:{}", ip, port).parse() {
            Ok(a) => a,
            Err(e) => {
                warn!("failed to parse UDP sidechannel listen addr"; "ip" => ip, "port" => port, "err" => ?e);
                return None;
            }
        };
        let listener = match UdpSocket::bind(listen_addr) {
            Ok(s) => s,
            Err(e) => {
                warn!("failed to bind UDP sidechannel listener"; "addr" => %listen_addr, "err" => ?e);
                return None;
            }
        };
        if let Err(e) = listener.set_read_timeout(Some(SOCKET_READ_TIMEOUT)) {
            warn!("failed to set UDP sidechannel read timeout"; "err" => ?e);
        }

        let (out_tx, out_rx) = bounded(OUT_CHANNEL_DEPTH);
        let sc = Arc::new(UdpSidechannel {
            store_id,
            magic,
            listener: Arc::new(listener),
            out_tx,
            out_dropped: AtomicU64::new(0),
            zero_stamps: AtomicU64::new(0),
            peers: Mutex::new(HashMap::new()),
            gate,
            feeder: Mutex::new(feeder),
            stopped: Arc::new(AtomicBool::new(false)),
            loop_handles: Mutex::new(Vec::new()),
        });

        let mut handles = Vec::new();
        {
            let sc = sc.clone();
            handles.push(
                thread::Builder::new()
                    .name("udp-sc-listen".to_owned())
                    .spawn(move || sc.read_loop())
                    .unwrap(),
            );
        }
        {
            let sc = sc.clone();
            handles.push(
                thread::Builder::new()
                    .name("udp-sc-send".to_owned())
                    .spawn(move || sc.send_loop(out_rx))
                    .unwrap(),
            );
        }
        {
            let sc = sc.clone();
            handles.push(
                thread::Builder::new()
                    .name("udp-sc-query".to_owned())
                    .spawn(move || sc.query_switch_index_loop(gate_rx))
                    .unwrap(),
            );
        }
        *sc.loop_handles.lock().unwrap() = handles;

        // Publish the shared gate so raftstore's read path (propose hint + serve
        // gate) can reach it, and the refresher so its leader-side tick can.
        raftstore::store::set_global_switch_read_gate(sc.gate.clone());
        raftstore::store::set_global_switch_register_refresher(sc.clone());

        info!("UDP sidechannel started"; "store_id" => store_id, "addr" => %listen_addr, "magic" => format!("{:04x}", magic));
        Some(sc)
    }

    pub fn magic(&self) -> u16 {
        self.magic
    }

    pub fn magic_as_str(&self) -> String {
        format!("{:04x}", self.magic)
    }

    /// The port the listener is bound to (advertised to peers via metadata).
    pub fn listen_port(&self) -> u16 {
        self.listener
            .local_addr()
            .map(|a| a.port())
            .unwrap_or(DEFAULT_UDP_SIDECHANNEL_PORT)
    }

    /// Parses a magic value from its hex string metadata form.
    pub fn magic_from_str(s: &str) -> Option<u16> {
        u16::from_str_radix(s, 16).ok()
    }

    /// Latest switch-confirmed quorum ack index for `region_id`. Currently
    /// unused: the propose-time hint reads the gate directly from `Peer`.
    pub fn latest_switch_index(&self, region_id: u64) -> u64 {
        self.gate.latest_switch_index(region_id)
    }

    /// Cadence hook kept for symmetry with `RaftClient::flush` /
    /// `ServerTransport::flush`. The dedicated send loop drains the out channel
    /// continuously, so there is nothing to flush; this is a no-op.
    pub fn flush(&self) {}

    /// Flow-1 tap. Mirrors an outgoing `MsgAppend` / `MsgAskAckIndex` to the
    /// switch (other types are ignored). `MsgAppend`s that don't advance the
    /// proposed index for the peer are deduped away. Never blocks raft: the
    /// send is a non-blocking enqueue, dropped on a full channel.
    pub fn process_outgoing_message(&self, msg: &RaftMessage) {
        let inner = msg.get_message();
        let msg_type = inner.get_msg_type();
        if msg_type != MessageType::MsgAppend && msg_type != MessageType::MsgAskAckIndex {
            return;
        }

        let region_id = msg.get_region_id();
        let to_store = msg.get_to_peer().get_store_id();
        let peers = self.peers.lock().unwrap();
        let peer = match peers.get(&to_store) {
            Some(p) => p,
            None => return,
        };

        // Refresh this region's routing template so a later reflected response
        // can be injected as if it came from this follower.
        peer.route.lock().unwrap().insert(
            region_id,
            RaftRoute {
                region_id,
                self_peer: msg.get_from_peer().clone(),
                remote_peer: msg.get_to_peer().clone(),
            },
        );

        let (conn, remote, regions, magic, store_id) = (
            peer.conn.clone(),
            peer.remote,
            peer.regions.clone(),
            peer.magic,
            peer.store_id,
        );
        drop(peers);

        let marker = 0u64;
        let bytes = match msg_type {
            MessageType::MsgAppend => {
                let proposed_index = inner.get_index() + inner.get_entries().len() as u64;
                // The switch only needs the latest proposed index per
                // (peer, region); skip MsgAppends that don't advance it.
                {
                    let mut regions = regions.lock().unwrap();
                    let state = regions.entry(region_id).or_default();
                    if proposed_index <= state.last_sent_index {
                        return;
                    }
                    state.last_sent_index = proposed_index;
                }
                encode(magic, MessageType::MsgAppend, store_id, self.store_id, marker, proposed_index, region_id)
            }
            MessageType::MsgAskAckIndex => {
                encode(magic, MessageType::MsgAskAckIndex, store_id, self.store_id, marker, 0, region_id)
            }
            _ => unreachable!(),
        };

        let label = if msg_type == MessageType::MsgAppend {
            "MsgAppend"
        } else {
            "MsgAskAckIndex"
        };
        raftstore::store::metrics::SIDECHANNEL_MSG_SENT_TOTAL
            .with_label_values(&[label])
            .inc();
        self.enqueue(OutMsg {
            bytes,
            conn,
            remote,
            store_id,
        });
    }

    /// Re-sends, to every peer whose switch has said nothing about `region_id`
    /// since the previous call, the highest proposed index already sent that
    /// peer for that region. Driven by raftstore's leader-only
    /// `PeerTick::SwitchRegisterRefresh` (see `SwitchRegisterRefresher`).
    ///
    /// Per-peer because each peer's path can cross a different switch, each
    /// holding its own register; `last_sent_index` rather than the commit index
    /// because it is `>=` every value that switch reflected to us.
    fn refresh_region(&self, region_id: u64) {
        // Collect under the peer lock, send after releasing it: `enqueue` can
        // contend with the send loop, and this runs on the raftstore poller.
        let mut to_send: Vec<OutMsg> = Vec::new();
        {
            let peers = self.peers.lock().unwrap();
            for peer in peers.values() {
                let last_sent = {
                    let mut regions = peer.regions.lock().unwrap();
                    let state = match regions.get_mut(&region_id) {
                        // Nothing ever tapped for this (peer, region); nothing
                        // to restore.
                        None => continue,
                        Some(s) => s,
                    };
                    // Swap-and-test: a real answer since the last pass means
                    // the path is live. The refresh's own reflection sets the
                    // flag, so a fully idle (peer, region) is refreshed every
                    // OTHER tick.
                    if std::mem::replace(&mut state.switch_updated, false) {
                        continue;
                    }
                    state.last_sent_index
                };
                if last_sent == 0 {
                    continue;
                }
                // Built directly, not through `process_outgoing_message`: that
                // path's dedupe only emits on a strictly higher index, so it
                // would refuse this retransmission.
                to_send.push(OutMsg {
                    bytes: encode(
                        peer.magic,
                        MessageType::MsgAppend,
                        peer.store_id,
                        self.store_id,
                        0,
                        last_sent,
                        region_id,
                    ),
                    conn: peer.conn.clone(),
                    remote: peer.remote,
                    store_id: peer.store_id,
                });
            }
        }
        if to_send.is_empty() {
            return;
        }
        raftstore::store::metrics::SIDECHANNEL_MSG_SENT_TOTAL
            .with_label_values(&["SwitchRegisterRefresh"])
            .inc_by(to_send.len() as u64);
        for msg in to_send {
            self.enqueue(msg);
        }
    }

    /// Non-blocking enqueue to the send loop. Drops (and counts) on a full
    /// channel rather than stalling raft.
    fn enqueue(&self, msg: OutMsg) {
        if self.out_tx.try_send(msg).is_err() {
            let n = self.out_dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 128 == 1 {
                warn!("UDP sidechannel out channel full, dropping switch hint"; "total_dropped" => n);
            }
        }
    }

    fn send_loop(&self, out_rx: Receiver<OutMsg>) {
        while !self.stopped.load(Ordering::Relaxed) {
            match out_rx.recv_timeout(SOCKET_READ_TIMEOUT) {
                Ok(msg) => {
                    let res = match &msg.conn {
                        Some(conn) => conn.send(&msg.bytes),
                        None => self.listener.send_to(&msg.bytes, msg.remote),
                    };
                    if let Err(e) = res {
                        warn!("failed to send UDP sidechannel message"; "store_id" => msg.store_id, "err" => ?e);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// The `:7700` listener loop. Handles inbound client `MsgReadIndex`
    /// (switch-tagged) and any `MsgAskAckIndexResp` that lands here.
    fn read_loop(&self) {
        let mut buf = [0u8; 9000];
        while !self.stopped.load(Ordering::Relaxed) {
            match self.listener.recv_from(&mut buf) {
                Ok((n, _)) => {
                    if n < WIRE_LEN {
                        warn!("received UDP sidechannel message with invalid length"; "length" => n);
                        continue;
                    }
                    self.handle_msg(&buf[..WIRE_LEN]);
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(e) => {
                    warn!("error reading from UDP sidechannel listener"; "err" => ?e);
                }
            }
        }
    }

    /// Per-peer read loop on a connected socket, consuming the switch's
    /// reflected `MsgAskAckIndexResp` (which returns to this socket's ephemeral
    /// source port, not the `:7700` listener). Exits when `reader_stop` is set.
    fn peer_read_loop(&self, conn: Arc<UdpSocket>, reader_stop: Arc<AtomicBool>, store_id: u64) {
        let mut buf = [0u8; 9000];
        while !reader_stop.load(Ordering::Relaxed) && !self.stopped.load(Ordering::Relaxed) {
            match conn.recv(&mut buf) {
                Ok(n) => {
                    if n < WIRE_LEN {
                        warn!("received UDP sidechannel message with invalid length"; "length" => n);
                        continue;
                    }
                    self.handle_msg(&buf[..WIRE_LEN]);
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(e) => {
                    // Connected UDP sockets surface ICMP errors (e.g. port
                    // unreachable) on recv; keep going rather than tearing down.
                    warn!("error reading from connected UDP sidechannel socket"; "store_id" => store_id, "err" => ?e);
                }
            }
        }
    }

    fn handle_msg(&self, buf: &[u8]) {
        let d = decode(buf);

        // MsgReadIndex is sent by the client (not a cluster peer) and tagged by
        // the switch. Handle it before the peer lookup since the client is not
        // in the peer table.
        if d.msg_type == MessageType::MsgReadIndex as i32 {
            if d.magic != self.magic {
                warn!("received MsgReadIndex with invalid magic"; "magic" => format!("{:04x}", d.magic));
                return;
            }
            if d.value == 0 {
                // Either the switch has no saved index for this region or no
                // switch touched the packet; indistinguishable and handled
                // identically — `update_switch_index` drops it rather than
                // resolving the marker at 0.
                self.warn_zero_switch_index("MsgReadIndex", d.region_id);
            }
            self.gate.update_switch_index(d.marker, d.region_id, d.value);
            return;
        }

        if d.magic != self.magic {
            warn!("received UDP sidechannel message with invalid magic"; "magic" => format!("{:04x}", d.magic));
            return;
        }

        if d.msg_type == MessageType::MsgAppend as i32
            || d.msg_type == MessageType::MsgAskAckIndex as i32
        {
            // These are purely for the switch; nothing to do on receipt.
            return;
        }

        if d.msg_type == MessageType::MsgAskAckIndexResp as i32 {
            // Switch response for one region. `marker` may identify a batched
            // local query; both the latest-index bump and the batch release are
            // scoped to `region_id`.
            if d.value == 0 {
                // Nothing saved for this region: never tapped, or the register
                // was lost to a reboot. Both reach the gate as "no information".
                self.warn_zero_switch_index("MsgAskAckIndexResp", d.region_id);
            }
            // Mark this (peer, region) path alive so the idle refresh skips it,
            // but only on a nonzero answer: a 0 is the condition the refresh
            // repairs, so letting it set the flag would keep the path looking
            // alive while the register stayed at 0.
            if d.value != 0 {
                self.mark_switch_updated(d.from, d.region_id);
            }
            self.gate.update_switch_index(0, d.region_id, d.value);
            self.gate
                .release_pending_batches(d.marker, d.region_id, d.value);
            // Feed the reflected ack into raft (Flow 1 write acceleration) when
            // it corresponds to a real peer we have a route for. Self-reflected
            // query responses (`from == self`) have no route and are skipped.
            if d.from != self.store_id {
                self.feed_reflected_ack(d.from, d.region_id, d.value);
            }
            return;
        }

        warn!("received UDP sidechannel message with unknown type"; "type" => d.msg_type);
    }

    /// Records that `store_id`'s switch has said something real about
    /// `region_id`, so the next `refresh_region` pass skips that path.
    fn mark_switch_updated(&self, store_id: u64, region_id: u64) {
        // Clone the handle out from under `self.peers` before taking the inner
        // lock, matching `feed_reflected_ack`.
        let regions = {
            let peers = self.peers.lock().unwrap();
            match peers.get(&store_id) {
                Some(p) => p.regions.clone(),
                // Not a peer of ours — e.g. a self-originated read-gate query
                // coming back, whose `from` is this store.
                None => return,
            }
        };
        // `get_mut`, not `entry().or_default()`: an entry exists only once
        // something has been tapped for this (peer, region), so inbound traffic
        // alone cannot grow the map.
        let mut regions = regions.lock().unwrap();
        if let Some(state) = regions.get_mut(&region_id) {
            state.switch_updated = true;
        }
    }

    /// Rate-limited warning for a switch answer of 0 (its reserved invalid
    /// value). Expected for a region the leader has never tapped; a persistent
    /// stream for a region under write load is the visible symptom of a switch
    /// that lost its register.
    fn warn_zero_switch_index(&self, kind: &str, region_id: u64) {
        let n = self.zero_stamps.fetch_add(1, Ordering::Relaxed) + 1;
        if n % 128 == 1 {
            warn!("switch reported no saved index (0) for region";
                "msg_type" => kind, "region_id" => region_id, "total" => n);
        }
    }

    /// Reconstructs the inbound `MsgAskAckIndexResp` from the cached route to
    /// `from_store` and feeds it into the local raft group as if the follower
    /// sent it (from/to swapped relative to the outgoing tap).
    fn feed_reflected_ack(&self, from_store: u64, region_id: u64, value: u64) {
        let route = {
            let peers = self.peers.lock().unwrap();
            match peers.get(&from_store) {
                Some(p) => p.route.lock().unwrap().get(&region_id).cloned(),
                None => None,
            }
        };
        let route = match route {
            Some(r) => r,
            None => return,
        };

        let mut inner = raft::eraftpb::Message::default();
        inner.set_msg_type(MessageType::MsgAskAckIndexResp);
        inner.set_to(route.self_peer.get_id());
        inner.set_from(route.remote_peer.get_id());
        inner.set_index(value);

        let mut rm = RaftMessage::default();
        rm.set_region_id(route.region_id);
        rm.set_from_peer(route.remote_peer.clone());
        rm.set_to_peer(route.self_peer.clone());
        rm.set_message(inner);

        self.feeder.lock().unwrap().feed_raft_message(rm);
    }

    /// Batches newly-pending read-gate markers and issues one local switch query
    /// per region after a short window (mirrors etcd's `querySwitchIndexLoop`);
    /// the switch's per-region ack table answers a `MsgAskAckIndex` for exactly
    /// the region it names.
    ///
    /// Markers still pending past `READ_GATE_QUERY_RETRY` are re-queried. Each
    /// retry records a fresh batch under a higher `local_marker`, so a later
    /// response for the same region releases its earlier batches too
    /// (`release_pending_batches` uses `<=` within a region).
    fn query_switch_index_loop(&self, gate_rx: Receiver<(u64, u64)>) {
        // (marker, region) pairs queried but not yet resolved, pruned to the
        // still-pending subset each pass via `retain_pending`.
        let mut outstanding: Vec<(u64, u64)> = Vec::new();
        let mut last_query = std::time::Instant::now();
        while !self.stopped.load(Ordering::Relaxed) {
            // Wait for the first newly-pending marker, waking at least every
            // SOCKET_READ_TIMEOUT to observe the stop flag and the retry
            // deadline.
            let mut current_batch: Vec<(u64, u64)> = Vec::new();
            match gate_rx.recv_timeout(SOCKET_READ_TIMEOUT) {
                Ok(first) => current_batch.push(first),
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => return,
            }
            // On a fresh marker, greedily drain a short window to batch more
            // before querying.
            if !current_batch.is_empty() {
                let deadline = std::time::Instant::now() + READ_GATE_ASK_TIMEOUT;
                loop {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match gate_rx.recv_timeout(deadline - now) {
                        Ok(m) => current_batch.push(m),
                        Err(_) => break,
                    }
                }
            }

            // Query when there are fresh markers, or when the retry interval has
            // elapsed for markers still awaiting a (possibly lost) response.
            let retry_due = last_query.elapsed() >= READ_GATE_QUERY_RETRY;
            if current_batch.is_empty() && !retry_due {
                continue;
            }

            // Fold fresh pairs in, then keep only still-pending markers.
            // Resetting the timer even when nothing remains keeps an idle loop
            // from re-querying every wake.
            outstanding.extend(current_batch.drain(..));
            let markers: Vec<u64> = outstanding.iter().map(|(m, _)| *m).collect();
            let still: HashSet<u64> = self.gate.retain_pending(&markers).into_iter().collect();
            outstanding.retain(|(m, _)| still.contains(m));
            last_query = std::time::Instant::now();
            if outstanding.is_empty() {
                continue;
            }

            // One query per region: group still-pending markers by region.
            let mut by_region: HashMap<u64, Vec<u64>> = HashMap::new();
            for (marker, region_id) in &outstanding {
                by_region.entry(*region_id).or_default().push(*marker);
            }
            for (region_id, markers) in by_region {
                let batch_marker = self.gate.next_marker();
                self.gate.record_pending_batch(batch_marker, region_id, markers);
                if let Err(e) = self.query_switch_index(region_id, batch_marker) {
                    warn!("failed to issue switch index query"; "region_id" => region_id, "err" => %e);
                }
            }
        }
    }

    /// Sends a `MsgAskAckIndex` for `region_id` tagged with `marker` through any
    /// connected peer's path (some destination is needed); the switch reflects it
    /// back as a `MsgAskAckIndexResp` carrying that region's ack index.
    fn query_switch_index(&self, region_id: u64, marker: u64) -> Result<(), String> {
        let (conn, remote) = {
            let peers = self.peers.lock().unwrap();
            match peers.values().next() {
                Some(p) => (p.conn.clone(), p.remote),
                None => return Err("no connected peers for switch index query".to_owned()),
            }
        };
        // Use our own magic + id: we want our own switch to answer.
        let bytes = encode(
            self.magic,
            MessageType::MsgAskAckIndex,
            self.store_id,
            self.store_id,
            marker,
            0,
            region_id,
        );
        self.enqueue(OutMsg {
            bytes,
            conn,
            remote,
            store_id: self.store_id,
        });
        Ok(())
    }

    /// Refcounted peer attach. Learns a peer's `(ip, udp_port, magic)` from the
    /// raft-stream metadata handshake, opens a connected socket + per-peer read
    /// loop, and on a peer-info change republishes a fresh entry and tears the
    /// old one down. Returns whether the peer was counted: the caller **must**
    /// only pair a `detach_peer` with a `true` return — the early bail below
    /// counts nothing.
    pub fn attach_peer(self: &Arc<Self>, store_id: u64, ip: &str, port: u16, magic: u16) -> bool {
        let remote: SocketAddr = match format!("{}:{}", ip, port).parse() {
            Ok(a) => a,
            Err(e) => {
                warn!("failed to parse UDP sidechannel peer addr"; "store_id" => store_id, "ip" => ip, "port" => port, "err" => ?e);
                return false;
            }
        };

        let mut peers = self.peers.lock().unwrap();
        if let Some(existing) = peers.get_mut(&store_id) {
            if existing.remote == remote && existing.magic == magic {
                existing.refcount += 1;
                return true;
            }
            warn!("UDP sidechannel peer info changed, updating";
                "store_id" => store_id,
                "old_remote" => %existing.remote, "new_remote" => %remote,
                "old_magic" => format!("{:04x}", existing.magic), "new_magic" => format!("{:04x}", magic));
            let refcount = existing.refcount + 1;
            let regions = std::mem::take(&mut *existing.regions.lock().unwrap());
            // Never join a reader while holding `self.peers` — `peer_read_loop`
            // takes that lock in `feed_reflected_ack`. Publish the replacement
            // entry first so sends keep flowing, then join outside the lock.
            existing.reader_stop.store(true, Ordering::Relaxed);
            let old_handle = existing.reader_handle.take();
            let entry = self.build_peer_entry(store_id, remote, magic, refcount, regions);
            peers.insert(store_id, entry);
            drop(peers);
            if let Some(h) = old_handle {
                let _ = h.join();
            }
            return true;
        }
        let entry = self.build_peer_entry(store_id, remote, magic, 1, HashMap::new());
        peers.insert(store_id, entry);
        true
    }

    fn build_peer_entry(
        self: &Arc<Self>,
        store_id: u64,
        remote: SocketAddr,
        magic: u16,
        refcount: usize,
        regions: HashMap<u64, RegionTapState>,
    ) -> PeerEntry {
        let conn = self.dial_peer(store_id, remote);
        let reader_stop = Arc::new(AtomicBool::new(false));
        let reader_handle = conn.as_ref().map(|conn| {
            let conn = conn.clone();
            let reader_stop = reader_stop.clone();
            let sc = self.clone();
            thread::Builder::new()
                .name("udp-sc-peer".to_owned())
                .spawn(move || sc.peer_read_loop(conn, reader_stop, store_id))
                .unwrap()
        });
        PeerEntry {
            store_id,
            remote,
            magic,
            conn,
            refcount,
            regions: Arc::new(Mutex::new(regions)),
            route: Arc::new(Mutex::new(HashMap::new())),
            reader_stop,
            reader_handle,
        }
    }

    /// Opens a connected UDP socket to `remote` so sends skip the per-call route
    /// lookup. Returns `None` on failure (the caller then falls back to the
    /// listener socket).
    fn dial_peer(&self, store_id: u64, remote: SocketAddr) -> Option<Arc<UdpSocket>> {
        let bind_ip = if remote.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let sock = match UdpSocket::bind(bind_ip) {
            Ok(s) => s,
            Err(e) => {
                warn!("failed to bind connected UDP sidechannel socket; falling back to listener"; "store_id" => store_id, "err" => ?e);
                return None;
            }
        };
        if let Err(e) = sock.connect(remote) {
            warn!("failed to connect UDP sidechannel socket; falling back to listener"; "store_id" => store_id, "remote" => %remote, "err" => ?e);
            return None;
        }
        if let Err(e) = sock.set_read_timeout(Some(SOCKET_READ_TIMEOUT)) {
            warn!("failed to set connected UDP sidechannel read timeout"; "err" => ?e);
        }
        Some(Arc::new(sock))
    }

    /// Refcounted peer detach. Tears the connected socket + read loop down when
    /// the last stream to the peer goes away.
    pub fn detach_peer(&self, store_id: u64) {
        let removed = {
            let mut peers = self.peers.lock().unwrap();
            match peers.get_mut(&store_id) {
                Some(entry) => {
                    // Saturate: a wrapped `usize` would pin the entry forever,
                    // leaking its socket and `peer_read_loop` thread.
                    entry.refcount = entry.refcount.saturating_sub(1);
                    if entry.refcount == 0 {
                        peers.remove(&store_id)
                    } else {
                        None
                    }
                }
                // Expected only after `stop()` drained the map; anywhere else it
                // means an attach/detach pairing bug.
                None => {
                    warn!("UDP sidechannel detach for an unattached peer"; "store_id" => store_id);
                    None
                }
            }
        };
        // Tear down with `self.peers` released (see `attach_peer`).
        if let Some(mut entry) = removed {
            entry.reader_stop.store(true, Ordering::Relaxed);
            if let Some(h) = entry.reader_handle.take() {
                let _ = h.join();
            }
        }
    }

    /// Stops all loops and connected sockets. Best-effort.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        // Drain under the lock, join with it released (see `detach_peer`).
        let drained: Vec<PeerEntry> = {
            let mut peers = self.peers.lock().unwrap();
            peers.drain().map(|(_, entry)| entry).collect()
        };
        for mut entry in drained {
            entry.reader_stop.store(true, Ordering::Relaxed);
            if let Some(h) = entry.reader_handle.take() {
                let _ = h.join();
            }
        }
        for h in self.loop_handles.lock().unwrap().drain(..) {
            let _ = h.join();
        }
    }
}
