// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! UDP sidechannel to the in-path P4 switch — the in-repo half of the "assist"
//! read mode. Ported closely from etcd's
//! `server/etcdserver/api/rafthttp/udp_sidechannel.go`, made per region.
//!
//! **Flow 1 — write acceleration (leader <-> switch).** Outgoing `MsgAppend` /
//! `MsgAskAckIndex` are mirrored to the switch by
//! [`UdpSidechannel::process_outgoing_message`] (a tap in `RaftClient::send`).
//! A `MsgAppend` copy is a *write*: it carries this store's high-water mark for
//! the region, the `MsgAppend`'s term and the boot ID this store knows for the
//! peer's switch. The switch applies a write only under its own boot ID, and
//! reflects a `MsgAskAckIndexResp` carrying its saved index for the region; we
//! rebuild a `RaftMessage` from a cached routing template and feed it to the
//! local raft group, which consumes it as a fast ack hint (a Term-0 credit).
//!
//! **SAFE-1 (switch reboots).** A reply carrying V promises that every later
//! stamp that switch gives the region is at least V; a reboot empties the
//! register and breaks that promise. So:
//! * the switch picks a fresh boot ID on boot and NACKs (type 0x80) any write
//!   carrying another one;
//! * the boot ID is tracked per switch (keyed by its magic), shared by every
//!   path behind it; each write puts the ID it was sent under in `marker`,
//!   which the switch echoes, and a NACK is adopted only if it answers a write
//!   sent under the current ID, so a delayed NACK cannot roll back to a dead
//!   boot;
//! * every write carries the region's high-water mark (this store's highest
//!   copied index), and a credit is capped at that mark as read before the boot
//!   check, so whichever write lands first after a reboot covers every credit
//!   this store gave;
//! * replies are credited only under the switch's current boot ID;
//! * the switch stamps reads with the highest term it accepted a write in,
//!   which followers use only in that term.
//!
//! **Flow 2 — follower read gate (client -> switch -> follower).** A client
//! reading from this follower also fires a `MsgReadIndex` UDP packet at this
//! store's `:7700` listener; the switch tags it in flight with the region's
//! saved index and term. The listener resolves the request's read-gate marker
//! via the shared [`SwitchReadGate`] (defined in `raftstore`, where the read
//! path can reach it). Marker-less reads are gated via the batched query loop
//! instead.
//!
//! Wire format: fixed 51-byte UDP payload, all big-endian (TIKV.md
//! "Wire format" is the contract)
//! ```text
//!   off 0  size 2  magic     (u16, default 0xFEED)
//!   off 2  size 1  type      (raft-rs eraftpb::MessageType value, or 0x80 NACK)
//!   off 3  size 8  to        (store id)
//!   off 11 size 8  from      (store id)
//!   off 19 size 8  marker    (per-request read-gate marker, or batch id;
//!                             writes: the boot ID they were sent under,
//!                             echoed by the switch)
//!   off 27 size 8  value     (writes: the region's high-water mark; replies
//!                             and stamps: the register; 0 = no information)
//!   off 35 size 8  region_id (the raft region this message concerns)
//!   off 43 size 4  boot_id   (writes: the boot ID known for the switch, 0
//!                             if none; replies, NACKs, stamps: the switch's)
//!   off 47 size 4  term      (writes: low 32 bits of the MsgAppend's term;
//!                             replies, NACKs, stamps: the slot's stored term)
//! ```
//! The switch indexes its ack table by `region_id % table_size`, so every
//! packet carries its region and both flows are region-scoped. Encode/decode
//! stays in one [`encode`]/[`decode`] pair.

use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Sender};
use kvproto::{metapb, raft_serverpb::RaftMessage};
use raft::eraftpb::MessageType;
use raftstore::store::{SwitchReadGate, SwitchStamp};
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
/// process-globals. Returns `None` if the listen socket can't be bound
/// (logged).
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
/// gRPC raft-stream metadata key advertising this store's sidechannel magic
/// (hex).
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
/// issuing a single local switch query (mirrors etcd's
/// `ReadGateAskTimeoutMillis`).
const READ_GATE_ASK_TIMEOUT: Duration = Duration::from_millis(3);
/// If a marker is still pending this long after its last switch query, re-issue
/// it. The query or its reflected `MsgAskAckIndexResp` can be dropped, or no
/// peer may have been attached when it first fired; a server-minted marker has
/// no other resolution path, so without the retry the gated read never
/// resolves.
const READ_GATE_QUERY_RETRY: Duration = Duration::from_millis(250);
/// Read timeout on the sockets so the blocking loops periodically observe the
/// stop flag.
const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(200);
/// Depth of the out channel drained by the send loop. On overflow the switch
/// hint is dropped (best-effort) rather than blocking raft.
const OUT_CHANNEL_DEPTH: usize = 128;
/// The 51-byte fixed wire payload length: the 43-byte layout (35-byte base +
/// 8-byte region_id) plus the 4-byte boot_id and 4-byte term (SAFE-1).
const WIRE_LEN: usize = 51;
/// Sidechannel-only type a switch reflects instead of applying a write whose
/// boot ID is not its own (SAFE-1). It carries the switch's boot ID and value
/// 0. It sits outside raft-rs's `MessageType` range, so it can never be
/// mistaken for a raft message.
const MSG_SWITCH_NACK: u8 = 0x80;

/// The wire byte for a raft-rs message type.
fn wire_type(t: MessageType) -> u8 {
    t as i32 as u8
}

/// Feeds a reconstructed inbound `RaftMessage` into the local raft group.
/// Implemented by any `RaftExtension`; boxed so the sidechannel stays
/// non-generic, and held behind a `Mutex` because `RaftExtension` is `Send` but
/// not `Sync`. Feeds from the listener / per-peer read-loop threads serialize
/// on it; feeding is a non-blocking router enqueue, off the write critical
/// path.
pub trait RaftMessageFeeder: Send + 'static {
    fn feed_raft_message(&self, msg: RaftMessage);
}

impl<R: RaftExtension + Send + 'static> RaftMessageFeeder for R {
    fn feed_raft_message(&self, msg: RaftMessage) {
        self.feed(msg, false);
    }
}

/// Routing template captured at tap time, used to reconstruct the inbound
/// `RaftMessage` for a switch-reflected `MsgAskAckIndexResp`. `self_peer` is
/// the local (leader) peer; `remote_peer` is the follower we sent to. The
/// reflected response is injected as if the follower sent it (from/to swapped).
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
    /// reflected responses, which return to this socket's ephemeral source
    /// port.
    conn: Option<Arc<UdpSocket>>,
    refcount: usize,
    /// Current boot ID of the switch on this path, learned from its NACK; 0
    /// until then (SAFE-1). Shared by every path with this `magic`
    /// (`UdpSidechannel::switch_boot`), since the magic names the switch. Every
    /// write on any of those paths carries it, and only replies carrying it are
    /// credited.
    boot: Arc<AtomicU32>,
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

impl PeerEntry {
    /// What a write or ask on this path needs, copied out so it can be built
    /// after `UdpSidechannel::peers` is released.
    fn path(&self) -> PeerPath {
        PeerPath {
            store_id: self.store_id,
            remote: self.remote,
            magic: self.magic,
            conn: self.conn.clone(),
            boot: self.boot.clone(),
        }
    }
}

/// A snapshot of one peer's path (see `PeerEntry::path`).
struct PeerPath {
    store_id: u64,
    remote: SocketAddr,
    magic: u16,
    conn: Option<Arc<UdpSocket>>,
    boot: Arc<AtomicU32>,
}

/// Per-(peer, region) tap state.
#[derive(Default)]
struct RegionTapState {
    /// The (term, high-water mark) last written to this (peer, region) by a
    /// `MsgAppend` copy: Flow-1's dedupe watermark. Keyed on the term too, so a
    /// re-elected leader writes in its new term even if its high-water mark has
    /// not moved; otherwise the switch keeps the old term and followers reject
    /// every stamp for the whole term (SAFE-1).
    last_sent_term: u64,
    last_sent_high_water: u64,
    /// Set by any nonzero, boot-matching switch reply naming this
    /// (peer, region); cleared by each `refresh_region` pass. A flag rather
    /// than a timestamp, so the receive path pays one store and no clock read.
    switch_updated: bool,
}

impl RegionTapState {
    /// Forgets the last write, so the next copy goes out whatever its
    /// (term, high-water mark). Used when the switch's boot ID changes.
    fn reset_last_sent(&mut self) {
        self.last_sent_term = 0;
        self.last_sent_high_water = 0;
    }

    /// Records a `MsgAppend` copy of `(term, high_water)` and reports whether
    /// it is newer, lexicographically, than the last one written.
    fn record_last_sent(&mut self, term: u64, high_water: u64) -> bool {
        if (term, high_water) <= (self.last_sent_term, self.last_sent_high_water) {
            return false;
        }
        self.last_sent_term = term;
        self.last_sent_high_water = high_water;
        true
    }
}

/// Leader-side write state for one region, store-wide (SAFE-1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RegionWriteState {
    /// The highest end index of any `MsgAppend` this store has copied for the
    /// region: its own proposals only, never a credited reply value (a
    /// deposed leader's stale-high register must not outlive a reboot). Every
    /// write carries it, and every credit is capped at it (read before the
    /// boot check), so whichever write reaches a freshly booted switch first
    /// covers every credit this store has given for the region.
    high_water: u64,
    /// The highest term of any `MsgAppend` copied for the region, i.e. a term
    /// in which this store led it. Refreshes and re-seeds, which no raft
    /// message carries a term for, write with it.
    leader_term: u64,
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
    /// Count of switch answers carrying no information (a 0 value or term), for
    /// the rate-limited warning in `warn_zero_switch_index`.
    zero_stamps: AtomicU64,
    /// Count of switch replies dropped for carrying a boot ID other than their
    /// switch's current one (SAFE-1), for a rate-limited log line.
    other_boot_replies: AtomicU64,
    /// Count of NACKs ignored as stale (SAFE-1), for a rate-limited log line.
    stale_nacks: AtomicU64,
    peers: Mutex<HashMap<u64, PeerEntry>>,
    /// Current boot ID per switch, keyed by its magic (unique per switch by
    /// configuration), shared by every path behind it (SAFE-1). Entries are
    /// never removed, so a reconnecting peer keeps its switch's ID. Lock
    /// order: may be taken while holding `peers`, never the other way round.
    switch_boots: Mutex<HashMap<u16, Arc<AtomicU32>>>,
    /// Per-region high-water mark and leader term (SAFE-1). Lock order: never
    /// taken while holding `peers`.
    region_writes: Mutex<HashMap<u64, RegionWriteState>>,
    gate: Arc<SwitchReadGate>,
    feeder: Mutex<Box<dyn RaftMessageFeeder>>,
    stopped: Arc<AtomicBool>,
    loop_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Test-only hooks, run once at a named point between two steps whose
    /// order SAFE-1 depends on: "credit" (between the high-water read and the
    /// boot check), "tap" and "refresh" (between the boot-ID load and the
    /// high-water read).
    #[cfg(test)]
    test_hooks: Mutex<HashMap<&'static str, Box<dyn FnOnce(&UdpSidechannel) + Send>>>,
}

impl raftstore::store::SwitchRegisterRefresher for UdpSidechannel {
    fn refresh_region(&self, region_id: u64) {
        UdpSidechannel::refresh_region(self, region_id)
    }
}

/// One sidechannel datagram, decoded or about to be encoded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WireMsg {
    magic: u16,
    msg_type: u8,
    /// The destination store. Parsed for wire completeness, but the receiver is
    /// always this store; routing uses `from`.
    to: u64,
    from: u64,
    marker: u64,
    value: u64,
    region_id: u64,
    boot_id: u32,
    term: u32,
}

/// Encodes a 51-byte sidechannel payload. The single wire encoder.
fn encode(m: &WireMsg) -> [u8; WIRE_LEN] {
    let mut buf = [0u8; WIRE_LEN];
    buf[0..2].copy_from_slice(&m.magic.to_be_bytes());
    buf[2] = m.msg_type;
    buf[3..11].copy_from_slice(&m.to.to_be_bytes());
    buf[11..19].copy_from_slice(&m.from.to_be_bytes());
    buf[19..27].copy_from_slice(&m.marker.to_be_bytes());
    buf[27..35].copy_from_slice(&m.value.to_be_bytes());
    buf[35..43].copy_from_slice(&m.region_id.to_be_bytes());
    buf[43..47].copy_from_slice(&m.boot_id.to_be_bytes());
    buf[47..51].copy_from_slice(&m.term.to_be_bytes());
    buf
}

/// Decodes a sidechannel payload of at least `WIRE_LEN` bytes (any tail is
/// ignored). The single wire decoder.
fn decode(buf: &[u8]) -> WireMsg {
    WireMsg {
        magic: u16::from_be_bytes(buf[0..2].try_into().unwrap()),
        msg_type: buf[2],
        to: u64::from_be_bytes(buf[3..11].try_into().unwrap()),
        from: u64::from_be_bytes(buf[11..19].try_into().unwrap()),
        marker: u64::from_be_bytes(buf[19..27].try_into().unwrap()),
        value: u64::from_be_bytes(buf[27..35].try_into().unwrap()),
        region_id: u64::from_be_bytes(buf[35..43].try_into().unwrap()),
        boot_id: u32::from_be_bytes(buf[43..47].try_into().unwrap()),
        term: u32::from_be_bytes(buf[47..51].try_into().unwrap()),
    }
}

/// The credit a switch reply of `value` gives, for a region whose high-water
/// mark (read before the boot check) is `high_water`: no more than the switch
/// promised, and no more than this store's own proposals (SAFE-1).
///
/// The switch's register keeps only the low 32 bits, so its promise is at most
/// `value`'s low 32 bits, taken as an index; this is deliberately not widened
/// back to 64 bits. A lower credit is always safe: it is only permission for
/// the leader to commit without the holder's own ack. 0 means no credit.
fn capped_credit(value: u64, high_water: u64) -> u64 {
    (value & u64::from(u32::MAX)).min(high_water)
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

        let (sc, out_rx) = Self::with_listener(store_id, magic, listener, feeder, gate);

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

        info!("UDP sidechannel started"; "store_id" => store_id, "addr" => %listen_addr, "magic" => format!("{:04x}", sc.magic));
        Some(sc)
    }

    /// Builds the sidechannel around a bound listener without starting any
    /// loop or publishing anything; returns the send loop's receiver. `new`
    /// wraps it; unit tests drive it directly.
    fn with_listener(
        store_id: u64,
        magic: u16,
        listener: UdpSocket,
        feeder: Box<dyn RaftMessageFeeder>,
        gate: Arc<SwitchReadGate>,
    ) -> (Arc<UdpSidechannel>, Receiver<OutMsg>) {
        let magic = if magic == 0 {
            DEFAULT_UDP_SIDECHANNEL_MAGIC
        } else {
            magic
        };
        let (out_tx, out_rx) = bounded(OUT_CHANNEL_DEPTH);
        let sc = Arc::new(UdpSidechannel {
            store_id,
            magic,
            listener: Arc::new(listener),
            out_tx,
            out_dropped: AtomicU64::new(0),
            zero_stamps: AtomicU64::new(0),
            other_boot_replies: AtomicU64::new(0),
            stale_nacks: AtomicU64::new(0),
            peers: Mutex::new(HashMap::new()),
            switch_boots: Mutex::new(HashMap::new()),
            region_writes: Mutex::new(HashMap::new()),
            gate,
            feeder: Mutex::new(feeder),
            stopped: Arc::new(AtomicBool::new(false)),
            loop_handles: Mutex::new(Vec::new()),
            #[cfg(test)]
            test_hooks: Mutex::new(HashMap::new()),
        });
        (sc, out_rx)
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

    /// Latest switch stamp cached for `region_id`. Currently unused: the
    /// propose-time hint reads the gate directly from `Peer`.
    pub fn latest_switch_stamp(&self, region_id: u64) -> SwitchStamp {
        self.gate.latest_switch_stamp(region_id)
    }

    /// Cadence hook kept for symmetry with `RaftClient::flush` /
    /// `ServerTransport::flush`. The dedicated send loop drains the out channel
    /// continuously, so there is nothing to flush; this is a no-op.
    pub fn flush(&self) {}

    /// Flow-1 tap. Mirrors an outgoing `MsgAppend` / `MsgAskAckIndex` to the
    /// switch (other types are ignored). Never blocks raft: the send is a
    /// non-blocking enqueue, dropped on a full channel.
    ///
    /// A `MsgAppend` copy writes the region's high-water mark, not the
    /// `MsgAppend`'s own end index: a lagging peer's `MsgAppend`s end below
    /// indexes the switch has already reflected, and after a reboot whichever
    /// write lands first must cover all of them (SAFE-1). Its term is the
    /// `MsgAppend`'s own, a term in which this store led the region; never the
    /// store's current term. Copies that advance neither the term nor the
    /// high-water mark for the (peer, region) are deduped away.
    pub fn process_outgoing_message(&self, msg: &RaftMessage) {
        let inner = msg.get_message();
        let msg_type = inner.get_msg_type();
        if msg_type != MessageType::MsgAppend && msg_type != MessageType::MsgAskAckIndex {
            return;
        }

        let region_id = msg.get_region_id();
        let to_store = msg.get_to_peer().get_store_id();
        let (path, regions) = {
            let peers = self.peers.lock().unwrap();
            let peer = match peers.get(&to_store) {
                Some(p) => p,
                None => return,
            };
            // Refresh this region's routing template so a later reflected
            // response can be injected as if it came from this follower.
            peer.route.lock().unwrap().insert(
                region_id,
                RaftRoute {
                    region_id,
                    self_peer: msg.get_from_peer().clone(),
                    remote_peer: msg.get_to_peer().clone(),
                },
            );
            (peer.path(), peer.regions.clone())
        };

        let (out, label) = match msg_type {
            MessageType::MsgAppend => {
                let term = inner.get_term();
                let end = inner.get_index() + inner.get_entries().len() as u64;
                // The boot ID first, then the mark (see `switch_write`).
                let boot_id = path.boot.load(Ordering::SeqCst);
                #[cfg(test)]
                self.run_test_hook("tap");
                let high_water = self.raise_on_copy(region_id, end, term);
                {
                    let mut regions = regions.lock().unwrap();
                    if !regions
                        .entry(region_id)
                        .or_default()
                        .record_last_sent(term, high_water)
                    {
                        return;
                    }
                }
                (
                    self.switch_write(path, boot_id, region_id, high_water, term),
                    "MsgAppend",
                )
            }
            MessageType::MsgAskAckIndex => {
                // Not a write: no value, boot ID or term, and never NACKed.
                let bytes = encode(&WireMsg {
                    magic: path.magic,
                    msg_type: wire_type(MessageType::MsgAskAckIndex),
                    to: path.store_id,
                    from: self.store_id,
                    region_id,
                    ..Default::default()
                });
                (
                    OutMsg {
                        bytes,
                        conn: path.conn,
                        remote: path.remote,
                        store_id: path.store_id,
                    },
                    "MsgAskAckIndex",
                )
            }
            _ => unreachable!(),
        };

        raftstore::store::metrics::SIDECHANNEL_MSG_SENT_TOTAL
            .with_label_values(&[label])
            .inc();
        self.enqueue(out);
    }

    /// Raises `region_id`'s high-water mark to at least `end` and its leader
    /// term to at least `term`, for a `MsgAppend` copied at `term`; returns the
    /// resulting high-water mark.
    fn raise_on_copy(&self, region_id: u64, end: u64, term: u64) -> u64 {
        let mut writes = self.region_writes.lock().unwrap();
        let w = writes.entry(region_id).or_default();
        w.high_water = w.high_water.max(end);
        w.leader_term = w.leader_term.max(term);
        w.high_water
    }

    /// Runs (once) the test hook registered for `point`, if any.
    #[cfg(test)]
    fn run_test_hook(&self, point: &'static str) {
        let hook = self.test_hooks.lock().unwrap().remove(point);
        if let Some(hook) = hook {
            hook(self);
        }
    }

    /// The current boot-ID cell of the switch with this `magic`, created (at 0,
    /// unknown) on first sight.
    fn switch_boot(&self, magic: u16) -> Arc<AtomicU32> {
        self.switch_boots
            .lock()
            .unwrap()
            .entry(magic)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone()
    }

    /// Resets the (term, high-water mark) dedupe state of every (peer, region)
    /// behind the switch with this `magic`, so the next copies go out under
    /// its newly adopted boot ID.
    fn reset_last_sent_behind(&self, magic: u16) {
        let peers = self.peers.lock().unwrap();
        for peer in peers.values().filter(|p| p.magic == magic) {
            for state in peer.regions.lock().unwrap().values_mut() {
                state.reset_last_sent();
            }
        }
    }

    /// `region_id`'s current high-water mark and leader term (zero if this
    /// store never copied a `MsgAppend` for it).
    fn region_write_state(&self, region_id: u64) -> RegionWriteState {
        self.region_writes
            .lock()
            .unwrap()
            .get(&region_id)
            .copied()
            .unwrap_or_default()
    }

    /// Builds a write (`MsgAppend`) of `high_water` at `term` for `region_id`
    /// to the switch on `path`, under `boot_id`. The switch applies it only if
    /// the boot ID is its own and NACKs it otherwise; either way it echoes
    /// `marker`, which names the ID the write was sent under, so a NACK can be
    /// matched to it (`handle_nack`). The switch keeps only the low 32 bits of
    /// the term.
    ///
    /// **Ordering (SAFE-1):** callers load `boot_id` from the switch's cell
    /// *before* they read `high_water`. A credit is capped at a mark read
    /// before its own boot check passed, and an adoption happens after that
    /// check; so a write that carries the adopted ID read its mark after the
    /// adoption, and covers the credit. Reading the mark first would let a
    /// write stalled across a credit and an adoption land in the new boot
    /// below that credit.
    fn switch_write(
        &self,
        path: PeerPath,
        boot_id: u32,
        region_id: u64,
        high_water: u64,
        term: u64,
    ) -> OutMsg {
        let bytes = encode(&WireMsg {
            magic: path.magic,
            msg_type: wire_type(MessageType::MsgAppend),
            to: path.store_id,
            from: self.store_id,
            marker: u64::from(boot_id),
            value: high_water,
            region_id,
            boot_id,
            term: term as u32,
        });
        OutMsg {
            bytes,
            conn: path.conn,
            remote: path.remote,
            store_id: path.store_id,
        }
    }

    /// Writes the region's high-water mark, at the last term this store led
    /// it in and under each path's known boot ID, to every peer whose switch
    /// has said nothing about `region_id` since the previous call. Driven by
    /// raftstore's leader-only `PeerTick::SwitchRegisterRefresh` (see
    /// `SwitchRegisterRefresher`).
    ///
    /// This restores a register a reboot emptied: the write is NACKed, and the
    /// NACK adopts the new boot ID and re-seeds (`handle_nack`). Per peer
    /// because each peer's path can cross a different switch, each holding its
    /// own register and boot ID. It bypasses the `MsgAppend` dedupe on purpose:
    /// it is a retransmission.
    fn refresh_region(&self, region_id: u64) {
        if self.region_write_state(region_id).high_water == 0 {
            // Never tapped anything for this region; nothing to restore.
            return;
        }
        // Collect under the peer lock, send after releasing it: `enqueue` can
        // contend with the send loop, and this runs on the raftstore poller.
        let mut paths: Vec<PeerPath> = Vec::new();
        {
            let peers = self.peers.lock().unwrap();
            for peer in peers.values() {
                {
                    let mut regions = peer.regions.lock().unwrap();
                    let state = match regions.get_mut(&region_id) {
                        // Nothing ever tapped for this (peer, region); the peer
                        // holds no replica of it that we know of.
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
                }
                paths.push(peer.path());
            }
        }
        let mut to_send: Vec<OutMsg> = Vec::with_capacity(paths.len());
        for path in paths {
            // The boot ID first, then the mark (see `switch_write`).
            let boot_id = path.boot.load(Ordering::SeqCst);
            #[cfg(test)]
            self.run_test_hook("refresh");
            let w = self.region_write_state(region_id);
            to_send.push(self.switch_write(path, boot_id, region_id, w.high_water, w.leader_term));
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
    /// (switch-tagged) and any reply or NACK that lands here.
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
    /// reflected replies and NACKs (which return to this socket's ephemeral
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
        if d.msg_type == wire_type(MessageType::MsgReadIndex) {
            if d.magic != self.magic {
                warn!("received MsgReadIndex with invalid magic"; "magic" => format!("{:04x}", d.magic));
                return;
            }
            let stamp = SwitchStamp::new(d.value, d.term);
            if !stamp.is_usable() {
                // Either the switch has nothing saved for this region in this
                // boot (or its slot holds another region), or no switch touched
                // the packet; indistinguishable and handled identically —
                // `update_switch_index` drops it rather than resolving the
                // marker on it.
                self.warn_zero_switch_index("MsgReadIndex", d.region_id);
            }
            self.gate.update_switch_index(d.marker, d.region_id, stamp);
            return;
        }

        if d.msg_type == wire_type(MessageType::MsgAppend)
            || d.msg_type == wire_type(MessageType::MsgAskAckIndex)
        {
            // These are purely for the switch (one reaches us only on a path
            // without one); nothing to do on receipt.
            return;
        }

        // A reply to this store's own read-gate query: to == from == self. It
        // feeds only the gate, which needs no boot check: a stamp is usable
        // only in its own term (SAFE-1).
        if d.from == self.store_id {
            if d.magic != self.magic {
                warn!("received UDP sidechannel query reply with invalid magic"; "magic" => format!("{:04x}", d.magic));
                return;
            }
            if d.msg_type == wire_type(MessageType::MsgAskAckIndexResp) {
                // The marker may identify a batched local query; both the
                // cache update and the batch release are scoped to `region_id`.
                let stamp = SwitchStamp::new(d.value, d.term);
                if !stamp.is_usable() {
                    self.warn_zero_switch_index("MsgAskAckIndexResp", d.region_id);
                }
                self.gate.update_switch_index(0, d.region_id, stamp);
                self.gate
                    .release_pending_batches(d.marker, d.region_id, stamp);
            }
            return;
        }

        // Everything else answers a write or ask this store sent on a peer's
        // path; the reflection swapped to/from, so `from` names the peer.
        if d.msg_type == MSG_SWITCH_NACK {
            self.handle_nack(&d);
        } else if d.msg_type == wire_type(MessageType::MsgAskAckIndexResp) {
            self.credit_switch_reply(&d);
        } else {
            warn!("received UDP sidechannel message with unknown type"; "type" => d.msg_type);
        }
    }

    /// Hands raft the credit in a switch's reply to a write or ask on the path
    /// to `d.from`: a promise that every later stamp that switch gives the
    /// region is at least `d.value`.
    ///
    /// The promise only holds within one switch boot, so a reply is credited
    /// only if it carries the switch's current boot ID, shared by every path
    /// behind that switch. A reply from any other boot (one sent before a
    /// reboot and delivered late, on this path or any other, or an ask answered
    /// by a switch that rebooted since) is dropped. It does not adopt that boot
    /// ID: only a NACK does (`handle_nack`). A reboot seen only here is still
    /// found, because the path then reports nothing nonzero and
    /// `refresh_region` writes, which the switch NACKs.
    ///
    /// The credit is capped at this store's high-water mark for the region
    /// (`capped_credit`), read *before* the boot check: if the check passes,
    /// any adoption racing it changes the ID after the check, and its
    /// re-seed reads a mark at least as high, so the re-seed covers the
    /// credit.
    fn credit_switch_reply(&self, d: &WireMsg) {
        let (boot, regions, route) = {
            let peers = self.peers.lock().unwrap();
            let peer = match peers.get(&d.from) {
                Some(p) => p,
                // Not (or no longer) a peer of ours.
                None => return,
            };
            // The switch on this path answers in the peer's magic, not
            // necessarily ours.
            if d.magic != peer.magic {
                warn!("received UDP sidechannel reply with invalid magic";
                    "magic" => format!("{:04x}", d.magic), "want" => format!("{:04x}", peer.magic));
                return;
            }
            let route = peer.route.lock().unwrap().get(&d.region_id).cloned();
            (peer.boot.clone(), peer.regions.clone(), route)
        };

        // Order matters: the high-water mark first, then the boot check.
        let high_water = self.region_write_state(d.region_id).high_water;
        #[cfg(test)]
        self.run_test_hook("credit");
        let current = boot.load(Ordering::SeqCst);
        if d.boot_id != current {
            let n = self.other_boot_replies.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 128 == 1 {
                info!("dropping switch reply from another boot";
                    "store_id" => d.from, "region_id" => d.region_id,
                    "boot_id" => d.boot_id, "switch_boot_id" => current, "total" => n);
            }
            return;
        }

        self.gate
            .update_switch_index(0, d.region_id, SwitchStamp::new(d.value, d.term));
        if d.value == 0 {
            // Nothing written for this region in this boot (or its slot holds
            // another region): no information, and no credit.
            self.warn_zero_switch_index("MsgAskAckIndexResp", d.region_id);
            return;
        }
        // Mark this (peer, region) path alive so the idle refresh skips it.
        // `get_mut`, not `entry().or_default()`: an entry exists only once
        // something has been tapped for this (peer, region), so inbound traffic
        // alone cannot grow the map.
        if let Some(state) = regions.lock().unwrap().get_mut(&d.region_id) {
            state.switch_updated = true;
        }
        let credit = capped_credit(d.value, high_water);
        // Feed the reflected ack into raft (Flow 1 write acceleration) when we
        // have a route for it.
        if credit == 0 {
            return;
        }
        if let Some(route) = route {
            self.feed_reflected_ack(&route, credit);
        }
    }

    /// Handles a NACK on the path to `d.from`: the switch refused a write
    /// because it carried another boot ID, and reported its own.
    ///
    /// * **Adoption.** If the NACK answers a write sent under the switch's
    ///   current boot ID (`d.marker`, echoed from the write), the switch has
    ///   rebooted since: the current ID becomes `d.boot_id` (compare-and-swap),
    ///   and the dedupe state of every (peer, region) behind that switch is
    ///   reset, so the next copies go out under the new ID.
    /// * **Re-seed.** Then, if `d.boot_id` is current (just adopted, or adopted
    ///   earlier while this write was in flight), the NACK's (peer, region) is
    ///   re-seeded: the region's high-water mark, at the last term this store
    ///   led it in, under that ID. A switch accepts no write for the region,
    ///   and so stamps nothing usable for it, until one arrives carrying its
    ///   boot ID; this is what brings assist back after a reboot. Other regions
    ///   recover through their next copied `MsgAppend` or idle refresh. A lost
    ///   re-seed is retried the same way.
    /// * **Stale.** A NACK whose marker is not the current ID and whose boot ID
    ///   is not current either answers an older write and names an older boot
    ///   (e.g. delayed across a second reboot). It is ignored: adopting it
    ///   would roll the switch back to a dead boot.
    fn handle_nack(&self, d: &WireMsg) {
        if d.boot_id == 0 {
            return; // never reported by a switch; nothing to adopt
        }
        let (path, boot, magic) = {
            let peers = self.peers.lock().unwrap();
            let peer = match peers.get(&d.from) {
                Some(p) => p,
                None => return,
            };
            if d.magic != peer.magic {
                warn!("received UDP sidechannel NACK with invalid magic";
                    "magic" => format!("{:04x}", d.magic), "want" => format!("{:04x}", peer.magic));
                return;
            }
            (peer.path(), peer.boot.clone(), peer.magic)
        };
        let current = boot.load(Ordering::SeqCst);
        if d.marker == u64::from(current)
            && d.boot_id != current
            && boot
                .compare_exchange(current, d.boot_id, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            info!("UDP sidechannel switch boot ID changed; re-seeding";
                "magic" => format!("{:04x}", magic), "store_id" => d.from,
                "region_id" => d.region_id, "old_boot_id" => current, "boot_id" => d.boot_id);
            self.reset_last_sent_behind(magic);
        }
        // The boot ID first, then the mark (see `switch_write`).
        let boot_id = boot.load(Ordering::SeqCst);
        if d.boot_id != boot_id {
            let n = self.stale_nacks.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 128 == 1 {
                info!("ignoring stale switch NACK";
                    "store_id" => d.from, "region_id" => d.region_id,
                    "boot_id" => d.boot_id, "marker" => d.marker, "total" => n);
            }
            return;
        }
        let w = self.region_write_state(d.region_id);
        if w.high_water == 0 {
            return;
        }
        raftstore::store::metrics::SIDECHANNEL_MSG_SENT_TOTAL
            .with_label_values(&["SwitchReseed"])
            .inc();
        self.enqueue(self.switch_write(path, boot_id, d.region_id, w.high_water, w.leader_term));
    }

    /// Rate-limited warning for a switch answer carrying no information (a 0
    /// value or term). Expected for a region the leader has never written in
    /// this switch boot; a persistent stream for a region under write load is
    /// the visible symptom of a switch that lost its register.
    fn warn_zero_switch_index(&self, kind: &str, region_id: u64) {
        let n = self.zero_stamps.fetch_add(1, Ordering::Relaxed) + 1;
        if n % 128 == 1 {
            warn!("switch reported no saved index (value or term 0) for region";
                "msg_type" => kind, "region_id" => region_id, "total" => n);
        }
    }

    /// Reconstructs the inbound `MsgAskAckIndexResp` from a cached route and
    /// feeds it into the local raft group as if the follower sent it (from/to
    /// swapped relative to the outgoing tap). Term 0: raft-rs credits it
    /// without an incarnation check.
    fn feed_reflected_ack(&self, route: &RaftRoute, value: u64) {
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

    /// Batches newly-pending read-gate markers and issues one local switch
    /// query per region after a short window (mirrors etcd's
    /// `querySwitchIndexLoop`); the switch's per-region ack table answers a
    /// `MsgAskAckIndex` for exactly the region it names.
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
                self.gate
                    .record_pending_batch(batch_marker, region_id, markers);
                if let Err(e) = self.query_switch_index(region_id, batch_marker) {
                    warn!("failed to issue switch index query"; "region_id" => region_id, "err" => %e);
                }
            }
        }
    }

    /// Sends a `MsgAskAckIndex` for `region_id` tagged with `marker` through
    /// any connected peer's path (some destination is needed); the switch
    /// reflects it back as a `MsgAskAckIndexResp` carrying that region's
    /// ack index and term.
    fn query_switch_index(&self, region_id: u64, marker: u64) -> Result<(), String> {
        let (conn, remote) = {
            let peers = self.peers.lock().unwrap();
            match peers.values().next() {
                Some(p) => (p.conn.clone(), p.remote),
                None => return Err("no connected peers for switch index query".to_owned()),
            }
        };
        // Use our own magic + id: we want our own switch to answer.
        let bytes = encode(&WireMsg {
            magic: self.magic,
            msg_type: wire_type(MessageType::MsgAskAckIndex),
            to: self.store_id,
            from: self.store_id,
            marker,
            region_id,
            ..Default::default()
        });
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
            // The new entry takes the boot ID of the switch its magic names
            // (a changed magic is a different switch). Its per-region dedupe
            // state starts over, so the next copies are written; the region
            // keys are kept so the idle refresh still covers them.
            let regions: HashMap<u64, RegionTapState> = existing
                .regions
                .lock()
                .unwrap()
                .keys()
                .map(|&region_id| (region_id, RegionTapState::default()))
                .collect();
            // Never join a reader while holding `self.peers` — `peer_read_loop`
            // takes that lock in `handle_msg`. Publish the replacement entry
            // first so sends keep flowing, then join outside the lock.
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
            boot: self.switch_boot(magic),
            regions: Arc::new(Mutex::new(regions)),
            route: Arc::new(Mutex::new(HashMap::new())),
            reader_stop,
            reader_handle,
        }
    }

    /// Opens a connected UDP socket to `remote` so sends skip the per-call
    /// route lookup. Returns `None` on failure (the caller then falls back
    /// to the listener socket).
    fn dial_peer(&self, store_id: u64, remote: SocketAddr) -> Option<Arc<UdpSocket>> {
        let bind_ip = if remote.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
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

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use crossbeam::channel::unbounded;
    use raft::eraftpb::{Entry, Message};
    use raftstore::store::ReadGatePoll;

    use super::*;

    // SAFE-1: a switch's reply carrying V promises that every later stamp it
    // gives the region is at least V. A reboot empties the register, so the
    // switch must not stamp anything usable until a write that covers every
    // earlier credit lands, and only a write carrying its new boot ID may land.
    // Ported from etcd's `rafthttp/udp_sidechannel_safe1_test.go`, per region.

    /// This store: the region's leader.
    const LEADER: u64 = 1;
    /// Switch slot 7, tag 2.
    const REGION: u64 = 2 * 1024 + 7;
    const OTHER_REGION: u64 = 3 * 1024 + 9;
    const TEST_BOOT: u32 = 0xB007_0001;
    const MAGIC: u16 = DEFAULT_UDP_SIDECHANNEL_MAGIC;
    const T_APPEND: u8 = MessageType::MsgAppend as i32 as u8;
    const T_READ_INDEX: u8 = MessageType::MsgReadIndex as i32 as u8;
    const T_ASK: u8 = MessageType::MsgAskAckIndex as i32 as u8;
    const T_REPLY: u8 = MessageType::MsgAskAckIndexResp as i32 as u8;

    /// Records each credit fed into raft, with the region's high-water mark at
    /// the moment it arrived.
    struct RecFeeder {
        sc: Arc<OnceLock<Weak<UdpSidechannel>>>,
        tx: Sender<(RaftMessage, u64)>,
    }

    impl RaftMessageFeeder for RecFeeder {
        fn feed_raft_message(&self, msg: RaftMessage) {
            let hw = self
                .sc
                .get()
                .and_then(Weak::upgrade)
                .map(|sc| sc.region_write_state(msg.get_region_id()).high_water)
                .unwrap_or(0);
            self.tx.send((msg, hw)).unwrap();
        }
    }

    struct Harness {
        sc: Arc<UdpSidechannel>,
        out: Receiver<OutMsg>,
        fed: Receiver<(RaftMessage, u64)>,
    }

    impl Harness {
        /// A sidechannel for store `LEADER` with no loops running: tests call
        /// `handle_msg` themselves and take writes off `out`.
        fn new() -> Harness {
            let listener = UdpSocket::bind("127.0.0.1:0").unwrap();
            let (gate, _gate_rx) = SwitchReadGate::new();
            let slot = Arc::new(OnceLock::new());
            let (tx, fed) = unbounded();
            let feeder = Box::new(RecFeeder {
                sc: slot.clone(),
                tx,
            });
            let (sc, out) = UdpSidechannel::with_listener(LEADER, 0, listener, feeder, gate);
            let _ = slot.set(Arc::downgrade(&sc));
            Harness { sc, out, fed }
        }

        /// Attaches `store_id` behind the `MAGIC` switch, without a socket or
        /// reader thread, and sets that switch's known boot ID to `boot_id`.
        fn add_peer(&self, store_id: u64, boot_id: u32) {
            self.add_peer_behind(store_id, MAGIC);
            self.set_boot(MAGIC, boot_id);
        }

        /// Attaches `store_id` behind the switch with `magic`.
        fn add_peer_behind(&self, store_id: u64, magic: u16) {
            let boot = self.sc.switch_boot(magic);
            self.sc.peers.lock().unwrap().insert(
                store_id,
                PeerEntry {
                    store_id,
                    remote: "127.0.0.1:9".parse().unwrap(),
                    magic,
                    conn: None,
                    refcount: 1,
                    boot,
                    regions: Arc::new(Mutex::new(HashMap::new())),
                    route: Arc::new(Mutex::new(HashMap::new())),
                    reader_stop: Arc::new(AtomicBool::new(false)),
                    reader_handle: None,
                },
            );
        }

        /// Registers a one-shot hook at a named point (see `test_hooks`).
        fn hook(&self, point: &'static str, f: impl FnOnce(&UdpSidechannel) + Send + 'static) {
            self.sc
                .test_hooks
                .lock()
                .unwrap()
                .insert(point, Box::new(f));
        }

        fn set_boot(&self, magic: u16, boot_id: u32) {
            self.sc.switch_boot(magic).store(boot_id, Ordering::SeqCst);
        }

        fn peer_boot(&self, store_id: u64) -> u32 {
            self.sc.peers.lock().unwrap()[&store_id]
                .boot
                .load(Ordering::SeqCst)
        }

        fn switch_updated(&self, store_id: u64, region_id: u64) -> bool {
            let regions = self.sc.peers.lock().unwrap()[&store_id].regions.clone();
            let regions = regions.lock().unwrap();
            regions.get(&region_id).map_or(false, |s| s.switch_updated)
        }

        fn high_water(&self, region_id: u64) -> u64 {
            self.sc.region_write_state(region_id).high_water
        }

        /// Copies a `MsgAppend` from `LEADER` to `to_store` (entries
        /// `prev + 1 ..= prev + n`), as `RaftClient::send` would.
        fn tap(&self, region_id: u64, to_store: u64, term: u64, prev: u64, n: usize) {
            let mut m = Message::default();
            m.set_msg_type(MessageType::MsgAppend);
            m.set_from(100 + LEADER);
            m.set_to(100 + to_store);
            m.set_term(term);
            m.set_index(prev);
            m.set_entries(vec![Entry::default(); n].into());
            let mut rm = RaftMessage::default();
            rm.set_region_id(region_id);
            rm.set_from_peer(new_peer(LEADER));
            rm.set_to_peer(new_peer(to_store));
            rm.set_message(m);
            self.sc.process_outgoing_message(&rm);
        }

        /// Everything queued for the send loop, decoded.
        fn drain(&self) -> Vec<WireMsg> {
            self.out.try_iter().map(|m| decode(&m.bytes)).collect()
        }

        /// Every credit fed into raft so far: `(store, index, hw at feed)`.
        fn credits(&self) -> Vec<(u64, u64, u64)> {
            self.fed
                .try_iter()
                .map(|(rm, hw)| {
                    let m = rm.get_message();
                    assert_eq!(m.get_msg_type(), MessageType::MsgAskAckIndexResp);
                    assert_eq!(m.get_term(), 0, "switch credits are Term 0");
                    (rm.get_from_peer().get_store_id(), m.get_index(), hw)
                })
                .collect()
        }

        fn handle(&self, m: &WireMsg) {
            self.sc.handle_msg(&encode(m));
        }

        /// Delivers `pkts` through `sw`, hands its reflections back, and keeps
        /// going until nothing more is queued (NACKs queue re-seeds).
        fn deliver(&self, sw: &mut FakeSwitch, pkts: Vec<WireMsg>) {
            let mut pkts = pkts;
            for _ in 0..100 {
                if pkts.is_empty() {
                    return;
                }
                for p in pkts {
                    if let Some(r) = sw.process(&encode(&p)) {
                        if r.msg_type == T_REPLY || r.msg_type == MSG_SWITCH_NACK {
                            self.handle(&r);
                        }
                    }
                }
                pkts = self.drain();
            }
            panic!("sidechannel never settled");
        }

        fn pump(&self, sw: &mut FakeSwitch) {
            let pkts = self.drain();
            self.deliver(sw, pkts);
        }
    }

    fn new_peer(store_id: u64) -> metapb::Peer {
        let mut p = metapb::Peer::default();
        p.set_id(100 + store_id);
        p.set_store_id(store_id);
        p
    }

    fn reply(from: u64, region_id: u64, value: u64, boot_id: u32, term: u32) -> WireMsg {
        WireMsg {
            magic: MAGIC,
            msg_type: T_REPLY,
            to: LEADER,
            from,
            value,
            region_id,
            boot_id,
            term,
            ..Default::default()
        }
    }

    /// A NACK naming `boot_id`, answering a write sent under `sent_under`
    /// (the echoed marker).
    fn nack(from: u64, region_id: u64, boot_id: u32, sent_under: u32) -> WireMsg {
        WireMsg {
            magic: MAGIC,
            msg_type: MSG_SWITCH_NACK,
            to: LEADER,
            from,
            marker: u64::from(sent_under),
            region_id,
            boot_id,
            ..Default::default()
        }
    }

    /// `tack-switch-tikv-multiregion.p4`'s sidechannel contract (TIKV.md
    /// "Wire format"), in memory. Boot IDs are deterministic so tests can name
    /// them.
    struct FakeSwitch {
        boots: u32,
        boot_id: u32,
        slots: HashMap<u64, Slot>,
    }

    #[derive(Clone, Copy, Default)]
    struct Slot {
        value: u32,
        tag: u32,
        term: u32,
    }

    /// RFC 1982 compare on the 32 bits the register keeps.
    fn wrap_gt(a: u32, b: u32) -> bool {
        (a.wrapping_sub(b) as i32) > 0
    }

    impl FakeSwitch {
        fn new() -> FakeSwitch {
            FakeSwitch {
                boots: 0,
                boot_id: 0,
                slots: HashMap::new(),
            }
        }

        fn reboot(&mut self) {
            self.boots += 1;
            self.boot_id = 0;
            self.slots.clear();
        }

        fn hit(&self, region_id: u64) -> Option<Slot> {
            self.slots
                .get(&(region_id % 1024))
                .copied()
                .filter(|s| s.tag == (region_id >> 10) as u32)
        }

        /// What the switch would stamp on a read for `region_id` right now.
        fn stamp(&self, region_id: u64) -> SwitchStamp {
            self.hit(region_id).map_or_else(SwitchStamp::default, |s| {
                SwitchStamp::new(u64::from(s.value), s.term)
            })
        }

        /// Processes one packet; returns what it sends on (a reflection, or a
        /// stamped read), or `None` if it forwards the packet unchanged.
        fn process(&mut self, pkt: &[u8]) -> Option<WireMsg> {
            let mut m = decode(pkt);
            if m.magic != MAGIC {
                return None;
            }
            if self.boot_id == 0 {
                self.boot_id = 0xB007_0000 | ((self.boots + 1) << 1) | 1;
            }
            let (idx, tag) = (m.region_id % 1024, (m.region_id >> 10) as u32);
            let hit = self.hit(m.region_id);
            let reflect = match m.msg_type {
                T_APPEND if m.boot_id == self.boot_id => {
                    let v = m.value as u32;
                    let s = self.slots.entry(idx).or_default();
                    if hit.is_some() {
                        if wrap_gt(v, s.value) {
                            s.value = v;
                        }
                        s.term = s.term.max(m.term);
                    } else {
                        *s = Slot {
                            value: v,
                            tag,
                            term: m.term,
                        };
                    }
                    m.msg_type = T_REPLY;
                    m.value = u64::from(s.value);
                    m.term = s.term;
                    true
                }
                T_APPEND => {
                    m.msg_type = MSG_SWITCH_NACK;
                    m.value = 0;
                    m.term = hit.map_or(0, |s| s.term);
                    true
                }
                T_ASK => {
                    m.msg_type = T_REPLY;
                    m.value = hit.map_or(0, |s| u64::from(s.value));
                    m.term = hit.map_or(0, |s| s.term);
                    true
                }
                T_READ_INDEX => {
                    m.value = hit.map_or(0, |s| u64::from(s.value));
                    m.term = hit.map_or(0, |s| s.term);
                    false
                }
                _ => return None,
            };
            m.boot_id = self.boot_id;
            if reflect {
                std::mem::swap(&mut m.to, &mut m.from);
            }
            Some(m)
        }
    }

    #[test]
    fn test_wire_round_trip() {
        let m = WireMsg {
            magic: 0xBEEF,
            msg_type: MSG_SWITCH_NACK,
            to: 1,
            from: 2,
            marker: 3,
            value: 4,
            region_id: 5,
            boot_id: 6,
            term: 7,
        };
        let buf = encode(&m);
        assert_eq!(buf.len(), 51);
        assert_eq!(&buf[43..47], &6u32.to_be_bytes());
        assert_eq!(&buf[47..51], &7u32.to_be_bytes());
        assert_eq!(decode(&buf), m);
        // A longer payload decodes the same; the tail is ignored.
        let mut long = buf.to_vec();
        long.extend_from_slice(&[0xFF; 9]);
        assert_eq!(decode(&long), m);
    }

    /// The SAFE-1 sequence. Up-to-date A (2) takes the register to 100; lagging
    /// B (3) is credited with it. The switch reboots. Whatever write reaches it
    /// next (B's own catch-up `MsgAppend`, a new entry for A, or the idle
    /// refresh), nothing may be stamped below 100: a stale write is NACKed, and
    /// the re-seed carries the high-water mark under the new boot ID. If that
    /// re-seed is lost, B's next catch-up write is the first the new boot
    /// accepts, and it carries the high-water mark too.
    #[test]
    fn test_switch_reboot_reseed_covers_credit() {
        for variant in ["lagging-tap", "idle-refresh", "reseed-lost"] {
            let h = Harness::new();
            h.add_peer(2, 0);
            h.add_peer(3, 0);
            let mut sw = FakeSwitch::new();

            // A's first write draws a NACK (no boot ID known yet); the re-seed
            // is then credited.
            h.tap(REGION, 2, 5, 99, 1);
            h.pump(&mut sw);
            assert_eq!(h.credits(), vec![(2, 100, 100)], "{}", variant);
            // B lags: its MsgAppend ends at 50, but the write carries the
            // high-water mark, and B is credited with the register, 100.
            h.tap(REGION, 3, 5, 49, 1);
            h.pump(&mut sw);
            assert_eq!(h.credits(), vec![(3, 100, 100)], "{}", variant);

            sw.reboot();
            assert_eq!(sw.stamp(REGION), SwitchStamp::default());

            match variant {
                "lagging-tap" => {
                    // B's next catch-up MsgAppend does not raise the high-water
                    // mark, so it is deduped; a new entry for A does, and its
                    // write is NACKed and re-seeds.
                    h.tap(REGION, 3, 5, 59, 1);
                    assert!(h.drain().is_empty(), "a catch-up below the mark wrote");
                    h.tap(REGION, 2, 5, 100, 1);
                }
                "idle-refresh" => {
                    // The first pass clears the flags the credits set.
                    h.sc.refresh_region(REGION);
                    assert!(h.drain().is_empty(), "refreshed a live path");
                    h.sc.refresh_region(REGION);
                }
                "reseed-lost" => {
                    // B's path learns the new boot ID from its refresh's NACK,
                    // but the re-seed that NACK triggers is lost.
                    h.sc.refresh_region(REGION);
                    h.sc.refresh_region(REGION);
                    for w in h.drain().into_iter().filter(|w| w.to == 3) {
                        let r = sw.process(&encode(&w)).unwrap();
                        assert_eq!(r.msg_type, MSG_SWITCH_NACK);
                        h.handle(&r);
                    }
                    assert_eq!(h.drain().len(), 1, "want B's re-seed (dropped)");
                    // A new entry raises the mark; its write to A stays in
                    // flight.
                    h.tap(REGION, 2, 5, 100, 1);
                    assert_eq!(h.drain().len(), 1);
                    // B's catch-up MsgAppend (ending at 60) now passes the
                    // dedupe and is the first write the new boot accepts.
                    h.tap(REGION, 3, 5, 59, 1);
                }
                _ => unreachable!(),
            }
            h.pump(&mut sw);
            let credits = h.credits();
            assert!(!credits.is_empty(), "{}: no re-seed credited", variant);
            for (store, index, _) in credits {
                assert!(
                    index >= 100,
                    "{}: post-reboot credit {} to {} is below the pre-reboot credit 100",
                    variant,
                    index,
                    store
                );
            }
            let st = sw.stamp(REGION);
            assert!(
                st.index >= 100 && st.term == 5,
                "{}: post-reboot stamp {:?}",
                variant,
                st
            );
        }
    }

    /// A write sent before a reboot and delivered after it carries the old boot
    /// ID. The new switch must not apply it, and stamps nothing until a write
    /// with its own boot ID lands; the NACK the stale write draws makes the
    /// leader re-seed with its high-water mark, not the stale value.
    #[test]
    fn test_delayed_pre_reboot_write_rejected() {
        let h = Harness::new();
        h.add_peer(2, 0);
        let mut sw = FakeSwitch::new();
        h.tap(REGION, 2, 5, 99, 1);
        h.pump(&mut sw);
        assert_eq!(h.credits().len(), 1);
        let old_boot = sw.boot_id;
        assert_eq!(h.peer_boot(2), old_boot);

        sw.reboot();

        // The delayed write: an older, lower value under the old boot.
        let stale = WireMsg {
            magic: MAGIC,
            msg_type: T_APPEND,
            to: 2,
            from: LEADER,
            value: 50,
            marker: u64::from(old_boot),
            region_id: REGION,
            boot_id: old_boot,
            term: 5,
            ..Default::default()
        };
        let r = sw.process(&encode(&stale)).unwrap();
        assert_eq!(
            r.marker,
            u64::from(old_boot),
            "the switch echoes the marker"
        );
        assert!(
            r.msg_type == MSG_SWITCH_NACK
                && r.value == 0
                && r.boot_id != old_boot
                && r.boot_id != 0,
            "stale write answered {:?}, want a NACK carrying the new boot ID",
            r
        );
        assert_eq!(
            sw.stamp(REGION),
            SwitchStamp::default(),
            "the stale write landed"
        );

        h.handle(&r);
        assert_eq!(h.peer_boot(2), r.boot_id);
        let reseed = h.drain();
        assert_eq!(reseed.len(), 1);
        let w = reseed[0];
        assert_eq!(
            (
                w.msg_type,
                w.value,
                w.boot_id,
                w.marker,
                w.term,
                w.region_id,
                w.to
            ),
            (T_APPEND, 100, r.boot_id, u64::from(r.boot_id), 5, REGION, 2)
        );
        h.deliver(&mut sw, reseed);
        assert_eq!(h.credits(), vec![(2, 100, 100)]);
        assert_eq!(sw.stamp(REGION), SwitchStamp::new(100, 5));
    }

    /// A reply that does not carry the switch's current boot ID (a pre-reboot
    /// reply delivered late) is dropped. It credits nothing, does not mark the
    /// path healthy or reach the gate cache, and does not adopt its boot ID,
    /// which would roll the switch back to a dead boot.
    #[test]
    fn test_reply_from_other_boot_not_credited() {
        let h = Harness::new();
        h.add_peer(2, TEST_BOOT);
        h.tap(REGION, 2, 5, 99, 1);
        assert_eq!(h.drain().len(), 1);

        h.handle(&reply(2, REGION, 900, TEST_BOOT + 2, 5));

        assert!(h.credits().is_empty());
        assert_eq!(h.high_water(REGION), 100);
        assert!(!h.switch_updated(2, REGION));
        assert_eq!(h.peer_boot(2), TEST_BOOT);
        assert_eq!(h.sc.latest_switch_stamp(REGION), SwitchStamp::default());
        assert!(
            h.drain().is_empty(),
            "a reply from another boot triggered writes"
        );
    }

    /// SAFE-1: a credit is capped at this store's own high-water mark for the
    /// region, and at the 32 bits the switch's register keeps. A reply above
    /// the mark (a deposed leader's stale-high register) never raises the
    /// mark, so it cannot outlive a reboot through this store's re-seeds.
    #[test]
    fn test_credit_capped_at_high_water() {
        let h = Harness::new();
        h.add_peer(2, TEST_BOOT);
        h.tap(REGION, 2, 5, 99, 1);
        h.drain();

        h.handle(&reply(2, REGION, 900, TEST_BOOT, 5));
        assert_eq!(h.credits(), vec![(2, 100, 100)]);
        assert_eq!(h.high_water(REGION), 100, "a credit raised the mark");
        assert!(h.switch_updated(2, REGION));
        // The gate cache records what the switch said, uncapped.
        assert_eq!(h.sc.latest_switch_stamp(REGION), SwitchStamp::new(900, 5));
        // Below the mark, the reply's own value.
        h.handle(&reply(2, REGION, 60, TEST_BOOT, 5));
        assert_eq!(h.credits(), vec![(2, 60, 100)]);
        // Only the low 32 bits are the switch's promise.
        h.handle(&reply(2, REGION, (1 << 32) + 50, TEST_BOOT, 5));
        assert_eq!(h.credits(), vec![(2, 50, 100)]);
        // Re-seeds carry the mark, not the reply.
        h.sc.refresh_region(REGION); // clears the flag
        h.sc.refresh_region(REGION);
        assert_eq!(h.drain()[0].value, 100);

        // A reply of 0 (nothing written for the region in this boot) is no
        // information and no credit; neither is any reply for a region this
        // store never copied an index for (mark 0).
        h.handle(&reply(2, REGION, 0, TEST_BOOT, 0));
        h.handle(&reply(2, OTHER_REGION, 900, TEST_BOOT, 5));
        assert!(h.credits().is_empty());

        assert_eq!(capped_credit(900, 100), 100);
        assert_eq!(capped_credit(60, 100), 60);
        assert_eq!(capped_credit((7 << 32) + 5, 1 << 40), 5);
        assert_eq!(capped_credit(0, 100), 0);
    }

    /// SAFE-1: the cap uses the high-water mark read *before* the boot check.
    /// Whatever moves between the two (here a tap raising the mark to 500)
    /// does not raise the credit: an adoption racing the check re-seeds with a
    /// mark read after it, which must cover the credit.
    #[test]
    fn test_credit_cap_reads_high_water_before_boot_check() {
        let h = Harness::new();
        h.add_peer(2, TEST_BOOT);
        h.tap(REGION, 2, 5, 99, 1);
        h.drain();

        h.hook("credit", |sc| {
            sc.raise_on_copy(REGION, 500, 5);
        });
        h.handle(&reply(2, REGION, 900, TEST_BOOT, 5));
        let c = h.credits();
        assert_eq!(c.len(), 1);
        assert_eq!(
            (c[0].0, c[0].1),
            (2, 100),
            "credit used a mark read after the check"
        );
        assert_eq!(h.high_water(REGION), 500);
    }

    /// SAFE-1: a write reads its high-water mark *after* loading the boot ID it
    /// carries. Here a tap (and then a refresh) stalls between the two while
    /// another proposal raises the mark to 200, a reply credits 200 under B1,
    /// and a NACK adopts B2. Every write that goes out under B2 must cover
    /// that credit.
    #[test]
    fn test_writes_read_high_water_after_boot_id() {
        const B1: u32 = 0xB007_0003;
        const B2: u32 = 0xB007_0005;
        const B3: u32 = 0xB007_0007;
        let h = Harness::new();
        h.add_peer(2, B1);
        h.add_peer(3, B1);
        h.tap(REGION, 2, 5, 99, 1);
        h.drain();

        h.hook("tap", |sc| {
            sc.raise_on_copy(REGION, 200, 5);
            sc.handle_msg(&encode(&reply(2, REGION, 200, B1, 5)));
            sc.handle_msg(&encode(&nack(2, REGION, B2, B1)));
        });
        h.tap(REGION, 3, 5, 49, 1); // a lagging peer's copy
        assert_eq!(h.credits(), vec![(2, 200, 200)]);
        let w = h.drain();
        assert_eq!(w.len(), 2, "{:?}", w);
        for m in &w {
            assert!(
                m.boot_id != B2 || m.value >= 200,
                "a write under the new boot is below the credit: {:?}",
                m
            );
        }

        // The same for a refresh, across a second reboot.
        h.hook("refresh", |sc| {
            sc.raise_on_copy(REGION, 300, 5);
            sc.handle_msg(&encode(&reply(2, REGION, 300, B2, 5)));
            sc.handle_msg(&encode(&nack(2, REGION, B3, B2)));
        });
        h.sc.refresh_region(REGION); // clears the credit's flag on path 2
        h.sc.refresh_region(REGION);
        assert_eq!(h.credits(), vec![(2, 300, 300)]);
        let w = h.drain();
        assert!(!w.is_empty());
        for m in &w {
            assert!(
                m.boot_id != B3 || m.value >= 300,
                "a write under the new boot is below the credit: {:?}",
                m
            );
        }
    }

    /// A NACK answering a write sent under the switch's current boot ID names
    /// its new one; the switch's ID is adopted and the NACK's region re-seeded
    /// with its high-water mark, at the region's leader term, under it. A NACK
    /// naming the already-adopted ID (another region's write was in flight)
    /// re-seeds its region too.
    #[test]
    fn test_nack_adopts_boot_and_reseeds() {
        let h = Harness::new();
        h.add_peer(2, 0);
        h.tap(REGION, 2, 6, 699, 1);
        h.tap(OTHER_REGION, 2, 4, 9, 1);
        assert_eq!(h.drain().len(), 2);
        const NEW_BOOT: u32 = 0xB007_0005;

        h.handle(&nack(2, REGION, NEW_BOOT, 0));
        assert_eq!(h.peer_boot(2), NEW_BOOT);
        let w = h.drain();
        assert_eq!(w.len(), 1, "want one re-seed: {:?}", w);
        assert_eq!(
            (
                w[0].msg_type,
                w[0].value,
                w[0].boot_id,
                w[0].marker,
                w[0].term,
                w[0].region_id,
                w[0].to
            ),
            (T_APPEND, 700, NEW_BOOT, u64::from(NEW_BOOT), 6, REGION, 2)
        );

        // The other region's write, sent under the old ID, is NACKed with the
        // ID already adopted: that region is re-seeded too.
        h.handle(&nack(2, OTHER_REGION, NEW_BOOT, 0));
        let w = h.drain();
        assert_eq!(w.len(), 1);
        assert_eq!(
            (w[0].value, w[0].boot_id, w[0].term, w[0].region_id),
            (10, NEW_BOOT, 4, OTHER_REGION)
        );

        // A region this store never wrote has nothing to re-seed.
        h.handle(&nack(2, REGION + 1, NEW_BOOT, NEW_BOOT));
        assert!(h.drain().is_empty());

        // A boot ID of 0 is never a switch's; ignore it.
        h.handle(&nack(2, REGION, 0, NEW_BOOT));
        assert_eq!(h.peer_boot(2), NEW_BOOT);
        assert!(h.drain().is_empty());

        // Subsequent copies write under the adopted ID.
        h.tap(REGION, 2, 6, 700, 1);
        let w = h.drain();
        assert_eq!(
            (w[0].value, w[0].boot_id, w[0].marker),
            (701, NEW_BOOT, u64::from(NEW_BOOT))
        );
    }

    /// SAFE-1: a NACK delayed across a second reboot answers a write sent
    /// under an older ID and names an older boot. It is ignored: no rollback to
    /// the dead boot, no re-seed. Writes keep going out under the current ID.
    #[test]
    fn test_delayed_stale_nack_ignored() {
        const B1: u32 = 0xB007_0003;
        const B2: u32 = 0xB007_0005;
        let h = Harness::new();
        h.add_peer(2, 0);
        h.tap(REGION, 2, 5, 99, 1);
        h.drain();
        // Boot 0 -> B1 -> B2, each adopted from a NACK of a current write.
        h.handle(&nack(2, REGION, B1, 0));
        h.handle(&nack(2, REGION, B2, B1));
        assert_eq!(h.peer_boot(2), B2);
        assert_eq!(h.drain().len(), 2, "one re-seed per adoption");

        // B1's NACK of the original boot-0 write, delivered last.
        h.handle(&nack(2, REGION, B1, 0));
        assert_eq!(h.peer_boot(2), B2, "a stale NACK rolled the switch back");
        assert!(h.drain().is_empty(), "a stale NACK re-seeded");
        // A NACK from the current boot of an old write still re-seeds.
        h.handle(&nack(2, REGION, B2, B1));
        assert_eq!(h.drain()[0].boot_id, B2);

        h.tap(REGION, 2, 5, 100, 1);
        assert_eq!(h.drain()[0].boot_id, B2);
    }

    /// SAFE-1: the boot ID belongs to the switch, not the path. Once any path
    /// behind a switch adopts its new ID, a pre-reboot reply arriving late on
    /// another path behind it is dropped, not credited.
    #[test]
    fn test_cross_path_late_reply_dropped_after_adoption() {
        const B1: u32 = 0xB007_0003;
        const B2: u32 = 0xB007_0005;
        let h = Harness::new();
        h.add_peer(2, B1);
        h.add_peer(3, B1);
        h.tap(REGION, 2, 5, 99, 1);
        h.tap(REGION, 3, 5, 99, 1);
        h.drain();

        // The switch reboots; path 2's next write draws a NACK naming B2.
        h.handle(&nack(2, REGION, B2, B1));
        assert_eq!((h.peer_boot(2), h.peer_boot(3)), (B2, B2));
        h.drain();

        // A reply from before the reboot, delivered late on path 3.
        h.handle(&reply(3, REGION, 100, B1, 5));
        assert!(
            h.credits().is_empty(),
            "a late pre-reboot reply was credited"
        );
        assert!(!h.switch_updated(3, REGION));
        // The same path's replies under the new ID are credited.
        h.handle(&reply(3, REGION, 100, B2, 5));
        assert_eq!(h.credits(), vec![(3, 100, 100)]);
    }

    /// SAFE-1: adopting a switch's new boot ID resets the (term, high-water
    /// mark) dedupe of every (peer, region) behind that switch, so the next
    /// copies go out under the new ID; paths behind another switch keep theirs.
    #[test]
    fn test_adoption_resets_dedupe_behind_that_switch() {
        const B1: u32 = 0xB007_0003;
        const B2: u32 = 0xB007_0005;
        const OTHER_MAGIC: u16 = 0xBEEF;
        let h = Harness::new();
        h.add_peer(2, B1);
        h.add_peer(3, B1);
        h.add_peer_behind(4, OTHER_MAGIC);
        h.set_boot(OTHER_MAGIC, 0xB007_0101);
        for store in [2, 3, 4] {
            h.tap(REGION, store, 5, 99, 1);
            h.tap(OTHER_REGION, store, 5, 9, 1);
        }
        assert_eq!(h.drain().len(), 6);

        h.handle(&nack(2, REGION, B2, B1));
        assert_eq!(h.drain().len(), 1, "the re-seed");

        // Copies that advance nothing: written again behind the rebooted
        // switch, in every region, under the new ID; deduped behind the other.
        for store in [3, 4] {
            h.tap(REGION, store, 5, 99, 1);
            h.tap(OTHER_REGION, store, 5, 9, 1);
        }
        let w = h.drain();
        let got: Vec<_> = w.iter().map(|m| (m.to, m.region_id, m.boot_id)).collect();
        assert_eq!(got, vec![(3, REGION, B2), (3, OTHER_REGION, B2)]);
        // And deduped again after that.
        h.tap(REGION, 3, 5, 99, 1);
        assert!(h.drain().is_empty());
    }

    /// A lagging peer's `MsgAppend` ends below what the switch has already
    /// reflected for an up-to-date peer. Its write must carry the region's
    /// high-water mark, not its own end index. The mark is per region.
    #[test]
    fn test_tap_carries_high_water_mark() {
        let h = Harness::new();
        h.add_peer(2, TEST_BOOT);
        h.add_peer(3, TEST_BOOT);

        h.tap(REGION, 2, 5, 899, 1); // up to date: ends at 900
        h.tap(REGION, 3, 5, 499, 1); // lagging: ends at 500
        let w = h.drain();
        assert_eq!(w.len(), 2);
        for m in &w {
            assert_eq!(
                (
                    m.msg_type,
                    m.value,
                    m.term,
                    m.boot_id,
                    m.marker,
                    m.region_id
                ),
                (T_APPEND, 900, 5, TEST_BOOT, u64::from(TEST_BOOT), REGION),
                "write to {}",
                m.to
            );
        }
        assert_eq!((w[0].to, w[1].to), (2, 3));

        // Another region has its own mark.
        h.tap(OTHER_REGION, 3, 5, 9, 1);
        let w = h.drain();
        assert_eq!((w[0].value, w[0].region_id), (10, OTHER_REGION));
    }

    /// A write goes out when the term or the high-water mark advances for the
    /// (peer, region). A re-elected leader whose mark is left over from an
    /// earlier term must still write in its new term, or the switch keeps the
    /// old term and followers reject every stamp for the whole term.
    #[test]
    fn test_tap_dedupes_on_term_and_high_water() {
        let h = Harness::new();
        h.add_peer(2, TEST_BOOT);

        let steps: [(u64, u64, bool); 6] = [
            (5, 899, true),  // first write
            (5, 899, false), // nothing new
            (5, 499, false), // a catch-up MsgAppend below the high-water mark
            (7, 499, true),  // same high-water mark, new term
            (6, 999, false), // an older term never writes, even with a higher index
            (7, 999, true),  // higher index in the current term
        ];
        for (i, (term, prev, want)) in steps.iter().enumerate() {
            h.tap(REGION, 2, *term, *prev, 1);
            let w = h.drain();
            assert_eq!(
                w.len() == 1,
                *want,
                "step {} (term {}, end {})",
                i,
                term,
                prev + 1
            );
            if *want {
                assert_eq!(w[0].term, *term as u32, "step {}: write term", i);
            }
        }
        // The dedupe is per region.
        h.tap(OTHER_REGION, 2, 5, 9, 1);
        assert_eq!(h.drain().len(), 1);
        // Refreshes use the highest term copied, never an older MsgAppend's.
        assert_eq!(
            h.sc.region_write_state(REGION),
            RegionWriteState {
                high_water: 1000,
                leader_term: 7
            }
        );
        h.sc.refresh_region(REGION);
        let w = h.drain();
        assert_eq!(
            (w.len(), w[0].value, w[0].term, w[0].boot_id),
            (1, 1000, 7, TEST_BOOT)
        );
    }

    /// Each switch acts only on its own magic, so a reply on a peer's path
    /// carries that peer's magic, which need not be this store's.
    #[test]
    fn test_reply_uses_peer_magic() {
        let h = Harness::new();
        h.add_peer_behind(2, 0xBEEF);
        h.set_boot(0xBEEF, TEST_BOOT);
        h.tap(REGION, 2, 5, 299, 1);
        assert_eq!(h.drain()[0].magic, 0xBEEF);

        h.handle(&reply(2, REGION, 300, TEST_BOOT, 5)); // this store's magic
        assert!(h.credits().is_empty());
        let mut theirs = reply(2, REGION, 300, TEST_BOOT, 5);
        theirs.magic = 0xBEEF;
        h.handle(&theirs);
        assert_eq!(h.credits(), vec![(2, 300, 300)]);
    }

    /// A read's stamp reaches the gate with the term the switch put on it,
    /// through both the client stamp and the batched query reply; a stamp with
    /// no term (or no value) carries no information. Query replies feed only
    /// the gate, without a boot check, and never credit raft.
    #[test]
    fn test_stamp_carries_term_to_the_gate() {
        let h = Harness::new();
        let gate = h.sc.gate.clone();
        let marker = 0xABCD_0000_0000_0077;
        let stamp = |value: u64, term: u32| WireMsg {
            magic: MAGIC,
            msg_type: T_READ_INDEX,
            to: LEADER,
            from: 9,
            marker,
            value,
            region_id: REGION,
            boot_id: TEST_BOOT,
            term,
        };
        let pending = |gate: &SwitchReadGate, marker: u64| {
            matches!(
                gate.poll_read_gate(marker, REGION, false),
                ReadGatePoll::Pending
            )
        };

        assert!(pending(&gate, marker));
        h.handle(&stamp(400, 0)); // no term
        assert!(pending(&gate, marker));
        h.handle(&stamp(0, 5)); // no value
        assert!(pending(&gate, marker));
        h.handle(&stamp(500, 5));
        match gate.poll_read_gate(marker, REGION, false) {
            ReadGatePoll::Resolved(s) => {
                assert_eq!(s, SwitchStamp::new(500, 5));
                assert_eq!(s.floor_in_term(5), Some(500));
                assert_eq!(s.floor_in_term(6), None);
            }
            _ => panic!("read not resolved by its stamp"),
        }

        // The batched path: a reply to this store's own query.
        let server = gate.mint_server_marker();
        assert!(pending(&gate, server));
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![server]);
        let mut q = reply(LEADER, REGION, 510, TEST_BOOT + 2, 6);
        q.marker = batch;
        h.handle(&q);
        match gate.poll_read_gate(server, REGION, false) {
            ReadGatePoll::Resolved(s) => assert_eq!(s, SwitchStamp::new(510, 6)),
            _ => panic!("batch not released by the query reply"),
        }
        assert_eq!(h.sc.latest_switch_stamp(REGION), SwitchStamp::new(510, 6));
        assert!(h.credits().is_empty());

        // And the hint raftstore proposes with carries the cached term.
        assert_eq!(gate.read_index_hint(REGION), (510, 6));
    }

    /// The reflection of a full write through the fake switch: a stamp the
    /// switch then puts on a read carries the leader's write term, so a
    /// follower in that term can use it and one in another term cannot.
    #[test]
    fn test_switch_stamps_reads_with_the_write_term() {
        let h = Harness::new();
        h.add_peer(2, 0);
        let mut sw = FakeSwitch::new();
        h.tap(REGION, 2, 8, 41, 1);
        h.pump(&mut sw);
        let read = WireMsg {
            magic: MAGIC,
            msg_type: T_READ_INDEX,
            to: 2,
            from: 9,
            marker: 0xABCD_0000_0000_0001,
            region_id: REGION,
            ..Default::default()
        };
        let stamped = sw.process(&encode(&read)).unwrap();
        let s = SwitchStamp::new(stamped.value, stamped.term);
        assert_eq!(s, SwitchStamp::new(42, 8));
        assert_eq!(s.floor_in_term(8), Some(42));
        assert_eq!(s.floor_in_term(9), None);

        // A tag miss (another region's slot, or nothing written yet) stamps
        // nothing usable.
        let mut other = read;
        other.region_id = OTHER_REGION;
        let stamped = sw.process(&encode(&other)).unwrap();
        assert!(!SwitchStamp::new(stamped.value, stamped.term).is_usable());
    }

    /// A changed path keeps the boot ID of the switch its magic names, so a
    /// new address behind the same switch writes under the known ID, while a
    /// new magic is a different switch with its own (here still unknown) ID.
    /// Either way the dedupe state starts over and the idle refresh keeps
    /// covering the peer's regions.
    #[test]
    fn test_path_change_keeps_switch_boot_and_resets_dedupe() {
        let h = Harness::new();
        assert!(h.sc.attach_peer(2, "127.0.0.1", 9, MAGIC));
        h.set_boot(MAGIC, TEST_BOOT);
        h.tap(REGION, 2, 5, 99, 1);
        assert_eq!(h.drain()[0].boot_id, TEST_BOOT);

        // New address, same switch.
        assert!(h.sc.attach_peer(2, "127.0.0.1", 10, MAGIC));
        assert_eq!(h.peer_boot(2), TEST_BOOT);
        // The same (term, high-water mark) is written again.
        h.tap(REGION, 2, 5, 99, 1);
        let w = h.drain();
        assert_eq!((w.len(), w[0].value, w[0].boot_id), (1, 100, TEST_BOOT));

        // New magic: another switch, whose boot ID this store doesn't know.
        assert!(h.sc.attach_peer(2, "127.0.0.1", 10, 0xBEEF));
        assert_eq!(h.peer_boot(2), 0);
        h.tap(REGION, 2, 5, 99, 1);
        let w = h.drain();
        assert_eq!((w.len(), w[0].magic, w[0].boot_id), (1, 0xBEEF, 0));
        // The region is still known on the path, so the refresh covers it.
        h.sc.refresh_region(REGION);
        assert_eq!(h.drain().len(), 1);

        h.sc.stop();
    }
}
