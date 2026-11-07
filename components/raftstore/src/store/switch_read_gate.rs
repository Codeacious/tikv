// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared state for the P4-switch-assisted follower read gate ("assist" mode).
//!
//! Owns the read-path half of sidechannel Flow 2 (see
//! `src/server/udp_sidechannel.rs`). A follower read waits for its own switch
//! stamp at the `raftkv` boundary, before it is proposed (`wait_for_stamp`),
//! and raftstore proposes it with that stamp as raft-rs's hint
//! (`switch_hint`). raft-rs serves the read from the follower lease only once
//! commit has reached the stamp; otherwise it holds the read briefly and
//! forwards it to the leader. A read with no stamp is forwarded. Only a read's
//! own stamp is known to postdate the read, which is what makes it a safe
//! hint (design note `2026-11-06-tikv-stamp-before-propose.md`).
//!
//! Every switch answer is a [`SwitchStamp`]: the region's register and the
//! highest term among the writes the switch accepted for it since it booted.
//! raft-rs uses a hint only in its own term (SAFE-1): after a switch reboot, a
//! deposed leader's lower write can land first, and only this term's leader's
//! writes cover this term's credits.
//!
//! The UDP side holds an `Arc` to the same `SwitchReadGate` and drives it
//! (`resolve_marker`, `release_pending_batches`, the batched query loop),
//! reaching it through a process-global slot — raftstore cannot depend on the
//! `tikv` crate. All state is keyed by region id.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::{Duration, Instant},
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::{
    channel::oneshot,
    compat::Future01CompatExt,
    future::{self, Either},
};
use tikv_util::{time::duration_to_sec, timer::SteadyTimer};

use crate::store::metrics::{
    SIDECHANNEL_READ_GATE_WAIT_DURATION, SIDECHANNEL_READ_GATE_WAIT_TOTAL,
};

/// How long a follower read waits for its switch stamp before it is proposed
/// without one, which forwards it to the leader. Matches etcd's
/// `ReadGateDeadlineMillis`.
pub const READ_GATE_DEADLINE: Duration = Duration::from_millis(50);

/// Cadence of the leader-side idle refresh (`SwitchRegisterRefresher`). An idle
/// region is refreshed every *other* tick, because the refresh's own reflection
/// sets the liveness flag for the tick after it.
pub const SWITCH_REGISTER_REFRESH_INTERVAL: Duration = Duration::from_millis(250);

/// How long a switch answer that no read has claimed yet is kept: one to two
/// periods while answers keep arriving, longer when they stop (see
/// `ResolvedStamps`). A read normally claims
/// its answer within milliseconds; an unclaimed one is an orphan (its read was
/// answered elsewhere, gave up, or went to a store that leads the region), and
/// this bounds how many accumulate. Matches etcd's `resolvedMarkerTTL`.
const RESOLVED_TTL: Duration = Duration::from_secs(1);

/// A read-gate marker scoped to the region its read concerns. A stamp is one
/// region's register, so it only ever answers a read in that region (SAFE-6):
/// region ids name raft groups, and the stamp's index is in that group's log.
type MarkerKey = (u64, u64);

/// Switch answers no read has claimed yet, by (marker, region): a client's
/// gate packet that landed before its read started waiting, or an orphan.
/// Two generations: an insert at least `RESOLVED_TTL` after the last rotation
/// makes the current one the previous one and drops the old previous one.
/// Rotation is driven by inserts only, so an unclaimed answer is gone one to
/// two periods after it arrived only while answers keep arriving; with none,
/// it stays. That is only memory, and an idle map does not grow. Safety never
/// relies on it: keys do not recur (random client namespaces,
/// `is_client_marker` at both entry points, batch release only for keys still
/// in `pending`).
struct ResolvedStamps {
    curr: HashMap<MarkerKey, SwitchStamp>,
    prev: HashMap<MarkerKey, SwitchStamp>,
    last_rotate: Instant,
}

impl ResolvedStamps {
    fn new(now: Instant) -> Self {
        ResolvedStamps {
            curr: HashMap::new(),
            prev: HashMap::new(),
            last_rotate: now,
        }
    }

    fn insert_at(&mut self, key: MarkerKey, stamp: SwitchStamp, now: Instant) {
        if now.saturating_duration_since(self.last_rotate) >= RESOLVED_TTL {
            self.prev = std::mem::take(&mut self.curr);
            self.last_rotate = now;
        }
        self.curr.insert(key, stamp);
    }

    fn insert(&mut self, key: MarkerKey, stamp: SwitchStamp) {
        self.insert_at(key, stamp, Instant::now());
    }

    fn remove(&mut self, key: &MarkerKey) -> Option<SwitchStamp> {
        self.curr.remove(key).or_else(|| self.prev.remove(key))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.curr.len() + self.prev.len()
    }
}

/// The stamps for (marker, region) keys and the reads waiting for one, under
/// one lock: a stamp either finds its read waiting or is kept for it.
struct Answers {
    stamps: ResolvedStamps,
    waiting: HashMap<MarkerKey, oneshot::Sender<SwitchStamp>>,
}

impl Answers {
    /// Hands `stamp` to the read waiting on `key`, or keeps it for the read
    /// that is about to wait (a client's packet usually lands before its RPC).
    fn deliver(&mut self, key: MarkerKey, stamp: SwitchStamp) {
        match self.waiting.remove(&key) {
            // A wait that just reached its deadline may already have dropped
            // its receiver; the stamp then goes unused, like any late answer.
            Some(tx) => {
                let _ = tx.send(stamp);
            }
            None => self.stamps.insert(key, stamp),
        }
    }
}

/// Whether `marker` lies in the client namespace: a nonzero upper 32 bits. The
/// zero-upper-32 namespace belongs to this store's server-minted read markers
/// (`mint_server_marker`), which restart at 1 on every start; a client marker
/// there could resolve one with a stamp taken before that read began (SAFE-7).
/// Both client-marker entry points accept only this namespace.
pub fn is_client_marker(marker: u64) -> bool {
    marker >> 32 != 0
}

/// Reserved "no saved index" value, matching `tack-switch-tikv-multiregion.p4`,
/// which serves 0 for a slot holding another region (or none). It is also what
/// a switch that rebooted answers until the next tapped `MsgAppend` rewrites
/// its register.
///
/// A 0 never resolves a waiting read: the gate fails closed, and the read waits
/// or is proposed without a stamp.
const INVALID_SWITCH_INDEX: u64 = 0;

/// What a switch reported for one region: its register (`index`) and the
/// highest term among the writes it has accepted for that region since it
/// booted (`term`). The switch carries the term in 32 bits.
///
/// A stamp with a zero index or a zero term carries no information: the switch
/// has accepted no write for the region since it booted, or its slot holds
/// another region. Such a stamp never resolves a read and never releases a
/// batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SwitchStamp {
    pub index: u64,
    pub term: u32,
}

impl SwitchStamp {
    pub fn new(index: u64, term: u32) -> SwitchStamp {
        SwitchStamp { index, term }
    }

    /// Whether the stamp carries information (nonzero index and term).
    pub fn is_usable(&self) -> bool {
        self.index != INVALID_SWITCH_INDEX && self.term != 0
    }
}

/// Hint for a follower read with no stamp. It travels to raft-rs as
/// `MsgReadIndex.commit`: an index this node must have committed before a lease
/// holder may serve the read locally. Naming an index no node can have
/// committed makes raft's ordinary rules forward the read to the leader, with
/// no switch-shaped special case in the library.
pub const UNKNOWN_SWITCH_INDEX_HINT: u64 = u64::MAX;

/// The `(hint, hint_term)` a follower read is proposed with in assist mode
/// (raft-rs `RawNode::read_index_switch_hint`): its own switch stamp, or, with
/// none, an index no node can have committed, which forwards the read to the
/// leader. Never 0, which raft-rs reads as "no requirement", and never another
/// read's stamp: only a read's own stamp is known to postdate it. raft-rs uses
/// a nonzero hint only if `hint_term` equals its term in 32 bits at serve time,
/// and forwards the read otherwise (SAFE-1).
pub fn switch_hint(stamp: Option<SwitchStamp>) -> (u64, u64) {
    match stamp {
        Some(s) if s.is_usable() => (s.index, u64::from(s.term)),
        _ => (UNKNOWN_SWITCH_INDEX_HINT, 0),
    }
}

/// Where a follower read's `(hint, hint_term)` stands when it is stepped at a
/// follower in `term` that has committed `committed`, by raft-rs's serve test:
/// `met` (the lease may serve it at once), `above_commit` (raft-rs holds it
/// briefly or forwards it), or `other_term` / `no_stamp` (raft-rs forwards
/// it). For metrics only.
pub fn hint_verdict(hint: u64, hint_term: u64, term: u64, committed: u64) -> &'static str {
    if hint == UNKNOWN_SWITCH_INDEX_HINT {
        "no_stamp"
    } else if hint_term as u32 != term as u32 {
        "other_term"
    } else if hint > committed {
        "above_commit"
    } else {
        "met"
    }
}

/// A read's switch stamp, or the wait for it
/// (`SwitchReadGate::wait_for_stamp`).
pub enum StampWait<F> {
    /// The stamp was already here: its client's gate packet landed first.
    Ready(SwitchStamp),
    /// The read is registered; this resolves with its stamp, or with `None` at
    /// the deadline.
    Pending(F),
}

impl<F: Future<Output = Option<SwitchStamp>>> StampWait<F> {
    /// The stamp, waiting for it if need be.
    pub async fn stamp(self) -> Option<SwitchStamp> {
        match self {
            StampWait::Ready(stamp) => Some(stamp),
            StampWait::Pending(wait) => wait.await,
        }
    }
}

/// A group of read-gate markers sharing one in-flight local switch query
/// (`QuerySwitchIndex`), identified by `local_marker`: the switch's
/// `MsgAskAckIndexResp` for `local_marker` releases every marker in the batch.
struct PendingMarkerBatch {
    local_marker: u64,
    region_id: u64,
    markers: Vec<u64>,
}

/// Shared switch read-gate state. Cloneable handle via `Arc`.
pub struct SwitchReadGate {
    /// Counter for server-minted markers. They are masked into the low 32
    /// bits so they cannot collide with client-minted markers, which have
    /// nonzero upper 32 bits.
    marker_seq: AtomicU64,
    /// Random, nonzero upper half of every batch id this instance mints; see
    /// `next_marker`.
    batch_incarnation: u32,
    /// Lower half of the last batch id minted.
    batch_seq: AtomicU32,
    /// (marker, region) pairs a read is waiting on; the query loop asks the
    /// switch about those still here when its batch closes.
    pending: Mutex<HashSet<MarkerKey>>,
    /// Stamps no read has claimed yet, and the reads waiting for one.
    answers: Mutex<Answers>,
    /// Batches with an in-flight `QuerySwitchIndex`, each for a single region.
    pending_batches: Mutex<Vec<PendingMarkerBatch>>,
    /// Newly-pending `(marker, region_id)` pairs handed to the sidechannel's
    /// batched query loop.
    pending_markers_tx: Sender<(u64, u64)>,
    /// Regions whose peer on this store is, as raftstore last published, not
    /// the leader. Only their replica reads wait for a stamp.
    followers: RwLock<HashSet<u64>>,
}

/// A random, nonzero batch-id upper half.
fn new_batch_incarnation() -> u32 {
    match rand::random::<u32>() {
        0 => 1,
        inc => inc,
    }
}

impl SwitchReadGate {
    /// Creates the gate and returns it alongside the receiver end of the
    /// pending-marker channel. The sidechannel's query loop owns the receiver.
    pub fn new() -> (Arc<SwitchReadGate>, Receiver<(u64, u64)>) {
        let (tx, rx) = unbounded();
        let gate = Arc::new(SwitchReadGate {
            // Start at 1 so 0 stays a reserved "no marker" sentinel.
            marker_seq: AtomicU64::new(1),
            batch_incarnation: new_batch_incarnation(),
            batch_seq: AtomicU32::new(0),
            pending: Mutex::new(HashSet::new()),
            answers: Mutex::new(Answers {
                stamps: ResolvedStamps::new(Instant::now()),
                waiting: HashMap::new(),
            }),
            pending_batches: Mutex::new(Vec::new()),
            pending_markers_tx: tx,
            followers: RwLock::new(HashSet::new()),
        });
        (gate, rx)
    }

    /// Mints a server-side marker for a marker-less read, in the low 32-bit
    /// namespace so it never collides with a client-minted marker. Wraps
    /// within the low 32 bits, skipping 0 (the "no marker" sentinel).
    pub fn mint_server_marker(&self) -> u64 {
        loop {
            let m = self.marker_seq.fetch_add(1, Ordering::Relaxed) & 0x0000_0000_FFFF_FFFF;
            if m != 0 {
                return m;
            }
        }
    }

    /// Records whether this store's peer of `region_id` is a follower (not the
    /// leader), from raftstore's role changes. A stale value costs latency, not
    /// safety: a follower read proposed without a stamp is forwarded, and a
    /// leader ignores the hint.
    pub fn set_local_follower(&self, region_id: u64, follower: bool) {
        let mut followers = self.followers.write().unwrap();
        if follower {
            followers.insert(region_id);
        } else {
            followers.remove(&region_id);
        }
    }

    /// Whether replica reads of `region_id` should wait for a stamp here.
    pub fn is_local_follower(&self, region_id: u64) -> bool {
        self.followers.read().unwrap().contains(&region_id)
    }

    /// Waits, at most `READ_GATE_DEADLINE`, for the switch stamp of the read
    /// keyed by (`marker`, `region_id`), before the read is proposed:
    ///
    ///   * the stamp the switch put on the client's gate packet, for a client
    ///     marker (the packet usually lands before its RPC, and then waits in
    ///     `answers`), or
    ///   * the answer to a batched query of the region's register, for a
    ///     server-minted marker, or for a client marker whose packet has not
    ///     landed by the time the query loop's batch closes.
    ///
    /// Either was taken after the read began: the client sends its packet once
    /// the read has begun, and a batch is recorded after its reads joined and
    /// queried after that. `None` at the deadline; the read is then proposed
    /// without a stamp, which forwards it to the leader.
    ///
    /// The wait is registered when this is called, not when the future is
    /// first polled. A stamp that is already here comes back as `Ready`, so the
    /// common case (packet first) needs no future at all. Callers drive the
    /// future to completion: one dropped early leaves its read registered, and
    /// queried, until the switch answers for it.
    pub fn wait_for_stamp(
        self: &Arc<Self>,
        marker: u64,
        region_id: u64,
    ) -> StampWait<impl Future<Output = Option<SwitchStamp>> + Send + 'static> {
        let key = (marker, region_id);
        let kind = if is_client_marker(marker) {
            "client"
        } else {
            "server"
        };
        let start = Instant::now();
        // Acquire `answers` before `pending`, as the writers do, so a stamp
        // either is already in `answers` or finds this read waiting.
        let rx = {
            let mut answers = self.answers.lock().unwrap();
            if let Some(stamp) = answers.stamps.remove(&key) {
                observe_wait(kind, "ready", start);
                return StampWait::Ready(stamp);
            }
            let (tx, rx) = oneshot::channel();
            answers.waiting.insert(key, tx);
            if self.pending.lock().unwrap().insert(key) {
                // Unbounded, so this never blocks; the query loop is the
                // batching point.
                let _ = self.pending_markers_tx.try_send(key);
            }
            rx
        };
        let gate = self.clone();
        StampWait::Pending(async move {
            let deadline = SteadyTimer::default().delay(READ_GATE_DEADLINE).compat();
            let (stamp, outcome) = match future::select(rx, deadline).await {
                Either::Left((Ok(stamp), _)) => (Some(stamp), "waited"),
                // Another wait for the same key replaced this one, and owns
                // the key now.
                Either::Left((Err(_), _)) => (None, "replaced"),
                // Stop waiting and stop querying for the key. A late answer
                // then finds it gone from `pending` and resolves nothing.
                Either::Right(_) => {
                    gate.answers.lock().unwrap().waiting.remove(&key);
                    gate.pending.lock().unwrap().remove(&key);
                    (None, "deadline")
                }
            };
            observe_wait(kind, outcome, start);
            stamp
        })
    }

    /// Resolves (`marker`, `region_id`) with the stamp the switch put on a
    /// client's gate packet, for the read waiting on it or about to. Called by
    /// the socket side on inbound `MsgReadIndex` (client, switch-tagged).
    pub fn resolve_marker(&self, marker: u64, region_id: u64, stamp: SwitchStamp) {
        // A zero index or term carries no information (see `SwitchStamp`): it
        // never resolves a read.
        if !stamp.is_usable() || marker == 0 {
            return;
        }
        // Keyed by the stamp's region: a read in another region never claims
        // it (SAFE-6), and waits for its own region's answer instead.
        let key = (marker, region_id);
        let mut answers = self.answers.lock().unwrap();
        self.pending.lock().unwrap().remove(&key);
        answers.deliver(key, stamp);
    }

    /// Mints the next batch id: `batch_incarnation` in the upper 32 bits, the
    /// batch sequence in the lower 32.
    ///
    /// SAFE-3: a sequence restarting at 1 on every start let a query reply
    /// delayed across a restart release this instance's batches (release is
    /// cumulative) with a register value read before their reads began. A
    /// random incarnation per instance makes such replies identifiable
    /// (`is_own_batch_marker`), and the reply path drops them. Mirrors the
    /// incarnation in raft-rs lease ids.
    ///
    /// Batch ids never enter `pending`, and travel only in this store's own
    /// queries and their replies (to == from == self), so they cannot meet a
    /// client marker despite the nonzero upper half. Server-minted read
    /// markers stay in the low 32-bit namespace (`mint_server_marker`).
    ///
    /// The sequence wraps within the lower 32 bits, keeping the incarnation;
    /// `release_pending_batches` compares it wrap-safely.
    pub fn next_marker(&self) -> u64 {
        let seq = self
            .batch_seq
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        u64::from(self.batch_incarnation) << 32 | u64::from(seq)
    }

    /// Whether `marker` is a batch id this instance minted, as opposed to one
    /// from a previous instance of this store (SAFE-3).
    pub fn is_own_batch_marker(&self, marker: u64) -> bool {
        (marker >> 32) as u32 == self.batch_incarnation
    }

    /// Records a batch of markers for `region_id` under an in-flight query's
    /// `local_marker`.
    pub fn record_pending_batch(&self, local_marker: u64, region_id: u64, markers: Vec<u64>) {
        self.pending_batches
            .lock()
            .unwrap()
            .push(PendingMarkerBatch {
                local_marker,
                region_id,
                markers,
            });
    }

    /// Filters `(marker, region)` pairs down to those still awaiting
    /// resolution.
    pub fn retain_pending(&self, keys: &[(u64, u64)]) -> Vec<(u64, u64)> {
        let pending = self.pending.lock().unwrap();
        keys.iter()
            .copied()
            .filter(|k| pending.contains(k))
            .collect()
    }

    /// Releases `region_id`'s batches with `local_marker <= batch_marker` (a
    /// response for `batch_marker` implies that region's earlier queries are
    /// answered too), resolving their still-pending markers at `answered`.
    /// A response for one region never releases another region's reads.
    ///
    /// An unusable `answered` (zero index or term) releases nothing — there is
    /// no stamp to hand out — and the markers stay pending for the query
    /// loop to retry.
    ///
    /// The caller has checked `is_own_batch_marker(batch_marker)` (SAFE-3), so
    /// only the lower halves are compared, as RFC 1982 serial numbers: the
    /// batches in flight span far less than 2^31 ids, so "at or below" stays
    /// correct across the sequence's wrap.
    pub fn release_pending_batches(
        &self,
        batch_marker: u64,
        region_id: u64,
        answered: SwitchStamp,
    ) {
        if batch_marker == 0 || !answered.is_usable() {
            return;
        }
        let released: Vec<PendingMarkerBatch> = {
            let mut batches = self.pending_batches.lock().unwrap();
            let mut released = Vec::new();
            let mut kept = Vec::new();
            for b in batches.drain(..) {
                let newer = (b.local_marker as u32).wrapping_sub(batch_marker as u32) as i32 > 0;
                if b.region_id == region_id && !newer {
                    released.push(b);
                } else {
                    kept.push(b);
                }
            }
            *batches = kept;
            released
        };
        if released.is_empty() {
            return;
        }
        let mut answers = self.answers.lock().unwrap();
        let mut pending = self.pending.lock().unwrap();
        for batch in released {
            for marker in batch.markers {
                let key = (marker, batch.region_id);
                if pending.remove(&key) {
                    answers.deliver(key, answered);
                }
            }
        }
    }
}

/// Counts a stamp wait's outcome, by marker namespace, and the time it added
/// to the read: 0 for a stamp that was already there.
fn observe_wait(kind: &str, outcome: &str, start: Instant) {
    SIDECHANNEL_READ_GATE_WAIT_TOTAL
        .with_label_values(&[kind, outcome])
        .inc();
    SIDECHANNEL_READ_GATE_WAIT_DURATION.observe(duration_to_sec(start.elapsed()));
}

/// Process-global switch read gate, published by the sidechannel at startup and
/// read back by raftstore and the `raftkv` read path. `None` when
/// `read_mode != "assist"`, in which case the read path is unchanged.
static GLOBAL_SWITCH_READ_GATE: OnceLock<Arc<SwitchReadGate>> = OnceLock::new();

/// Publishes the process-global switch read gate. Idempotent: a second call is
/// ignored (a store hosts one sidechannel).
pub fn set_global_switch_read_gate(gate: Arc<SwitchReadGate>) {
    let _ = GLOBAL_SWITCH_READ_GATE.set(gate);
}

/// Returns the process-global switch read gate, or `None` if the sidechannel
/// has not been enabled.
pub fn global_switch_read_gate() -> Option<Arc<SwitchReadGate>> {
    GLOBAL_SWITCH_READ_GATE.get().cloned()
}

/// Re-arms the in-path switches' saved index for one region while nothing is
/// being written to it. The register is only ever written by a tapped
/// `MsgAppend`, so without this a switch that reboots during a read-only
/// stretch never recovers. Recovery, not safety.
///
/// Implemented by `UdpSidechannel` in the `tikv` crate and published here at
/// startup, like `SwitchReadGate`; the caller is a raftstore `PeerTick`.
pub trait SwitchRegisterRefresher: Send + Sync + 'static {
    /// For each attached peer, if that peer's switch has said nothing about
    /// `region_id` since the previous call, re-send this store's high-water
    /// mark for the region, at the last term this store led it in, under the
    /// boot ID known for that peer's switch (SAFE-1). A peer whose switch is
    /// talking, or for which nothing was ever tapped for this region, is
    /// skipped.
    fn refresh_region(&self, region_id: u64);

    /// Records the stores holding a live read lease for `region_id` from this
    /// leader ("credit-all"): a switch reply for the region is credited to
    /// every one of them behind the same switch, since that switch's one slot
    /// stamps all their reads. An empty list clears it; raftstore passes one
    /// when this peer is no longer a lease-holding leader.
    fn set_lease_holders(&self, region_id: u64, holder_stores: Vec<u64>);
}

static GLOBAL_SWITCH_REGISTER_REFRESHER: OnceLock<Arc<dyn SwitchRegisterRefresher>> =
    OnceLock::new();

/// Publishes the process-global switch register refresher. Idempotent.
pub fn set_global_switch_register_refresher(r: Arc<dyn SwitchRegisterRefresher>) {
    let _ = GLOBAL_SWITCH_REGISTER_REFRESHER.set(r);
}

/// Returns the process-global switch register refresher, or `None` if the
/// sidechannel has not been enabled.
pub fn global_switch_register_refresher() -> Option<&'static Arc<dyn SwitchRegisterRefresher>> {
    GLOBAL_SWITCH_REGISTER_REFRESHER.get()
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    const REGION: u64 = 7;
    const TERM: u32 = 5;

    fn st(index: u64) -> SwitchStamp {
        SwitchStamp::new(index, TERM)
    }

    /// Whether a read is still waiting on (`marker`, `region`).
    fn waiting(gate: &SwitchReadGate, marker: u64, region: u64) -> bool {
        !gate.retain_pending(&[(marker, region)]).is_empty()
    }

    /// A client's stamp that landed before its read started waiting is handed
    /// over at once; one that lands while the read waits wakes it.
    #[test]
    fn test_wait_takes_its_stamp_before_or_while_waiting() {
        let (gate, _rx) = SwitchReadGate::new();
        let first = 0xABCD_0000_0000_0001;
        gate.resolve_marker(first, REGION, st(40));
        assert_eq!(
            block_on(gate.wait_for_stamp(first, REGION).stamp()),
            Some(st(40))
        );
        assert!(!waiting(&gate, first, REGION));

        let later = 0xABCD_0000_0000_0002;
        let w = gate.wait_for_stamp(later, REGION);
        assert!(waiting(&gate, later, REGION));
        gate.resolve_marker(later, REGION, st(41));
        assert!(!waiting(&gate, later, REGION));
        assert_eq!(block_on(w.stamp()), Some(st(41)));
    }

    /// A newly waiting read is handed to the query loop once; a client marker
    /// too, in case its packet was lost.
    #[test]
    fn test_waiting_read_is_queued_for_a_query() {
        let (gate, rx) = SwitchReadGate::new();
        let client = 0xABCD_0000_0000_0003;
        let server = gate.mint_server_marker();
        let _c = gate.wait_for_stamp(client, REGION);
        let _s = gate.wait_for_stamp(server, REGION);
        let queued: Vec<(u64, u64)> = rx.try_iter().collect();
        assert_eq!(queued, vec![(client, REGION), (server, REGION)]);
    }

    /// A 0 answer must not resolve a waiting read; a real one must. raft-rs
    /// twin: `test_switch_stamp_unstamped_read_is_not_held`.
    #[test]
    fn test_zero_switch_index_never_resolves_a_waiting_read() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = 0xABCD_0000_0000_0004;
        let w = gate.wait_for_stamp(marker, REGION);
        // The switch answers this marker with its reserved invalid value.
        gate.resolve_marker(marker, REGION, st(0));
        assert!(waiting(&gate, marker, REGION));

        // A real answer resolves it, at that index and term.
        gate.resolve_marker(marker, REGION, st(42));
        assert_eq!(block_on(w.stamp()), Some(st(42)));
    }

    /// SAFE-1: a stamp without a term (the switch has accepted no write for the
    /// region since it booted, or its slot holds another region) is no
    /// information either, from a client packet or a batch answer.
    #[test]
    fn test_zero_term_stamp_is_unusable() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        let w = gate.wait_for_stamp(marker, REGION);

        gate.resolve_marker(marker, REGION, SwitchStamp::new(400, 0));
        assert!(waiting(&gate, marker, REGION));
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, SwitchStamp::new(400, 0));
        assert!(waiting(&gate, marker, REGION));

        gate.resolve_marker(marker, REGION, SwitchStamp::new(500, 6));
        assert_eq!(block_on(w.stamp()), Some(SwitchStamp::new(500, 6)));
    }

    /// A batch answer wakes the reads in it, at the batch's own stamp, term
    /// included: the register when the switch answered that query, which it
    /// did after the reads joined the batch.
    #[test]
    fn test_batch_answer_wakes_its_reads() {
        let (gate, _rx) = SwitchReadGate::new();
        let reads: Vec<u64> = (0..2).map(|_| gate.mint_server_marker()).collect();
        let waits: Vec<_> = reads
            .iter()
            .map(|&m| gate.wait_for_stamp(m, REGION))
            .collect();
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, reads.clone());
        gate.release_pending_batches(batch, REGION, SwitchStamp::new(510, 10));
        for w in waits {
            assert_eq!(block_on(w.stamp()), Some(SwitchStamp::new(510, 10)));
        }
    }

    /// The batched path fails closed on a 0 answer the same way the stamp path
    /// does.
    #[test]
    fn test_zero_answer_does_not_release_pending_batches() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        let w = gate.wait_for_stamp(marker, REGION);

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, st(0));
        assert!(waiting(&gate, marker, REGION));

        // Once the switch answers with a real index again, the batch releases.
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, st(500));
        assert_eq!(block_on(w.stamp()), Some(st(500)));
    }

    /// A read that hears nothing gives up at the deadline. It is then neither
    /// queried for nor resolved by a late answer.
    #[test]
    fn test_wait_gives_up_at_the_deadline() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        let w = gate.wait_for_stamp(marker, REGION);
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);

        let start = Instant::now();
        assert_eq!(block_on(w.stamp()), None);
        assert!(start.elapsed() >= READ_GATE_DEADLINE);
        assert!(!waiting(&gate, marker, REGION));

        gate.release_pending_batches(batch, REGION, st(500));
        assert_eq!(gate.answers.lock().unwrap().stamps.len(), 0);
        assert!(gate.answers.lock().unwrap().waiting.is_empty());
    }

    /// A read is proposed with its own stamp, term included; with no stamp, or
    /// an unusable one, with a hint no node can commit, which forwards it.
    /// raft-rs twin: `test_switch_hint_gate`.
    #[test]
    fn test_switch_hint() {
        assert_eq!(switch_hint(Some(SwitchStamp::new(77, 5))), (77, 5));
        assert_eq!(switch_hint(None), (UNKNOWN_SWITCH_INDEX_HINT, 0));
        assert_eq!(
            switch_hint(Some(SwitchStamp::new(0, 5))),
            (UNKNOWN_SWITCH_INDEX_HINT, 0)
        );
        assert_eq!(
            switch_hint(Some(SwitchStamp::new(77, 0))),
            (UNKNOWN_SWITCH_INDEX_HINT, 0)
        );
    }

    /// The hint verdict follows raft-rs's serve test, term compared in 32 bits.
    /// raft-rs twin: `test_switch_hint_term_gate`.
    #[test]
    fn test_hint_verdict() {
        let term = u64::from(TERM);
        let verdict = |stamp| {
            let (hint, hint_term) = switch_hint(stamp);
            hint_verdict(hint, hint_term, term, 40)
        };
        assert_eq!(verdict(None), "no_stamp");
        assert_eq!(verdict(Some(SwitchStamp::new(40, TERM + 1))), "other_term");
        assert_eq!(verdict(Some(st(41))), "above_commit");
        assert_eq!(verdict(Some(st(40))), "met");
        assert_eq!(hint_verdict(40, term, term + (1 << 32), 40), "met");
    }

    /// Only regions published as having a local follower wait for stamps.
    #[test]
    fn test_local_follower_flag() {
        let (gate, _rx) = SwitchReadGate::new();
        assert!(!gate.is_local_follower(REGION));
        gate.set_local_follower(REGION, true);
        assert!(gate.is_local_follower(REGION));
        assert!(!gate.is_local_follower(REGION + 1));
        gate.set_local_follower(REGION, false);
        assert!(!gate.is_local_follower(REGION));
    }

    /// SAFE-3: every instance draws its own nonzero incarnation, every batch
    /// id it mints carries it, and another instance's ids are not its own.
    #[test]
    fn test_batch_markers_carry_incarnation() {
        let (a, _ra) = SwitchReadGate::new();
        let (b, _rb) = SwitchReadGate::new();
        assert_ne!(a.batch_incarnation, 0);
        assert_ne!(b.batch_incarnation, 0);
        // 2^-32 flake odds.
        assert_ne!(a.batch_incarnation, b.batch_incarnation);
        for _ in 0..3 {
            let m = a.next_marker();
            assert!(a.is_own_batch_marker(m));
            assert!(!b.is_own_batch_marker(m));
        }
        // A pre-fix batch id (upper half 0) is nobody's.
        assert!(!a.is_own_batch_marker(5000));
    }

    /// SAFE-3: release is cumulative, so it must order batch ids correctly
    /// across the lower half's wrap. A reply to the last id before the wrap
    /// releases nothing minted after it; one after the wrap releases all.
    #[test]
    fn test_batch_release_across_seq_wrap() {
        let (gate, _rx) = SwitchReadGate::new();
        let id = |seq: u32| u64::from(gate.batch_incarnation) << 32 | u64::from(seq);
        let reads: Vec<u64> = (0..4).map(|_| gate.mint_server_marker()).collect();
        let mut waits = Vec::new();
        for (&read, seq) in reads.iter().zip([u32::MAX - 1, u32::MAX, 0, 1]) {
            waits.push(gate.wait_for_stamp(read, REGION));
            gate.record_pending_batch(id(seq), REGION, vec![read]);
        }

        gate.release_pending_batches(id(u32::MAX), REGION, st(900));
        assert!(!waiting(&gate, reads[0], REGION));
        assert!(!waiting(&gate, reads[1], REGION));
        assert!(waiting(&gate, reads[2], REGION));
        assert!(waiting(&gate, reads[3], REGION));

        gate.release_pending_batches(id(1), REGION, st(1200));
        let got: Vec<_> = waits.into_iter().map(|w| block_on(w.stamp())).collect();
        assert_eq!(
            got,
            vec![Some(st(900)), Some(st(900)), Some(st(1200)), Some(st(1200))]
        );
        assert!(gate.pending_batches.lock().unwrap().is_empty());
    }

    /// SAFE-6: a stamp is one region's register, so it never answers a read in
    /// another region, whether it lands before the read waits or while it
    /// waits. The read is answered by its own region's stamp.
    #[test]
    fn test_stamp_for_another_region_does_not_resolve() {
        const OTHER: u64 = REGION + 1;
        let (gate, _rx) = SwitchReadGate::new();
        let during = 0xABCD_0000_0000_0005;
        let before = 0xABCD_0000_0000_0006;

        let w = gate.wait_for_stamp(during, REGION);
        gate.resolve_marker(during, OTHER, st(500));
        assert!(waiting(&gate, during, REGION));

        gate.resolve_marker(before, OTHER, st(500));
        let _b = gate.wait_for_stamp(before, REGION);
        assert!(waiting(&gate, before, REGION));

        // A batch answer for the other region releases neither.
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, OTHER, vec![during, before]);
        gate.release_pending_batches(batch, OTHER, st(600));
        assert!(waiting(&gate, during, REGION));
        assert!(waiting(&gate, before, REGION));

        gate.resolve_marker(during, REGION, st(42));
        assert_eq!(block_on(w.stamp()), Some(st(42)));
    }

    /// SAFE-6: one marker waiting in two regions is two waiting reads. Each is
    /// queued for its own region's query, and each region's answer resolves
    /// only its own.
    #[test]
    fn test_same_marker_in_two_regions_is_tracked_per_region() {
        const OTHER: u64 = REGION + 1;
        let (gate, rx) = SwitchReadGate::new();
        let marker = 0xABCD_0000_0000_0007;

        let in_region = gate.wait_for_stamp(marker, REGION);
        let in_other = gate.wait_for_stamp(marker, OTHER);
        let queued: Vec<(u64, u64)> = rx.try_iter().collect();
        assert_eq!(queued, vec![(marker, REGION), (marker, OTHER)]);
        assert_eq!(gate.retain_pending(&queued), queued);

        gate.resolve_marker(marker, OTHER, st(900));
        assert_eq!(block_on(in_other.stamp()), Some(st(900)));
        assert_eq!(gate.retain_pending(&queued), vec![(marker, REGION)]);
        gate.resolve_marker(marker, REGION, st(901));
        assert_eq!(block_on(in_region.stamp()), Some(st(901)));
    }

    /// Under steady inserts, unclaimed answers expire one to two `RESOLVED_TTL`
    /// periods after they arrive; a claimed one is gone at once.
    #[test]
    fn test_unclaimed_answers_expire() {
        let t0 = Instant::now();
        let mut r = ResolvedStamps::new(t0);
        let (a, b, c) = ((1, REGION), (2, REGION), (3, REGION));
        r.insert_at(a, st(1), t0);
        r.insert_at(b, st(2), t0 + RESOLVED_TTL); // rotates: a is now previous
        assert_eq!(r.len(), 2);
        r.insert_at(c, st(3), t0 + 2 * RESOLVED_TTL); // rotates: a dropped
        assert_eq!(r.remove(&a), None);
        assert_eq!(r.remove(&b), Some(st(2)));
        assert_eq!(r.remove(&b), None);
        assert_eq!(r.remove(&c), Some(st(3)));
    }

    /// Server markers wrap within the low 32 bits and skip 0.
    #[test]
    fn test_server_markers_wrap_and_skip_zero() {
        let (gate, _rx) = SwitchReadGate::new();
        gate.marker_seq.store(0xFFFF_FFFF, Ordering::Relaxed);
        assert_eq!(gate.mint_server_marker(), 0xFFFF_FFFF);
        assert_eq!(gate.mint_server_marker(), 1);
        assert_eq!(gate.mint_server_marker(), 2);
    }
}
