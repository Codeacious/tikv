// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared state for the P4-switch-assisted follower read gate ("assist" mode).
//!
//! Owns the read-path half of sidechannel Flow 2 (see
//! `src/server/udp_sidechannel.rs`). Two touch-points, both inside raftstore's
//! `Peer`:
//!
//!   * propose-time hint — stamp the read-index with the region's cached switch
//!     stamp and its term (`read_index_hint`), and
//!   * serve-time gate  — hold a marked replica read until the switch answers
//!     that read's marker (`poll_read_gate`), then serve at or above the
//!     answered index, if the answer was stamped in the term raft served the
//!     read in (`SwitchStamp::floor_in_term`).
//!
//! Every switch answer is a [`SwitchStamp`]: the region's register and the
//! highest term among the writes the switch accepted for it since it booted.
//! A stamp is used only in its own term (SAFE-1): after a switch reboot, a
//! deposed leader's lower write can land first, and only this term's leader's
//! writes cover this term's credits.
//!
//! The UDP side holds an `Arc` to the same `SwitchReadGate` and drives it
//! (`update_switch_index`, `release_pending_batches`, the batched query loop),
//! reaching it through a process-global slot — raftstore cannot depend on the
//! `tikv` crate. All state is keyed by region id.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, Sender};

/// How long a marked read may sit in the serve gate before it is failed. The
/// query loop re-queries every 250ms, so this allows several retries.
pub const READ_GATE_GIVE_UP: Duration = Duration::from_secs(1);

/// Cadence of the leader-side idle refresh (`SwitchRegisterRefresher`). An idle
/// region is refreshed every *other* tick, because the refresh's own reflection
/// sets the liveness flag for the tick after it.
pub const SWITCH_REGISTER_REFRESH_INTERVAL: Duration = Duration::from_millis(250);

/// Reserved "no saved index" value, matching `tack-switch-tikv-multiregion.p4`,
/// which serves 0 for a slot holding another region (or none). It is also what
/// a switch that rebooted answers until the next tapped `MsgAppend` rewrites
/// its register.
///
/// A 0 never resolves a gated read and is never cached as a region's latest
/// index: the gate fails closed and the read waits or gives up.
const INVALID_SWITCH_INDEX: u64 = 0;

/// What a switch reported for one region: its register (`index`) and the
/// highest term among the writes it has accepted for that region since it
/// booted (`term`). The switch carries the term in 32 bits.
///
/// A stamp with a zero index or a zero term carries no information: the switch
/// has accepted no write for the region since it booted, or its slot holds
/// another region. Such a stamp never resolves a read, never releases a batch
/// and is never cached.
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

    /// The floor this stamp puts on a read that raft produced in
    /// `read_state_term` (raft-rs `ReadState::term`): its index, but only if
    /// the stamp was taken in that same term, compared on the 32 bits the
    /// switch carries (SAFE-1). `None` means the stamp says nothing about
    /// that term, and the read must not be served on it.
    pub fn floor_in_term(&self, read_state_term: u64) -> Option<u64> {
        if self.is_usable() && self.term == read_state_term as u32 {
            Some(self.index)
        } else {
            None
        }
    }

    /// Lexicographic order key: the term first, then the index.
    fn order_key(&self) -> (u32, u64) {
        (self.term, self.index)
    }
}

/// Propose-time hint for a region the switch has told us nothing about
/// (`latest_switch_index == INVALID_SWITCH_INDEX`).
///
/// The hint travels to raft-rs as `MsgReadIndex.commit`: an index this node
/// must have committed before a lease holder may serve the read locally. Naming
/// an index no node can have committed makes raft's ordinary rules forward the
/// read to the leader, with no switch-shaped special case in the library.
const UNKNOWN_SWITCH_INDEX_HINT: u64 = u64::MAX;

/// Outcome of a serve-gate poll (`SwitchReadGate::poll_read_gate`).
pub enum ReadGatePoll {
    /// The switch confirmed this marker with this stamp (always usable). The
    /// caller serves the read at *or above* its index, and only if it was
    /// stamped in the term raft served the read in
    /// (`SwitchStamp::floor_in_term`).
    Resolved(SwitchStamp),
    /// Not yet confirmed and still within the give-up window; the caller
    /// re-queues the read and retries on the next poll.
    Pending,
    /// The read waited past `READ_GATE_GIVE_UP`; the marker has been dropped
    /// from tracking and the caller should fail the read.
    GiveUp,
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
    /// Latest switch stamp, per region: the lexicographic max of (term, index)
    /// over every usable stamp seen for the region. A newer term replaces the
    /// stamp even with a lower index: a hint is used only in its own term. The
    /// `RwLock` guards only the map container; each region has its own small
    /// lock, so different regions never serialize on the hot path.
    latest: RwLock<HashMap<u64, Arc<Mutex<SwitchStamp>>>>,
    /// Counter for server-minted markers and batch ids. Server markers are
    /// masked into the low 32 bits so they cannot collide with client-minted
    /// markers, which have nonzero upper 32 bits.
    marker_seq: AtomicU64,
    /// Markers registered by the serve gate and awaiting confirmation, mapped
    /// to the region each read concerns.
    pending: Mutex<HashMap<u64, u64>>,
    /// Confirmed marker -> switch stamp, consumed by the next serve-gate poll.
    resolved: Mutex<HashMap<u64, SwitchStamp>>,
    /// Batches with an in-flight `QuerySwitchIndex`, each for a single region.
    pending_batches: Mutex<Vec<PendingMarkerBatch>>,
    /// Newly-pending `(marker, region_id)` pairs handed to the sidechannel's
    /// batched query loop.
    pending_markers_tx: Sender<(u64, u64)>,
}

impl SwitchReadGate {
    /// Creates the gate and returns it alongside the receiver end of the
    /// pending-marker channel. The sidechannel's query loop owns the receiver.
    pub fn new() -> (Arc<SwitchReadGate>, Receiver<(u64, u64)>) {
        let (tx, rx) = unbounded();
        let gate = Arc::new(SwitchReadGate {
            latest: RwLock::new(HashMap::new()),
            // Start at 1 so 0 stays a reserved "no marker" sentinel.
            marker_seq: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            resolved: Mutex::new(HashMap::new()),
            pending_batches: Mutex::new(Vec::new()),
            pending_markers_tx: tx,
        });
        (gate, rx)
    }

    /// Returns the region's latest-stamp slot, creating it under a brief write
    /// lock on first sight.
    fn region_stamp(&self, region_id: u64) -> Arc<Mutex<SwitchStamp>> {
        if let Some(a) = self.latest.read().unwrap().get(&region_id) {
            return a.clone();
        }
        self.latest
            .write()
            .unwrap()
            .entry(region_id)
            .or_insert_with(|| Arc::new(Mutex::new(SwitchStamp::default())))
            .clone()
    }

    /// Latest switch stamp for `region_id` (the (term, index) max), or the
    /// all-zero, unusable stamp if the switch has told us nothing. Does not
    /// insert a map entry. The raw cache; `read_index_hint` is the propose-time
    /// view of it.
    pub fn latest_switch_stamp(&self, region_id: u64) -> SwitchStamp {
        self.latest
            .read()
            .unwrap()
            .get(&region_id)
            .map(|a| *a.lock().unwrap())
            .unwrap_or_default()
    }

    /// The `(hint, hint_term)` to put on this region's `MsgReadIndex` (raft-rs
    /// `RawNode::read_index_switch_hint`): the cached stamp's index and term,
    /// or `(UNKNOWN_SWITCH_INDEX_HINT, 0)` when the cache is still empty,
    /// which forwards the read to the leader. raft-rs uses a nonzero hint
    /// only if `hint_term` equals its term in 32 bits at serve time, and
    /// forwards the read otherwise (SAFE-1).
    ///
    /// "Never stamped" and "the switch has nothing saved right now" are the
    /// same condition and take the same path — the cache already carries
    /// both, so there is no per-region "ever stamped" bookkeeping and must
    /// not be.
    pub fn read_index_hint(&self, region_id: u64) -> (u64, u64) {
        let stamp = self.latest_switch_stamp(region_id);
        if stamp.is_usable() {
            (stamp.index, u64::from(stamp.term))
        } else {
            (UNKNOWN_SWITCH_INDEX_HINT, 0)
        }
    }

    /// Mints a server-side marker for a marker-less read, in the low 32-bit
    /// namespace so it never collides with a client-minted marker.
    pub fn mint_server_marker(&self) -> u64 {
        self.marker_seq.fetch_add(1, Ordering::Relaxed) & 0x0000_0000_FFFF_FFFF
    }

    /// Non-blocking serve-gate poll. Returns `Resolved(stamp)` once the switch
    /// has confirmed `marker`; otherwise registers it (issuing a batched
    /// local query on first sight) and returns `Pending`, so the caller
    /// re-queues the read — the raftstore poller thread is never parked.
    ///
    /// `expired` is set by the caller once the read has waited past
    /// `READ_GATE_GIVE_UP`; an expired, still-unresolved marker is dropped from
    /// tracking and `GiveUp` returned.
    pub fn poll_read_gate(&self, marker: u64, region_id: u64, expired: bool) -> ReadGatePoll {
        // Acquire `resolved` before `pending`, matching `update_switch_index`.
        let mut resolved = self.resolved.lock().unwrap();
        // An unusable stamp carries no index floor, so it is not a resolution.
        // Both writers already refuse to record one; restated here at the single
        // consumption point.
        if let Some(stamp) = resolved.remove(&marker)
            && stamp.is_usable()
        {
            drop(resolved);
            self.pending.lock().unwrap().remove(&marker);
            return ReadGatePoll::Resolved(stamp);
        }
        if expired {
            // Drop from `pending` so `retain_pending` stops re-querying and a
            // batch release can't re-resolve it. A late client `MsgReadIndex` can
            // still leave an orphaned `resolved` entry — bounded, never
            // re-consumed, left as-is.
            drop(resolved);
            self.pending.lock().unwrap().remove(&marker);
            return ReadGatePoll::GiveUp;
        }
        let newly = self
            .pending
            .lock()
            .unwrap()
            .insert(marker, region_id)
            .is_none();
        drop(resolved);
        if newly {
            // Unbounded, so this never blocks; the query loop is the batching
            // point.
            let _ = self.pending_markers_tx.try_send((marker, region_id));
        }
        ReadGatePoll::Pending
    }

    /// Raises `region_id`'s cached stamp to the (term, index) max and, for a
    /// nonzero marker, resolves it. Called by the socket side on inbound
    /// `MsgReadIndex` (client, switch-tagged) and — with `marker == 0` — on
    /// `MsgAskAckIndexResp` (cache update only; batch release is separate).
    pub fn update_switch_index(&self, marker: u64, region_id: u64, stamp: SwitchStamp) {
        // A zero index or term carries no information (see `SwitchStamp`): don't
        // cache it, don't let it resolve a gated read.
        if !stamp.is_usable() {
            return;
        }
        // region_id 0 is the placeholder sentinel and the switch always serves 0
        // for it, so skip the cache entry. The marker is still resolved below, so
        // a placeholder read falls through to the host-ack path instead of gating
        // until give-up.
        if region_id != 0 {
            let slot = self.region_stamp(region_id);
            let mut latest = slot.lock().unwrap();
            if stamp.order_key() > latest.order_key() {
                *latest = stamp;
            }
        }
        if marker == 0 {
            return;
        }
        let mut resolved = self.resolved.lock().unwrap();
        self.pending.lock().unwrap().remove(&marker);
        resolved.insert(marker, stamp);
    }

    /// Mints the next marker from the shared sequence (used for batch ids).
    pub fn next_marker(&self) -> u64 {
        self.marker_seq.fetch_add(1, Ordering::Relaxed)
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

    /// Filters `markers` down to those still awaiting resolution.
    pub fn retain_pending(&self, markers: &[u64]) -> Vec<u64> {
        let pending = self.pending.lock().unwrap();
        markers
            .iter()
            .copied()
            .filter(|m| pending.contains_key(m))
            .collect()
    }

    /// Releases `region_id`'s batches with `local_marker <= batch_marker` (a
    /// response for `batch_marker` implies that region's earlier queries are
    /// answered too), resolving their still-pending markers at `answered`.
    /// A response for one region never releases another region's reads.
    ///
    /// An unusable `answered` (zero index or term) releases nothing — there is
    /// no floor to hand out — and the markers stay pending for the query
    /// loop to retry.
    ///
    /// The floor is `answered`, not the cached `latest_switch_stamp`: a higher
    /// cache only means another read sampled the same register later.
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
                if b.region_id == region_id && b.local_marker <= batch_marker {
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
        let mut resolved = self.resolved.lock().unwrap();
        let mut pending = self.pending.lock().unwrap();
        for batch in released {
            for marker in batch.markers {
                if pending.remove(&marker).is_some() {
                    resolved.insert(marker, answered);
                }
            }
        }
    }
}

/// Process-global switch read gate, published by the sidechannel at startup and
/// read back by every `Peer`. `None` when `read_mode != "assist"`, in which
/// case the read path is unchanged.
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
    use super::*;

    const REGION: u64 = 7;
    const TERM: u32 = 5;

    fn st(index: u64) -> SwitchStamp {
        SwitchStamp::new(index, TERM)
    }

    fn poll(gate: &SwitchReadGate, marker: u64, expired: bool) -> ReadGatePoll {
        gate.poll_read_gate(marker, REGION, expired)
    }

    fn assert_pending(poll: ReadGatePoll) {
        match poll {
            ReadGatePoll::Pending => {}
            ReadGatePoll::Resolved(s) => panic!("expected Pending, got Resolved({:?})", s),
            ReadGatePoll::GiveUp => panic!("expected Pending, got GiveUp"),
        }
    }

    fn assert_resolved(poll: ReadGatePoll, want: SwitchStamp) {
        match poll {
            ReadGatePoll::Resolved(s) => assert_eq!(s, want),
            ReadGatePoll::Pending => panic!("expected Resolved({:?}), got Pending", want),
            ReadGatePoll::GiveUp => panic!("expected Resolved({:?}), got GiveUp", want),
        }
    }

    /// A 0 answer must not release a gated read; a real one must. raft-rs twin:
    /// `test_switch_stamp_unstamped_read_is_not_held`.
    #[test]
    fn test_zero_switch_index_never_resolves_a_gated_read() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();

        assert_pending(poll(&gate, marker, false));
        // The switch answers this marker with its reserved invalid value.
        gate.update_switch_index(marker, REGION, st(0));
        assert_pending(poll(&gate, marker, false));

        // A real answer releases it, at that index and term.
        gate.update_switch_index(marker, REGION, st(42));
        assert_resolved(poll(&gate, marker, false), st(42));
    }

    /// SAFE-1: a stamp without a term (the switch has accepted no write for the
    /// region since it booted, or its slot holds another region) is no
    /// information either: it neither resolves a read nor reaches the cache.
    #[test]
    fn test_zero_term_stamp_is_unusable() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));

        gate.update_switch_index(marker, REGION, SwitchStamp::new(400, 0));
        assert_pending(poll(&gate, marker, false));
        assert_eq!(gate.latest_switch_stamp(REGION), SwitchStamp::default());
        assert_eq!(gate.read_index_hint(REGION), (u64::MAX, 0));

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, SwitchStamp::new(400, 0));
        assert_pending(poll(&gate, marker, false));

        gate.update_switch_index(marker, REGION, SwitchStamp::new(500, 6));
        assert_resolved(poll(&gate, marker, false), SwitchStamp::new(500, 6));
    }

    /// A 0 neither raises the cache from 0 nor lowers it once real. Contract
    /// test only — the ordering underneath already ignores a 0.
    #[test]
    fn test_zero_switch_index_is_not_cached() {
        let (gate, _rx) = SwitchReadGate::new();
        gate.update_switch_index(0, REGION, st(0));
        assert_eq!(gate.latest_switch_stamp(REGION), SwitchStamp::default());

        gate.update_switch_index(0, REGION, st(90));
        assert_eq!(gate.latest_switch_stamp(REGION), st(90));

        // A later reboot answer neither lowers nor clears the cache.
        gate.update_switch_index(0, REGION, st(0));
        assert_eq!(gate.latest_switch_stamp(REGION), st(90));
    }

    /// SAFE-1: the per-region cache is the lexicographic max of (term, index).
    /// Within a term it only rises; a later term replaces it even with a lower
    /// index (a hint is used only in its own term); an earlier term never
    /// replaces it, however high its index.
    #[test]
    fn test_cache_orders_by_term_then_index() {
        let (gate, _rx) = SwitchReadGate::new();
        let steps: [((u64, u32), (u64, u32)); 6] = [
            ((100, 5), (100, 5)), // first stamp
            ((90, 5), (100, 5)),  // lower index, same term: no change
            ((120, 5), (120, 5)), // higher index, same term
            ((60, 6), (60, 6)),   // later term, lower index: replaces
            ((900, 5), (60, 6)),  // earlier term, higher index: ignored
            ((61, 6), (61, 6)),   // higher index in the current term
        ];
        for (i, ((idx, term), (want_idx, want_term))) in steps.iter().enumerate() {
            gate.update_switch_index(0, REGION, SwitchStamp::new(*idx, *term));
            assert_eq!(
                gate.latest_switch_stamp(REGION),
                SwitchStamp::new(*want_idx, *want_term),
                "step {}",
                i
            );
            assert_eq!(
                gate.read_index_hint(REGION),
                (*want_idx, u64::from(*want_term)),
                "step {}",
                i
            );
        }
        // Per region.
        assert_eq!(gate.read_index_hint(REGION + 1), (u64::MAX, 0));
    }

    /// The batched path fails closed on a 0 answer the same way the stamp path
    /// does.
    #[test]
    fn test_zero_answer_does_not_release_pending_batches() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));

        // This store saw a high value before the switch rebooted.
        gate.update_switch_index(0, REGION, st(500));

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, st(0));
        assert_pending(poll(&gate, marker, false));

        // Once the switch answers with a real index again, the batch releases.
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, st(500));
        assert_resolved(poll(&gate, marker, false), st(500));
    }

    /// The floor is this batch's answer, not the region's high-water cache.
    #[test]
    fn test_batch_releases_at_the_answered_index_not_the_cache() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));

        // An unrelated, later sample raises the region cache.
        gate.update_switch_index(0, REGION, st(900));

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, SwitchStamp::new(500, 4));
        // The read gets the batch's own stamp, term included.
        assert_resolved(poll(&gate, marker, false), SwitchStamp::new(500, 4));
    }

    /// A region the switch has said nothing about hints an index no node can
    /// have committed, so raft-rs forwards the read. raft-rs twin:
    /// `test_switch_hint_gate`.
    #[test]
    fn test_read_index_hint_is_unreachable_until_the_switch_speaks() {
        let (gate, _rx) = SwitchReadGate::new();
        assert_eq!(gate.read_index_hint(REGION), (u64::MAX, 0));

        // A zero answer is not information, so it does not change the hint.
        gate.update_switch_index(0, REGION, st(0));
        assert_eq!(gate.read_index_hint(REGION), (u64::MAX, 0));

        // A real answer becomes the hint verbatim, with its term.
        gate.update_switch_index(0, REGION, st(77));
        assert_eq!(gate.read_index_hint(REGION), (77, u64::from(TERM)));

        // A later reboot answer neither lowers the cache nor re-arms the
        // unreachable hint.
        gate.update_switch_index(0, REGION, st(0));
        assert_eq!(gate.read_index_hint(REGION), (77, u64::from(TERM)));

        // The hint is per region, like the cache.
        assert_eq!(gate.read_index_hint(REGION + 1), (u64::MAX, 0));
    }

    /// Failing closed still terminates: a read that only ever gets 0 answers
    /// gives up at the deadline.
    #[test]
    fn test_zero_stamp_still_gives_up_when_expired() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));
        gate.update_switch_index(marker, REGION, st(0));
        match poll(&gate, marker, true) {
            ReadGatePoll::GiveUp => {}
            ReadGatePoll::Resolved(s) => panic!("expected GiveUp, got Resolved({:?})", s),
            ReadGatePoll::Pending => panic!("expected GiveUp, got Pending"),
        }
    }

    /// SAFE-1: a read's own stamp is a floor only in the term raft served the
    /// read in, compared on the 32 bits the switch carries. In any other term
    /// (or with no information) it gives no floor, and the assist gate fails
    /// the read through its GiveUp path.
    #[test]
    fn test_floor_applies_only_in_the_stamps_term() {
        let s = SwitchStamp::new(300, 7);
        assert_eq!(s.floor_in_term(7), Some(300));
        // The low 32 bits of a term above 2^32.
        assert_eq!(s.floor_in_term((1 << 32) + 7), Some(300));
        // An earlier or later term.
        assert_eq!(s.floor_in_term(6), None);
        assert_eq!(s.floor_in_term(8), None);
        // Unusable stamps never give a floor, whatever the term.
        assert_eq!(SwitchStamp::new(300, 0).floor_in_term(0), None);
        assert_eq!(SwitchStamp::new(0, 7).floor_in_term(7), None);
    }

    /// SAFE-1: the term a stamp was taken in survives the gate. A read
    /// resolved by its client stamp and one released by a batch each get their
    /// own stamp's term, which is what the floor check compares.
    #[test]
    fn test_resolution_carries_the_stamps_term() {
        let (gate, _rx) = SwitchReadGate::new();
        let client = 0xABCD_0000_0000_0077;
        gate.update_switch_index(client, REGION, SwitchStamp::new(500, 9));
        assert_resolved(poll(&gate, client, false), SwitchStamp::new(500, 9));

        let server = gate.mint_server_marker();
        assert_pending(poll(&gate, server, false));
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![server]);
        gate.release_pending_batches(batch, REGION, SwitchStamp::new(510, 10));
        match poll(&gate, server, false) {
            ReadGatePoll::Resolved(s) => {
                assert_eq!(s, SwitchStamp::new(510, 10));
                assert_eq!(s.floor_in_term(10), Some(510));
                assert_eq!(s.floor_in_term(9), None);
            }
            _ => panic!("batch did not release the read"),
        }
    }
}
