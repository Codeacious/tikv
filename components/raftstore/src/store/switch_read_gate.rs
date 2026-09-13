// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared state for the P4-switch-assisted follower read gate ("assist" mode).
//!
//! Owns the read-path half of sidechannel Flow 2 (see
//! `src/server/udp_sidechannel.rs`). Two touch-points, both inside raftstore's
//! `Peer`:
//!
//!   * propose-time hint — stamp the read-index with the region's switch index
//!     (`read_index_hint`), and
//!   * serve-time gate  — hold a marked replica read until the switch answers
//!     that read's marker (`poll_read_gate`), then serve at or above the
//!     answered index.
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
/// which serves 0 for a slot holding another region (or none). It is also what a
/// switch that rebooted answers until the next tapped `MsgAppend` rewrites its register.
///
/// A 0 never resolves a gated read and is never cached as a region's latest
/// index: the gate fails closed and the read waits or gives up.
const INVALID_SWITCH_INDEX: u64 = 0;

/// Propose-time hint for a region the switch has told us nothing about
/// (`latest_switch_index == INVALID_SWITCH_INDEX`).
///
/// The hint travels to raft-rs as `MsgReadIndex.commit`: an index this node must
/// have committed before a lease holder may serve the read locally. Naming an
/// index no node can have committed makes raft's ordinary rules forward the read
/// to the leader, with no switch-shaped special case in the library.
const UNKNOWN_SWITCH_INDEX_HINT: u64 = u64::MAX;

/// Outcome of a serve-gate poll (`SwitchReadGate::poll_read_gate`).
pub enum ReadGatePoll {
    /// The switch confirmed this marker at this switch index (always nonzero).
    /// The caller serves the read at *or above* it.
    Resolved(u64),
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
    /// Latest switch-confirmed quorum ack index, per region. The `RwLock` guards
    /// only the map container; per-region values are lock-free atomics, so
    /// different regions never serialize on the hot path.
    latest_index: RwLock<HashMap<u64, Arc<AtomicU64>>>,
    /// Counter for server-minted markers and batch ids. Server markers are
    /// masked into the low 32 bits so they cannot collide with client-minted
    /// markers, which have nonzero upper 32 bits.
    marker_seq: AtomicU64,
    /// Markers registered by the serve gate and awaiting confirmation, mapped to
    /// the region each read concerns.
    pending: Mutex<HashMap<u64, u64>>,
    /// Confirmed marker -> switch index, consumed by the next serve-gate poll.
    resolved: Mutex<HashMap<u64, u64>>,
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
            latest_index: RwLock::new(HashMap::new()),
            // Start at 1 so 0 stays a reserved "no marker" sentinel.
            marker_seq: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            resolved: Mutex::new(HashMap::new()),
            pending_batches: Mutex::new(Vec::new()),
            pending_markers_tx: tx,
        });
        (gate, rx)
    }

    /// Returns the region's latest-index atomic, creating it under a brief write
    /// lock on first sight.
    fn region_index(&self, region_id: u64) -> Arc<AtomicU64> {
        if let Some(a) = self.latest_index.read().unwrap().get(&region_id) {
            return a.clone();
        }
        self.latest_index
            .write()
            .unwrap()
            .entry(region_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    /// Latest switch-confirmed quorum ack index for `region_id`, or
    /// `INVALID_SWITCH_INDEX` if never confirmed. Does not insert a map entry.
    /// The raw cache; `read_index_hint` is the propose-time view of it.
    pub fn latest_switch_index(&self, region_id: u64) -> u64 {
        self.latest_index
            .read()
            .unwrap()
            .get(&region_id)
            .map(|a| a.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// The value to stamp on this region's `MsgReadIndex` (raft-rs
    /// `RawNode::read_index_switch_hint`): the highest index any in-path switch
    /// has reflected to us for the region, or `UNKNOWN_SWITCH_INDEX_HINT` when
    /// the cache is still empty, which forwards the read to the leader.
    ///
    /// "Never stamped" and "the switch has nothing saved right now" are the same
    /// condition and take the same path — the monotone cache already carries
    /// both, so there is no per-region "ever stamped" bookkeeping and must not
    /// be.
    pub fn read_index_hint(&self, region_id: u64) -> u64 {
        match self.latest_switch_index(region_id) {
            INVALID_SWITCH_INDEX => UNKNOWN_SWITCH_INDEX_HINT,
            idx => idx,
        }
    }

    /// Mints a server-side marker for a marker-less read, in the low 32-bit
    /// namespace so it never collides with a client-minted marker.
    pub fn mint_server_marker(&self) -> u64 {
        self.marker_seq.fetch_add(1, Ordering::Relaxed) & 0x0000_0000_FFFF_FFFF
    }

    /// Non-blocking serve-gate poll. Returns `Resolved(switch_index)` once the
    /// switch has confirmed `marker`; otherwise registers it (issuing a batched
    /// local query on first sight) and returns `Pending`, so the caller re-queues
    /// the read — the raftstore poller thread is never parked.
    ///
    /// `expired` is set by the caller once the read has waited past
    /// `READ_GATE_GIVE_UP`; an expired, still-unresolved marker is dropped from
    /// tracking and `GiveUp` returned.
    pub fn poll_read_gate(&self, marker: u64, region_id: u64, expired: bool) -> ReadGatePoll {
        // Acquire `resolved` before `pending`, matching `update_switch_index`.
        let mut resolved = self.resolved.lock().unwrap();
        // A resolution at `INVALID_SWITCH_INDEX` carries no index floor, so it is
        // not a resolution. Both writers already refuse to record one; restated
        // here at the single consumption point.
        if let Some(idx) = resolved.remove(&marker)
            && idx != INVALID_SWITCH_INDEX
        {
            drop(resolved);
            self.pending.lock().unwrap().remove(&marker);
            return ReadGatePoll::Resolved(idx);
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
        let newly = self.pending.lock().unwrap().insert(marker, region_id).is_none();
        drop(resolved);
        if newly {
            // Unbounded, so this never blocks; the query loop is the batching
            // point.
            let _ = self.pending_markers_tx.try_send((marker, region_id));
        }
        ReadGatePoll::Pending
    }

    /// Bumps `region_id`'s latest switch index and, for a nonzero marker,
    /// resolves it. Called by the socket side on inbound `MsgReadIndex` (client,
    /// switch-tagged) and — with `marker == 0` — on `MsgAskAckIndexResp`
    /// (latest-index bump only; batch release is separate).
    pub fn update_switch_index(&self, marker: u64, region_id: u64, switch_index: u64) {
        // A zero carries no information (see `INVALID_SWITCH_INDEX`): don't cache
        // it, don't let it resolve a gated read.
        if switch_index == INVALID_SWITCH_INDEX {
            return;
        }
        // region_id 0 is the placeholder sentinel and the switch always serves 0
        // for it, so skip the cache entry. The marker is still resolved below, so
        // a placeholder read falls through to the host-ack path instead of gating
        // until give-up.
        if region_id != 0 {
            self.region_index(region_id)
                .fetch_max(switch_index, Ordering::Relaxed);
        }
        if marker == 0 {
            return;
        }
        let mut resolved = self.resolved.lock().unwrap();
        self.pending.lock().unwrap().remove(&marker);
        resolved.insert(marker, switch_index);
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
    /// answered too), resolving their still-pending markers at `answered_index`.
    /// A response for one region never releases another region's reads.
    ///
    /// `answered_index == INVALID_SWITCH_INDEX` releases nothing — there is no
    /// floor to hand out — and the markers stay pending for the query loop to retry.
    ///
    /// The floor is `answered_index`, not the cached `latest_switch_index`: a
    /// higher cache only means another read sampled the same monotone register later.
    pub fn release_pending_batches(&self, batch_marker: u64, region_id: u64, answered_index: u64) {
        if batch_marker == 0 || answered_index == INVALID_SWITCH_INDEX {
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
                    resolved.insert(marker, answered_index);
                }
            }
        }
    }
}

/// Process-global switch read gate, published by the sidechannel at startup and
/// read back by every `Peer`. `None` when `read_mode != "assist"`, in which case
/// the read path is unchanged.
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
/// `MsgAppend`, so without this a switch that reboots during a read-only stretch
/// never recovers. Recovery, not safety.
///
/// Implemented by `UdpSidechannel` in the `tikv` crate and published here at
/// startup, like `SwitchReadGate`; the caller is a raftstore `PeerTick`.
pub trait SwitchRegisterRefresher: Send + Sync + 'static {
    /// For each attached peer, if that peer's switch has said nothing about
    /// `region_id` since the previous call, re-send the highest proposed index
    /// already sent that peer for that region. A peer whose switch is talking,
    /// or for which nothing was ever tapped for this region, is skipped.
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

    fn poll(gate: &SwitchReadGate, marker: u64, expired: bool) -> ReadGatePoll {
        gate.poll_read_gate(marker, REGION, expired)
    }

    fn assert_pending(poll: ReadGatePoll) {
        match poll {
            ReadGatePoll::Pending => {}
            ReadGatePoll::Resolved(i) => panic!("expected Pending, got Resolved({})", i),
            ReadGatePoll::GiveUp => panic!("expected Pending, got GiveUp"),
        }
    }

    fn assert_resolved(poll: ReadGatePoll, want: u64) {
        match poll {
            ReadGatePoll::Resolved(i) => assert_eq!(i, want),
            ReadGatePoll::Pending => panic!("expected Resolved({}), got Pending", want),
            ReadGatePoll::GiveUp => panic!("expected Resolved({}), got GiveUp", want),
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
        gate.update_switch_index(marker, REGION, 0);
        assert_pending(poll(&gate, marker, false));

        // A real answer releases it, at that index.
        gate.update_switch_index(marker, REGION, 42);
        assert_resolved(poll(&gate, marker, false), 42);
    }

    /// A 0 neither raises the cache from 0 nor lowers it once real. Contract test
    /// only — the `fetch_max` underneath already ignores a 0.
    #[test]
    fn test_zero_switch_index_is_not_cached() {
        let (gate, _rx) = SwitchReadGate::new();
        gate.update_switch_index(0, REGION, 0);
        assert_eq!(gate.latest_switch_index(REGION), 0);

        gate.update_switch_index(0, REGION, 90);
        assert_eq!(gate.latest_switch_index(REGION), 90);

        // A later reboot answer neither lowers nor clears the cache.
        gate.update_switch_index(0, REGION, 0);
        assert_eq!(gate.latest_switch_index(REGION), 90);
    }

    /// The batched path fails closed on a 0 answer the same way the stamp path
    /// does.
    #[test]
    fn test_zero_answer_does_not_release_pending_batches() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));

        // This store saw a high value before the switch rebooted.
        gate.update_switch_index(0, REGION, 500);

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, 0);
        assert_pending(poll(&gate, marker, false));

        // Once the switch answers with a real index again, the batch releases.
        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, 500);
        assert_resolved(poll(&gate, marker, false), 500);
    }

    /// The floor is this batch's answer, not the region's high-water cache.
    #[test]
    fn test_batch_releases_at_the_answered_index_not_the_cache() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));

        // An unrelated, later sample raises the region cache.
        gate.update_switch_index(0, REGION, 900);

        let batch = gate.next_marker();
        gate.record_pending_batch(batch, REGION, vec![marker]);
        gate.release_pending_batches(batch, REGION, 500);
        assert_resolved(poll(&gate, marker, false), 500);
    }

    /// A region the switch has said nothing about hints an index no node can have
    /// committed, so raft-rs forwards the read. raft-rs twin:
    /// `test_switch_hint_gate`.
    #[test]
    fn test_read_index_hint_is_unreachable_until_the_switch_speaks() {
        let (gate, _rx) = SwitchReadGate::new();
        assert_eq!(gate.read_index_hint(REGION), u64::MAX);

        // A zero answer is not information, so it does not change the hint.
        gate.update_switch_index(0, REGION, 0);
        assert_eq!(gate.read_index_hint(REGION), u64::MAX);

        // A real answer becomes the hint verbatim.
        gate.update_switch_index(0, REGION, 77);
        assert_eq!(gate.read_index_hint(REGION), 77);

        // A later reboot answer neither lowers the cache nor re-arms the
        // unreachable hint.
        gate.update_switch_index(0, REGION, 0);
        assert_eq!(gate.read_index_hint(REGION), 77);

        // The hint is per region, like the cache.
        assert_eq!(gate.read_index_hint(REGION + 1), u64::MAX);
    }

    /// Failing closed still terminates: a read that only ever gets 0 answers
    /// gives up at the deadline.
    #[test]
    fn test_zero_stamp_still_gives_up_when_expired() {
        let (gate, _rx) = SwitchReadGate::new();
        let marker = gate.mint_server_marker();
        assert_pending(poll(&gate, marker, false));
        gate.update_switch_index(marker, REGION, 0);
        match poll(&gate, marker, true) {
            ReadGatePoll::GiveUp => {}
            ReadGatePoll::Resolved(i) => panic!("expected GiveUp, got Resolved({})", i),
            ReadGatePoll::Pending => panic!("expected GiveUp, got Pending"),
        }
    }
}
