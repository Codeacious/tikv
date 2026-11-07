// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    mem,
    sync::{
        atomic::AtomicBool,
        mpsc::{self, RecvTimeoutError},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use futures::executor::block_on;
use kvproto::{raft_cmdpb::RaftCmdResponse, raft_serverpb::RaftMessage};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::{store::ReadIndexContext, Result};
use test_raftstore::{Simulator as S1, *};
use test_raftstore_macro::test_case;
use tikv_util::{config::*, future::block_on_timeout, time::Instant, HandyRwLock};
use txn_types::{Key, Lock, LockType};
use uuid::Uuid;

#[derive(Default)]
struct CommitToFilter {
    // map[peer_id] -> committed index.
    committed: Arc<Mutex<HashMap<u64, u64>>>,
}

impl CommitToFilter {
    fn new(committed: Arc<Mutex<HashMap<u64, u64>>>) -> Self {
        Self { committed }
    }
}

impl Filter for CommitToFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut committed = self.committed.lock().unwrap();
        for msg in msgs.iter_mut() {
            let cmt = msg.get_message().get_commit();
            if cmt != 0 {
                let to = msg.get_message().get_to();
                committed.insert(to, cmt);
                msg.mut_message().set_commit(0);
            }
        }
        Ok(())
    }

    fn after(&self, _: Result<()>) -> Result<()> {
        Ok(())
    }
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_replica_read_not_applied() {
    let mut cluster = new_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(30));
    let max_lease = Duration::from_secs(1);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(max_lease);
    // After the leader has committed to its term, pending reads on followers can be
    // responsed. However followers can receive `ReadIndexResp` after become
    // candidate if the leader has hibernated. So, disable the feature to avoid
    // read requests on followers to be cleared as stale.
    cluster.cfg.raft_store.hibernate_regions = false;

    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Add a filter to forbid peer 2 and 3 to know the last entry is committed.
    let committed_indices = Arc::new(Mutex::new(HashMap::default()));
    let filter = Box::new(CommitToFilter::new(committed_indices));
    cluster.sim.wl().add_send_filter(1, filter);

    cluster.must_put(b"k1", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Add a filter to forbid the new leader to commit its first entry.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_recv_filter(2, filter);

    cluster.must_transfer_leader(1, new_peer(2, 2));
    let r1 = cluster.get_region(b"k1");

    // Read index on follower should be blocked instead of get an old value.
    let mut resp1_ch =
        async_read_on_peer(&mut cluster, new_peer(3, 3), r1.clone(), b"k1", true, true);
    block_on_timeout(resp1_ch.as_mut(), Duration::from_secs(1)).unwrap_err();

    // Unpark all append responses so that the new leader can commit its first
    // entry.
    let router = cluster.sim.wl().get_router(2).unwrap();
    for raft_msg in mem::take::<Vec<_>>(dropped_msgs.lock().unwrap().as_mut()) {
        #[allow(clippy::useless_conversion)]
        router.send_raft_message(raft_msg.into()).unwrap();
    }

    // The old read index request won't be blocked forever as it's retried
    // internally.
    cluster.sim.wl().clear_send_filters(1);
    cluster.sim.wl().clear_recv_filters(2);
    let resp1 = block_on_timeout(resp1_ch, Duration::from_secs(6)).unwrap();
    let exp_value = resp1.get_responses()[0].get_get().get_value();
    assert_eq!(exp_value, b"v2");

    // New read index requests can be resolved quickly.
    let resp2_ch = async_read_on_peer(&mut cluster, new_peer(3, 3), r1, b"k1", true, true);
    let resp2 = block_on_timeout(resp2_ch, Duration::from_secs(3)).unwrap();
    let exp_value = resp2.get_responses()[0].get_get().get_value();
    assert_eq!(exp_value, b"v2");
}

#[test]
fn test_replica_read_on_hibernate() {
    let mut cluster = new_node_cluster(0, 3);

    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));

    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let filter = Box::new(
        RegionPacketFilter::new(1, 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgReadIndex),
    );
    cluster.sim.wl().add_recv_filter(3, filter);
    cluster.must_transfer_leader(1, new_peer(3, 3));

    let r1 = cluster.get_region(b"k1");

    // Read index on follower should be blocked.
    let mut resp1_ch = async_read_on_peer(&mut cluster, new_peer(1, 1), r1, b"k1", true, true);
    block_on_timeout(resp1_ch.as_mut(), Duration::from_secs(1)).unwrap_err();

    let (tx, rx) = mpsc::sync_channel(1024);
    let cb = Arc::new(move |msg: &RaftMessage| {
        let _ = tx.send(msg.clone());
    }) as Arc<dyn Fn(&RaftMessage) + Send + Sync>;
    for i in 1..=3 {
        let filter = Box::new(
            RegionPacketFilter::new(1, i)
                .when(Arc::new(AtomicBool::new(false)))
                .set_msg_callback(Arc::clone(&cb)),
        );
        cluster.sim.wl().add_send_filter(i, filter);
    }

    // In the loop, peer 1 will keep sending read index messages to 3,
    // but peer 3 and peer 2 will hibernate later. So, peer 1 will start
    // a new election finally because it always ticks.
    let start = Instant::now();
    loop {
        if start.saturating_elapsed() >= Duration::from_secs(6) {
            break;
        }
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(m) => {
                let m = m.get_message();
                if m.get_msg_type() == MessageType::MsgRequestPreVote && m.from == 1 {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => panic!("shouldn't hibernate"),
            Err(_) => unreachable!(),
        }
    }
}

#[test]
fn test_read_hibernated_region() {
    let mut cluster = new_node_cluster(0, 3);
    // Initialize the cluster.
    configure_for_lease_read(&mut cluster.cfg, Some(100), Some(8));
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(Duration::from_millis(1));
    cluster.cfg.raft_store.check_leader_lease_interval = ReadableDuration::hours(10);
    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    let region = cluster.get_region(b"k0");
    cluster.must_transfer_leader(region.get_id(), p3);
    // Make sure leader writes the data.
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
    // Wait for region is hibernated.
    thread::sleep(Duration::from_secs(1));
    cluster.stop_node(2);
    cluster.run_node(2).unwrap();

    let store2_sent_msgs = Arc::new(Mutex::new(Vec::new()));
    let filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Send)
            .reserve_dropped(Arc::clone(&store2_sent_msgs)),
    );
    cluster.sim.wl().add_send_filter(2, filter);
    cluster.pd_client.trigger_leader_info_loss();
    // This request will fail because no valid leader.
    let resp1_ch = async_read_on_peer(&mut cluster, p2.clone(), region.clone(), b"k1", true, true);
    let resp1 = block_on_timeout(resp1_ch, Duration::from_secs(5)).unwrap();
    assert!(
        resp1.get_header().get_error().has_not_leader(),
        "{:?}",
        resp1.get_header()
    );
    thread::sleep(Duration::from_millis(300));
    cluster.sim.wl().clear_send_filters(2);
    let mut has_extra_message = false;
    for msg in std::mem::take(&mut *store2_sent_msgs.lock().unwrap()) {
        let to_store = msg.get_to_peer().get_store_id();
        assert_ne!(to_store, 0, "{:?}", msg);
        if to_store == 3 && msg.has_extra_msg() {
            has_extra_message = true;
        }
        let router = cluster.sim.wl().get_router(to_store).unwrap();
        router.send_raft_message(msg).unwrap();
    }
    // Had a wakeup message from 2 to 3.
    assert!(has_extra_message);
    // Wait for the leader is woken up.
    thread::sleep(Duration::from_millis(500));
    let resp2_ch = async_read_on_peer(&mut cluster, p2, region, b"k1", true, true);
    let resp2 = block_on_timeout(resp2_ch, Duration::from_secs(5)).unwrap();
    assert!(!resp2.get_header().has_error(), "{:?}", resp2);
}

/// The read index response can advance the commit index.
/// But in previous implementation, we forget to set term in read index response
/// which causes panic in raft-rs. This test is to reproduce the case.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_replica_read_on_stale_peer() {
    let mut cluster = new_cluster(0, 3);

    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(30));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();

    let peer_on_store1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    let peer_on_store3 = find_peer(&region, 3).unwrap().to_owned();

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    );
    cluster.sim.wl().add_recv_filter(3, filter);
    cluster.must_put(b"k2", b"v2");
    let resp1_ch = async_read_on_peer(&mut cluster, peer_on_store3, region, b"k2", true, true);
    // must be timeout
    block_on_timeout(resp1_ch, Duration::from_micros(100)).unwrap_err();
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_read_index_out_of_order() {
    let mut cluster = new_cluster(0, 2);

    // Use long election timeout and short lease.
    configure_for_lease_read(&mut cluster.cfg, Some(1000), Some(10));
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(Duration::from_millis(100));

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = cluster.run_conf_change();
    pd_client.must_add_peer(rid, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    let filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgHeartbeatResponse),
    );
    cluster.sim.wl().add_recv_filter(1, filter);

    // Can't get read resonse because heartbeat responses are blocked.
    let r1 = cluster.get_region(b"k1");
    let mut resp1 = async_read_on_peer(&mut cluster, new_peer(1, 1), r1.clone(), b"k1", true, true);
    block_on_timeout(resp1.as_mut(), Duration::from_secs(2)).unwrap_err();

    pd_client.must_remove_peer(rid, new_peer(2, 2));

    // After peer 2 is removed, we can get 2 read responses.
    let resp2 = async_read_on_peer(&mut cluster, new_peer(1, 1), r1, b"k1", true, true);
    block_on_timeout(resp2, Duration::from_secs(1)).unwrap();
    block_on_timeout(resp1, Duration::from_secs(1)).unwrap();
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_read_index_retry_lock_checking() {
    let mut cluster = new_cluster(0, 2);

    // Use long election timeout and short lease.
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(Duration::from_millis(100));

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = cluster.run_conf_change();
    pd_client.must_add_peer(rid, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // block the follower from receiving read index resp first
    let filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgReadIndexResp),
    );
    cluster.sim.wl().add_recv_filter(2, filter);

    // Can't get response because read index responses are blocked.
    let r1 = cluster.get_region(b"k1");
    let mut resp1 = async_read_index_on_peer(&mut cluster, new_peer(2, 2), r1.clone(), b"k1", true);
    let mut resp2 = async_read_index_on_peer(&mut cluster, new_peer(2, 2), r1, b"k2", true);
    block_on_timeout(resp1.as_mut(), Duration::from_secs(2)).unwrap_err();
    block_on_timeout(resp2.as_mut(), Duration::from_millis(1)).unwrap_err();

    // k1 has a memory lock
    let leader_cm = cluster.sim.rl().get_concurrency_manager(1);
    let lock = Lock::new(
        LockType::Put,
        b"k1".to_vec(),
        10.into(),
        20000,
        None,
        10.into(),
        1,
        20.into(),
        false,
    )
    .use_async_commit(vec![]);
    let guard = block_on(leader_cm.lock_key(&Key::from_raw(b"k1")));
    guard.with_lock(|l| *l = Some(lock.clone()));

    // clear filters, so later read index responses can be received
    cluster.sim.wl().clear_recv_filters(2);
    // resp1 should contain key is locked error
    assert!(
        block_on_timeout(resp1, Duration::from_secs(2))
            .unwrap()
            .responses[0]
            .get_read_index()
            .has_locked()
    );
    // resp2 should has a successful read index
    let resp = block_on_timeout(resp2, Duration::from_secs(2)).unwrap();
    assert!(
        !resp.get_header().has_error()
            && resp
                .get_responses()
                .first()
                .map_or(true, |r| !r.get_read_index().has_locked()),
        "{:?}",
        resp,
    );
}

fn must_read_value(resp: &RaftCmdResponse, value: &[u8]) {
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), value);
}

// LIVE-1: a replica read without a lock-check request whose read index answer
// is lost gets moved past by a later read's answer. Before the fix the follower
// popped it without an index and aborted on
// `assert!(read.read_index.is_some())`. Now it is re-proposed and both reads
// succeed. raftstore v1 only (the fix is v1-only; raftstore-v2 ignores
// `read_mode`).
#[test]
fn test_replica_read_jumped_over_after_lost_answer() {
    let mut cluster = new_node_cluster(0, 2);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));
    // Keep the retry timer out of the way: only the later read's answer may
    // move the queue past the first read.
    cluster.cfg.raft_store.raft_read_index_retry_interval_ticks = 1000;

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = cluster.run_conf_change();
    pd_client.must_add_peer(rid, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Drop the leader's answer to the first replica read.
    let filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgReadIndexResp),
    );
    cluster.sim.wl().add_recv_filter(2, filter);
    let r1 = cluster.get_region(b"k1");
    let mut resp1 = async_read_on_peer(&mut cluster, new_peer(2, 2), r1.clone(), b"k1", true, true);
    block_on_timeout(resp1.as_mut(), Duration::from_millis(300)).unwrap_err();

    // The second read's answer is the first to arrive and moves the queue past
    // the first read, which must be re-proposed rather than popped without an
    // index.
    cluster.sim.wl().clear_recv_filters(2);
    let resp2 = async_read_on_peer(&mut cluster, new_peer(2, 2), r1, b"k1", true, true);
    must_read_value(
        &block_on_timeout(resp2, Duration::from_secs(1)).unwrap(),
        b"v1",
    );
    must_read_value(
        &block_on_timeout(resp1, Duration::from_secs(1)).unwrap(),
        b"v1",
    );
}

// LIVE-1 under `grant-leases`: a forwarded replica read is still waiting for
// the leader when a later read is served from the follower's own lease, so the
// later read's ReadState comes first. The leader's answers never reach the
// follower and the retry timer is disabled, so the forwarded reads can complete
// only by being re-proposed and served from the lease. raftstore v1 only.
#[test]
fn test_replica_read_forwarded_then_lease_served() {
    // No switch gate in test clusters, so every re-proposal is lease-eligible.
    let reproposed = || {
        raftstore::store::metrics::RAFT_READ_INDEX_REPROPOSED_TOTAL
            .with_label_values(&["lease_eligible"])
            .get()
    };
    let mut cluster = new_node_cluster(0, 2);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));
    cluster.cfg.raft_store.read_mode = "grant-leases".to_owned();
    cluster.cfg.raft_store.raft_read_index_retry_interval_ticks = 1000;

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let no_grants = || {
        Box::new(
            RegionPacketFilter::new(1, 2)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgAskReadLeaseResp),
        )
    };
    let no_answers = || {
        Box::new(
            RegionPacketFilter::new(1, 2)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgReadIndexResp),
        )
    };

    // Peer 2 asks for a lease as soon as it joins, so hold back its grants
    // until its first read has been forwarded.
    let rid = cluster.run_conf_change();
    cluster.sim.wl().add_recv_filter(2, no_grants());
    pd_client.must_add_peer(rid, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Every answer the leader sends to peer 2 is dropped for the rest of the
    // test.
    cluster.sim.wl().add_recv_filter(2, no_answers());

    // Peer 2 holds no lease, so the first read is forwarded and its answer
    // dropped.
    let r1 = cluster.get_region(b"k1");
    let mut first = async_read_on_peer(&mut cluster, new_peer(2, 2), r1.clone(), b"k1", true, true);
    block_on_timeout(first.as_mut(), Duration::from_millis(300))
        .expect_err("the first read must be forwarded, not served from a lease");

    // Let grants through again; answers to peer 2 stay dropped.
    cluster.sim.wl().clear_recv_filters(2);
    cluster.sim.wl().add_recv_filter(2, no_answers());

    // Read until one is served from the lease. Reads issued before the lease is
    // granted are forwarded and wait like the first one.
    let before = reproposed();
    let mut waiting = vec![first];
    let mut served = None;
    for _ in 0..30 {
        let mut resp =
            async_read_on_peer(&mut cluster, new_peer(2, 2), r1.clone(), b"k1", true, true);
        match block_on_timeout(resp.as_mut(), Duration::from_millis(100)) {
            Ok(r) => {
                served = Some(r);
                break;
            }
            Err(_) => waiting.push(resp),
        }
    }
    must_read_value(
        &served.expect("no read was served from the follower lease"),
        b"v1",
    );

    // Every read queued ahead of the lease-served one, the first included, was
    // moved past without an index and must complete through a re-propose. The
    // counter is process-wide, so other tests can only raise it.
    let n = waiting.len() as u64;
    for resp in waiting {
        must_read_value(
            &block_on_timeout(resp, Duration::from_secs(1)).unwrap(),
            b"v1",
        );
    }
    assert!(
        reproposed() - before >= n,
        "every forwarded read must go through a re-propose"
    );
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_split_isolation() {
    let mut cluster = new_cluster(0, 2);
    // Use long election timeout and short lease.
    configure_for_hibernate(&mut cluster.cfg);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(11);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = cluster.run_conf_change();
    pd_client.must_add_peer(rid, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.stop_node(2);
    // Split region into ['', 'k2') and ['k2', '')
    let r1 = cluster.get_region(b"k2");
    cluster.must_split(&r1, b"k2");
    let idx = cluster.truncated_state(1, 1).get_index();
    // Trigger a log compaction, so the left region ['', 'k2'] cannot created
    // through split cmd.
    for i in 2..cluster.cfg.raft_store.raft_log_gc_count_limit() * 2 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    cluster.wait_log_truncated(1, 1, idx + 1);
    // Wait till leader peer goes to sleep again.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    let mut peer = None;
    let r2 = cluster.get_region(b"k1");
    for p in r2.get_peers() {
        if p.store_id == 2 {
            peer = Some(p.clone());
            break;
        }
    }
    let peer = peer.unwrap();
    cluster.run_node(2).unwrap();
    // Originally leader of region ['', 'k2'] will go to sleep, so the learner peer
    // cannot be created.
    let start = Instant::now();
    loop {
        let resp = async_read_on_peer(&mut cluster, peer.clone(), r2.clone(), b"k1", true, true);
        let resp = block_on_timeout(resp, Duration::from_secs(1)).unwrap();
        if !resp.get_header().has_error() {
            return;
        }
        if start.saturating_elapsed() > Duration::from_secs(5) {
            panic!("test failed: {:?}", resp);
        }
        thread::sleep(Duration::from_millis(200));
    }
}

/// Testing after applying snapshot, the `ReadDelegate` stored at `StoreMeta`
/// will be replace with the new `ReadDelegate`, and the `ReadDelegate` stored
/// at `LocalReader` should also be updated
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_read_local_after_snapshot_replace_peer() {
    let mut cluster = new_cluster(0, 3);
    configure_for_lease_read(&mut cluster.cfg, Some(50), None);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    for i in 1..=3 {
        must_get_equal(&cluster.get_engine(i), b"k1", b"v1");
    }

    // send read request to peer 3, so the local reader will cache the
    // `ReadDelegate` of peer 3 it is okay only send one request because the
    // read pool thread count is 1
    let r = cluster.get_region(b"k1");
    // wait applying snapshot finish
    sleep_ms(100);
    let resp = async_read_on_peer(&mut cluster, new_peer(3, 3), r, b"k1", true, true);
    let resp = block_on_timeout(resp, Duration::from_secs(1)).unwrap();
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");

    // trigger leader send snapshot to peer 3
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 0..12 {
        cluster.must_put(format!("k2{}", i).as_bytes(), b"v2");
    }
    cluster.clear_send_filters();
    // wait peer 3 apply snapshot and replace the `ReadDelegate` on `StoreMeta`
    must_get_equal(&cluster.get_engine(3), b"k20", b"v2");

    // replace peer 3 with peer 1003
    cluster
        .pd_client
        .must_remove_peer(region_id, new_peer(3, 3));
    cluster
        .pd_client
        .must_add_peer(region_id, new_peer(3, 1003));

    cluster.must_put(b"k3", b"v3");
    // wait peer 1003 apply snapshot
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
    // value can be readed from `engine` doesn't mean applying snapshot is finished
    // wait little more time
    sleep_ms(100);

    let r = cluster.get_region(b"k1");
    let resp = async_read_on_peer(&mut cluster, new_peer(3, 1003), r, b"k3", true, true);
    let resp = block_on_timeout(resp, Duration::from_secs(1)).unwrap();
    // should not have `mismatch peer id` error
    if resp.get_header().has_error() {
        panic!("unexpected err: {:?}", resp.get_header().get_error());
    }
    let exp_value = resp.get_responses()[0].get_get().get_value();
    assert_eq!(exp_value, b"v3");
}

/// The case checks if a malformed request should not corrupt the leader's read
/// queue.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_malformed_read_index() {
    let mut cluster = new_cluster(0, 3);
    configure_for_lease_read(&mut cluster.cfg, Some(50), None);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
    cluster.cfg.raft_store.hibernate_regions = true;
    cluster.cfg.raft_store.check_leader_lease_interval = ReadableDuration::hours(10);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    for i in 1..=3 {
        must_get_equal(&cluster.get_engine(i), b"k1", b"v1");
    }

    // Wait till lease expires.
    std::thread::sleep(
        cluster
            .cfg
            .raft_store
            .raft_store_max_leader_lease()
            .to_std()
            .unwrap(),
    );
    let region = cluster.get_region(b"k1");
    // Send a malformed request to leader
    let mut raft_msg = raft::eraftpb::Message::default();
    raft_msg.set_msg_type(MessageType::MsgReadIndex);
    let rctx = ReadIndexContext {
        id: Uuid::new_v4(),
        request: None,
        locked: None,
    };
    let mut e = raft::eraftpb::Entry::default();
    e.set_data(rctx.to_bytes().into());
    raft_msg.mut_entries().push(e);
    raft_msg.from = 1;
    raft_msg.to = 1;
    let mut message = RaftMessage::default();
    message.set_region_id(region_id);
    message.set_from_peer(new_peer(1, 1));
    message.set_to_peer(new_peer(1, 1));
    message.set_region_epoch(region.get_region_epoch().clone());
    message.set_message(raft_msg);
    // So the read won't be handled soon.
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    cluster.send_raft_msg(message).unwrap();
    // Also send a correct request. If the malformed request doesn't corrupt
    // the read queue, the correct request should be responded.
    let resp = async_read_on_peer(&mut cluster, new_peer(1, 1), region, b"k1", true, false);
    cluster.clear_send_filters();
    let resp = block_on_timeout(resp, Duration::from_secs(10)).unwrap();
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_replica_read_with_pending_peer() {
    let mut cluster = new_cluster(0, 3);

    cluster.cfg.tikv.raft_store.raft_log_gc_count_limit = Some(100);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(100));

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r = cluster.run_conf_change();
    assert_eq!(r, 1);
    pd_client.must_add_peer(1, new_peer(2, 2));
    pd_client.must_add_peer(1, new_peer(3, 3));

    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Make sure the peer 3 exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Make sure the peer 3 is pending
    let new_region = cluster.get_region(b"k1");
    let filter = Box::new(
        RegionPacketFilter::new(new_region.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .msg_type(MessageType::MsgSnapshot),
    );
    cluster.sim.wl().add_recv_filter(3, filter);
    cluster.must_put(b"k1", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    for i in 0..200 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v2");
    }

    let new_region = cluster.get_region(b"k1");
    let resp_ch = async_read_on_peer(&mut cluster, new_peer(3, 3), new_region, b"k1", true, true);

    let response = block_on_timeout(resp_ch, Duration::from_secs(3)).unwrap();
    assert!(response.get_header().get_error().has_read_index_not_ready());
}

/// EXP-1 (review tack-08 F9): a switch credit reaches raft-rs through
/// raftstore's own checks (`validate_raft_msg`, `check_msg`) only if it
/// carries the region epoch. Follower 3 holds a lease and none of its acks
/// reach the leader, so the commit clamp holds a write back (3 keeps renewing,
/// as it stays within the catch-up margin; raft-log GC is off, because its
/// CompactLog entries, every 100 ms in the test config, would soon carry the
/// quorum index past that margin, and LIVE-2's renewal check would then rightly
/// refuse 3 and end the hold). A Term-0 `MsgAskAckIndexResp` for
/// 3, built as the sidechannel's `feed_reflected_ack` builds it and injected
/// at the leader's store, releases the write. The same credit without an
/// epoch, as every credit was built before EXP-1, is dropped and releases
/// nothing.
#[test]
fn test_switch_credit_reaches_raft_through_raftstore() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(20));
    cluster.cfg.raft_store.read_mode = "grant-leases".to_owned();
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::hours(1);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k0", b"v0");
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
    // Let the followers' lease asks be granted.
    thread::sleep(Duration::from_millis(500));

    // From here on 3 appends but its acks never reach the leader. Its lease
    // asks and heartbeat responses still do.
    for t in [
        MessageType::MsgAppendResponse,
        MessageType::MsgAskAckIndexResp,
    ] {
        cluster.add_send_filter(CloneFilterFactory(
            RegionPacketFilter::new(1, 3)
                .direction(Direction::Send)
                .msg_type(t),
        ));
    }
    let mut put = cluster.async_put(b"k1", b"v1").unwrap();
    block_on_timeout(put.as_mut(), Duration::from_secs(1))
        .expect_err("the clamp should hold the write while 3 holds a lease");

    let state = cluster.raft_local_state(1, 1);
    let (term, last) = (state.get_hard_state().get_term(), state.get_last_index());
    let credit = |epoch: Option<kvproto::metapb::RegionEpoch>| {
        let mut m = raft::eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgAskAckIndexResp);
        m.set_from(3);
        m.set_to(1);
        m.set_index(last);
        m.set_log_term(term); // term 0: a switch credit, in the register's term
        let mut rm = RaftMessage::default();
        rm.set_region_id(1);
        rm.set_from_peer(new_peer(3, 3));
        rm.set_to_peer(new_peer(1, 1));
        if let Some(e) = epoch {
            rm.set_region_epoch(e);
        }
        rm.set_message(m);
        rm
    };

    cluster.send_raft_msg(credit(None)).unwrap();
    block_on_timeout(put.as_mut(), Duration::from_secs(1))
        .expect_err("a credit without an epoch must be dropped by raftstore");

    let epoch = cluster.get_region(b"k1").get_region_epoch().clone();
    cluster.send_raft_msg(credit(Some(epoch))).unwrap();
    let resp = block_on_timeout(put.as_mut(), Duration::from_secs(3))
        .expect("a credit with the epoch should lift the clamp");
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}
