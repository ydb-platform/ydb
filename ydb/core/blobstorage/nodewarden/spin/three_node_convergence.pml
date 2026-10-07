#ifndef STORAGE_NODE
#define STORAGE_NODE 1
#endif
#ifndef COMMITTED_MASK
#define COMMITTED_MASK (1 << (STORAGE_NODE - 1))
#endif
#ifndef BOOTSTRAP_ROOT
#define BOOTSTRAP_ROOT (STORAGE_NODE % 3 + 1)
#endif
#ifndef DRIVE_LAYOUT
#define DRIVE_LAYOUT 0 /* 0: one drive per node/DC; 1: storage host has 3/5 drives */
#endif
#ifndef NO_SERVICE_SET_MASK
#define NO_SERVICE_SET_MASK 0 /* generation zero, BlobStorageConfig present, ServiceSet absent */
#endif
#ifndef ONLINE_MASK
#define ONLINE_MASK 7
#endif
#ifndef INITIAL_TREE
#define INITIAL_TREE 1 /* 0: singletons; 1: storage root + two-node tree; 2: joined */
#endif
#ifndef INITIAL_LINKS
#define INITIAL_LINKS 7
#endif
#ifndef FAULT_BUDGET
#define FAULT_BUDGET 0
#endif
#ifndef TIMEOUT_BUDGET
#define TIMEOUT_BUDGET 0
#endif
#ifndef QUEUE_CAPACITY
#define QUEUE_CAPACITY 6
#endif
#ifndef OP_MASK
#define OP_MASK 0 /* roots on these nodes execute opaque operations */
#endif
#ifndef OP_WORKLOAD
#define OP_WORKLOAD 2 /* 1: idle gaps; 2: continuous backlog; 3: no completion */
#endif

#define BIT(n) (1 << ((n) - 1))
#define SLOT(a, b) (3 * ((a) - 1) + (b) - 1)
#define EDGE(a, b) ((a) + (b) - 3)
#define LINK(a, b) (1 << EDGE(a, b))
#define ONLINE(n) (ONLINE_MASK & BIT(n))
#define UP(a, b) (ONLINE(a) && ONLINE(b) && (links & LINK(a, b)))
#define SESSION_UP(a, b, sid) (UP(a, b) && (sid) == session[EDGE(a, b)])
#define MAJORITY(mask) ((mask) == 3 || (mask) == 5 || (mask) == 6 || (mask) == 7)
#define WORKING_CONFIG(n) (COMMITTED_MASK & BIT(n))
#if DRIVE_LAYOUT == 1
#define DRIVE_QUORUM(nodes) ((nodes) & BIT(STORAGE_NODE))
#else
#define DRIVE_QUORUM(nodes) MAJORITY(nodes)
#endif
#define NO_SERVICE_SET(n) (NO_SERVICE_SET_MASK & BIT(n))
#ifdef IGNORE_BOOTSTRAP_NODE_MAJORITY
#define INITIAL_NODE_QUORUM(nodes) true
#elif defined(USE_DRIVES_FOR_BOOTSTRAP)
#define INITIAL_NODE_QUORUM(nodes) DRIVE_QUORUM(nodes)
#else
#define INITIAL_NODE_QUORUM(nodes) MAJORITY(nodes)
#endif
#define BOOTSTRAP_QUORUM(n) ((NO_SERVICE_SET(n) && INITIAL_NODE_QUORUM(subtree[n])) || (!NO_SERVICE_SET(n) && DRIVE_QUORUM(subtree[n])))
#ifdef REQUIRE_WORKING_NODE_MAJORITY
#define GROUP_QUORUM(n) ((subtree[n] & BIT(STORAGE_NODE)) && MAJORITY(subtree[n]))
#else
#define GROUP_QUORUM(n) (subtree[n] & BIT(STORAGE_NODE))
#endif
#define QUORUM(n) ((WORKING_CONFIG(n) && GROUP_QUORUM(n)) || (!WORKING_CONFIG(n) && BOOTSTRAP_QUORUM(n)))
#define SCEPTER(n) (scepters & BIT(n))
#define REQUEST_PEER(n) (probe[n] -> probe[n] : binding[n])
#define WORKING_ROOT(n) (SCEPTER(n) && WORKING_CONFIG(n))
#define ACCEPTED(n) (binding[n] && child_cookie[SLOT(binding[n], n)] == cookie[n] && child_session[SLOT(binding[n], n)] == binding_session[n])
#define ATTACHED(n, r) ((n) == (r) || (ACCEPTED(n) && (binding[n] == (r) || (ACCEPTED(binding[n]) && binding[binding[n]] == (r)))))
#define JOINED(n, r) (!ONLINE(n) || (root[n] == r && ATTACHED(n, r)))
#if COMMITTED_MASK
#define CONVERGED (SCEPTER(STORAGE_NODE) && JOINED(1, STORAGE_NODE) && JOINED(2, STORAGE_NODE) && JOINED(3, STORAGE_NODE))
#else
#if ONLINE_MASK & 1
#define ASSEMBLY_ROOT root[1]
#elif ONLINE_MASK & 2
#define ASSEMBLY_ROOT root[2]
#else
#define ASSEMBLY_ROOT root[3]
#endif
#define CONVERGED (ONLINE(ASSEMBLY_ROOT) && scepters == BIT(ASSEMBLY_ROOT) && JOINED(1, ASSEMBLY_ROOT) && JOINED(2, ASSEMBLY_ROOT) && JOINED(3, ASSEMBLY_ROOT))
#endif
#define COOKIE_COUNT (3 * QUEUE_CAPACITY + 4)
#define OP_COOKIE_COUNT 4 /* two queued completions, one pending source, one fresh name */

#if STORAGE_NODE < 1 || STORAGE_NODE > 3 || ONLINE_MASK < 1 || ONLINE_MASK > 7
#error Invalid storage host or online-node mask
#endif
#if COMMITTED_MASK < 0 || COMMITTED_MASK > 7 || (COMMITTED_MASK && (!(COMMITTED_MASK & BIT(STORAGE_NODE)) || !ONLINE(STORAGE_NODE)))
#error A working configuration requires its storage host to have that configuration and stay online
#endif
#if !COMMITTED_MASK && INITIAL_TREE != 0
#error Bootstrap assembly must start with separate nodes
#endif
#if NO_SERVICE_SET_MASK < 0 || NO_SERVICE_SET_MASK > 7 || (NO_SERVICE_SET_MASK & COMMITTED_MASK)
#error ServiceSet cannot be absent from a working configuration
#endif
#if INITIAL_TREE < 0 || INITIAL_TREE > 2 || INITIAL_LINKS < 0 || INITIAL_LINKS > 7
#error Invalid initial topology
#endif
#if BOOTSTRAP_ROOT < 1 || BOOTSTRAP_ROOT > 3 || BOOTSTRAP_ROOT == STORAGE_NODE
#error BOOTSTRAP_ROOT must name one of the other two nodes
#endif
#if FAULT_BUDGET > 2 || TIMEOUT_BUDGET > 2
#error Fault budgets above two have not been sized for this model
#endif
#if OP_MASK < 0 || OP_MASK > 7 || OP_WORKLOAD < 1 || OP_WORKLOAD > 3
#error Invalid operation workload
#endif

mtype = { Initial, Push, ReversePush, Reject, Unbind, QueryRoot, RootReply, Disconnected, Expired, Wakeup, ErrorTimeout };
chan inbox[4] = [QUEUE_CAPACITY] of { mtype, byte, byte, byte, byte };

byte links = INITIAL_LINKS;
byte session[3];
bool network_stable;
byte faults_left = FAULT_BUDGET;
byte timeouts_left = TIMEOUT_BUDGET;
byte scepters;
byte protected_roots;
#if !COMMITTED_MASK
bool tree_converged;
#endif

byte binding[4];
byte binding_session[4];
byte root[4];
byte subtree[4];
byte child_cookie[9];
byte child_session[9];
byte child_nodes[9];
byte last_root[9];
byte local_session[9];
byte cookie[4];
byte probe[4];
bool awaiting[4];
bool timer_armed[4];
bool request_sent[4];
bool retry_ready[4];
bool wakeup_pending[4];
bool error_wait[4];
bool error_timer_armed[4];
byte next_peer[4];

#if OP_MASK
/* Tokens represent (pipeline generation, actor ID); retired operations may still finish.
 * The backlog is unbounded in duration. */
chan op_done[4] = [2] of { byte };
byte op_current[4];
byte op_pending[4];
byte handoff[4];
byte handoff_session[4];
bool handoff_ready[4];
byte op_used[OP_COOKIE_COUNT + 1];
byte op_index; byte op_count; byte op_name; byte op_selected;
#endif

/* Scratch is confined to atomic handlers and reset before yielding. */
byte f; byte r; byte previous; bool push_dirty;
byte candidate; byte checked; byte target;
byte audit_n;
byte fresh_used[COOKIE_COUNT + 1];
byte fresh_a; byte fresh_k; byte fresh_count; byte fresh_selected;
byte fresh_peer; byte fresh_sid; byte fresh_c; byte fresh_value;
mtype fresh_kind;

/* CHECK_EVENT deliberately fails at the selected witness:
 * 1: request timeout; 2: stale reply; 3: stale timer; 4: disconnect; 5: convergence after timeout;
 * 6: bind timeout; 7: stale bind reply; 8: disconnect during binding; 9: timer after confirmation;
 * 10: fencing a busy root; 11: stale completion while bound; 12: stale completion during a new operation;
 * 13: busy root replies; 14: operation during probe; 15: bind despite unfinished operation;
 * 16: disconnect after fencing; 17: accept queued positive reply after session loss;
 * 18: bind before observing disconnect; 19: quorum loss enters backoff;
 * 20: regain leadership after backoff; 21: binding message during backoff. */
#if CHECK_EVENT == 5
bool had_timeout;
#endif
inline witness(event_id) {
#ifdef CHECK_EVENT
    if
    :: CHECK_EVENT == event_id -> printf("REACHED event=%d\n", event_id); assert(false)
    :: else -> skip
    fi;
#else
    skip
#endif
}

inline fence_operations(n) {
#if OP_MASK
    op_current[n] = 0
#else
    skip
#endif
}

inline cancel_handoff(n) {
#if OP_MASK
    handoff[n] = 0;
    handoff_session[n] = 0;
    handoff_ready[n] = false
#else
    skip
#endif
}

#if OP_MASK
inline start_operation(n) {
    d_step {
        assert(!op_current[n] && !op_pending[n] && SCEPTER(n) && !binding[n]);
        for (op_index : 0 .. OP_COOKIE_COUNT) { op_used[op_index] = 0 };
        op_count = len(op_done[n]); op_index = 0;
        do
        :: op_index < op_count ->
            op_done[n]?op_name;
            op_used[op_name] = 1;
            op_done[n]!op_name;
            op_index++
        :: else -> break
        od;
        op_selected = 1;
        do
        :: op_selected < OP_COOKIE_COUNT && op_used[op_selected] -> op_selected++
        :: else -> break
        od;
        assert(!op_used[op_selected]);
        op_current[n] = op_selected;
        op_pending[n] = op_selected;
        for (op_index : 0 .. OP_COOKIE_COUNT) { op_used[op_index] = 0 };
        op_index = 0; op_count = 0; op_name = 0; op_selected = 0
    };
    if
    :: probe[n] -> witness(14)
    :: else -> skip
    fi
}
#endif

inline scan_names(q, receiver, owner) {
    fresh_count = len(q);
    fresh_k = 0;
    do
    :: fresh_k < fresh_count ->
        q?fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value;
        if
        :: ((fresh_peer == owner && (fresh_kind == Initial || fresh_kind == Push || fresh_kind == Unbind || fresh_kind == QueryRoot))
            || (receiver == owner && (fresh_kind == ReversePush || fresh_kind == Reject || fresh_kind == RootReply || fresh_kind == Expired))) ->
            fresh_used[fresh_c] = 1
        :: else -> skip
        fi;
        q!fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value;
        fresh_k++
    :: else -> break
    od
}

inline fresh_cookie(n) {
    d_step {
        for (fresh_k : 0 .. COOKIE_COUNT) { fresh_used[fresh_k] = 0 };
        for (fresh_a : 1 .. 3) {
            if
            :: fresh_a != n -> fresh_used[child_cookie[SLOT(fresh_a, n)]] = 1
            :: else -> skip
            fi;
            scan_names(inbox[fresh_a], fresh_a, n)
        };
        fresh_selected = 1;
        do
        :: fresh_selected < COOKIE_COUNT && fresh_used[fresh_selected] -> fresh_selected++
        :: else -> break
        od;
        assert(!fresh_used[fresh_selected]);
        cookie[n] = fresh_selected;
        for (fresh_k : 0 .. COOKIE_COUNT) { fresh_used[fresh_k] = 0 };
        fresh_a = 0; fresh_k = 0; fresh_count = 0; fresh_selected = 0;
        fresh_peer = 0; fresh_sid = 0; fresh_c = 0; fresh_value = 0; fresh_kind = 0
    }
}

inline enqueue(a, b, kind, sid, c, value) {
    assert(len(inbox[b]) < QUEUE_CAPACITY);
    inbox[b]!kind,a,sid,c,value
}

inline send_message(a, b, kind, sid, c, value) {
    /* Once enqueued, a message has reached the Keeper's mailbox. */
    if
    :: SESSION_UP(a, b, sid) -> enqueue(a, b, kind, sid, c, value)
    :: else -> skip
    fi
}

inline fanout(n) {
    for (f : 1 .. 3) {
        if
        :: f != n && child_cookie[SLOT(n, f)] && last_root[SLOT(n, f)] != root[n] ->
            send_message(n, f, ReversePush, child_session[SLOT(n, f)], child_cookie[SLOT(n, f)], root[n]);
            last_root[SLOT(n, f)] = root[n]
        :: else -> skip
        fi
    };
    f = 0
}

inline refresh_local(n) {
    d_step {
        previous = subtree[n];
        subtree[n] = BIT(n);
        for (r : 1 .. 3) {
            if
            :: r != n -> subtree[n] = subtree[n] | child_nodes[SLOT(n, r)]
            :: else -> skip
            fi
        };
        if
        :: previous & ~subtree[n] -> retry_ready[n] = true /* TBindQueue::Enable makes removed nodes active. */
        :: else -> skip
        fi;
        push_dirty = binding[n] && previous != subtree[n];
        previous = 0; r = 0
    }
}

inline send_update(n) {
    if
    :: push_dirty -> send_message(n, binding[n], Push, binding_session[n], cookie[n], subtree[n])
    :: else -> skip
    fi;
    push_dirty = false
}

inline replace_child(n, peer, nodes) {
    child_nodes[SLOT(n, peer)] = nodes;
    refresh_local(n)
}

inline forget_child(n, peer) {
    child_cookie[SLOT(n, peer)] = 0;
    child_session[SLOT(n, peer)] = 0;
    last_root[SLOT(n, peer)] = 0;
    replace_child(n, peer, 0);
    send_update(n)
}

inline cancel_probe(n) {
    probe[n] = 0;
    awaiting[n] = false;
    timer_armed[n] = false;
    request_sent[n] = false
}

inline abort_binding(n, notify_peer, notify_children) {
    if
    :: binding[n] ->
        if
        :: notify_peer -> send_message(n, binding[n], Unbind, binding_session[n], cookie[n], 0)
        :: else -> skip
        fi;
        binding[n] = 0;
        binding_session[n] = 0;
        cookie[n] = 0;
        awaiting[n] = false;
        timer_armed[n] = false;
        request_sent[n] = false;
        root[n] = n;
        if
        :: notify_children -> fanout(n)
        :: else -> skip
        fi
    :: else -> skip
    fi
}

inline disconnect_peer(n, peer) {
    if
    :: child_cookie[SLOT(n, peer)] -> forget_child(n, peer)
    :: else -> skip
    fi;
    if
    :: binding[n] == peer -> abort_binding(n, false, true)
    :: else -> skip
    fi;
    if
    :: probe[n] == peer -> cancel_probe(n)
    :: else -> skip
    fi;
#if OP_MASK
    if
    :: handoff[n] == peer -> cancel_handoff(n)
    :: else -> skip
    fi;
#endif
    local_session[SLOT(n, peer)] = 0
}

inline use_session(n, peer, sid) {
    if
    :: local_session[SLOT(n, peer)] != sid ->
        disconnect_peer(n, peer);
        local_session[SLOT(n, peer)] = sid
    :: else -> skip
    fi
}

inline arm_request(n, peer, sid) {
    awaiting[n] = true;
    timer_armed[n] = true;
    request_sent[n] = SESSION_UP(n, peer, sid);
    retry_ready[n] = false
}

inline start_binding(n, peer, sid) {
    assert(!SCEPTER(n) && !binding[n] && !probe[n]);
#if OP_MASK
    assert(!op_current[n]);
#endif
    use_session(n, peer, sid);
    fresh_cookie(n);
    binding[n] = peer;
    binding_session[n] = sid;
    arm_request(n, peer, sid);
    printf("bind %d -> %d cookie=%d session=%d\n", n, peer, cookie[n], binding_session[n]);
    send_message(n, peer, Initial, binding_session[n], cookie[n], subtree[n]);
#if CHECK_EVENT == 18
    if
    :: binding_session[n] != session[EDGE(n, peer)] -> witness(18)
    :: else -> skip
    fi
#endif
}

inline start_probe(n, peer) {
    assert(SCEPTER(n) && !binding[n] && !probe[n]);
    use_session(n, peer, session[EDGE(n, peer)]);
    fresh_cookie(n);
    probe[n] = peer;
    arm_request(n, peer, local_session[SLOT(n, peer)]);
    printf("probe %d -> %d cookie=%d\n", n, peer, cookie[n]);
    send_message(n, peer, QueryRoot, session[EDGE(n, peer)], cookie[n], 0)
}

inline reconcile_role(n) {
    if
    :: !binding[n] && !error_wait[n] && QUORUM(n) ->
        if
        :: !SCEPTER(n) ->
            /* BecomeRoot starts config work, providing another actor activation. */
            retry_ready[n] = false;
            scepters = scepters | BIT(n)
        :: else -> skip
        fi
    :: !binding[n] && !error_wait[n] && !QUORUM(n) ->
        if
        :: SCEPTER(n) ->
            error_wait[n] = true;
            error_timer_armed[n] = true;
#if CHECK_EVENT == 19
            witness(19)
#endif
        :: else -> skip
        fi;
        scepters = scepters & ~BIT(n);
        fence_operations(n);
        cancel_handoff(n);
        if
        :: probe[n] -> cancel_probe(n)
        :: else -> skip
        fi
    :: else -> skip
    fi
}

/* Cyclic selection abstracts C++ bind queues. */
inline issue_next(n) {
    if
    :: (ONLINE(n) && retry_ready[n] && !binding[n] && !probe[n] && !error_wait[n]
#if OP_MASK
        && !handoff[n]
#ifdef QUEUE_BLOCKS_DISCOVERY
        && !op_current[n]
#endif
#endif
#ifdef YIELD_WORKING_ROOT
        && ((!SCEPTER(n) && !QUORUM(n)) || (SCEPTER(n) && subtree[n] != 7))
#elif !defined(OLD_POLICY)
        && ((!SCEPTER(n) && !QUORUM(n)) || (SCEPTER(n) && !WORKING_CONFIG(n) && subtree[n] != 7))
#else
        && !SCEPTER(n) && !QUORUM(n)
#endif
        ) ->
        candidate = 0; checked = 0;
#ifdef REPEAT_FIRST_CANDIDATE
        next_peer[n] = 0;
#endif
        do
        :: checked < 3 && !candidate ->
            next_peer[n] = next_peer[n] % 3 + 1;
            checked++;
            if
            :: !(subtree[n] & BIT(next_peer[n])) -> candidate = next_peer[n]
            :: else -> skip
            fi
        :: else -> break
        od;
        if
        :: candidate ->
            if
            :: SCEPTER(n) -> start_probe(n, candidate)
            :: else -> start_binding(n, candidate, session[EDGE(n, candidate)])
            fi
        :: else -> retry_ready[n] = false
        fi;
        candidate = 0; checked = 0;
    :: else -> skip
    fi
}

/* StateFunc checks for the next request before changing the root role. */
inline reconcile(n) {
    if
    :: probe[n] && (subtree[n] & BIT(probe[n])) -> cancel_probe(n)
    :: else -> skip
    fi;
    issue_next(n);
    reconcile_role(n)
}

inline audit() {
#if CHECK_EVENT == 5
    if
    :: had_timeout && CONVERGED -> witness(5)
    :: else -> skip
    fi;
#endif
    for (audit_n : 1 .. 3) {
        if
        :: ONLINE(audit_n) ->
            assert(!SCEPTER(audit_n) || !binding[audit_n]);
            assert(!error_wait[audit_n] || (!SCEPTER(audit_n) && !binding[audit_n]));
#if NO_SERVICE_SET_MASK
            if
            :: NO_SERVICE_SET(audit_n) ->
                assert(!SCEPTER(audit_n) || MAJORITY(subtree[audit_n]));
                assert(binding[audit_n] || error_wait[audit_n] || !MAJORITY(subtree[audit_n]) || SCEPTER(audit_n))
            :: else -> skip
            fi;
#endif
#if OP_MASK
            assert(!op_current[audit_n] || (SCEPTER(audit_n) && !binding[audit_n]));
            assert(!handoff_ready[audit_n] || (handoff[audit_n] && !op_current[audit_n]));
#endif
            if
            :: (protected_roots & BIT(audit_n)) && QUORUM(audit_n) -> assert(SCEPTER(audit_n))
            :: else -> protected_roots = protected_roots & ~BIT(audit_n)
            fi;
            if
            :: WORKING_ROOT(audit_n) -> protected_roots = protected_roots | BIT(audit_n)
            :: else -> skip
            fi
        :: else -> skip
        fi
    };
#if !COMMITTED_MASK
    /* Cache at handler boundaries to keep the LTL claim small. */
    tree_converged = CONVERGED;
#endif
    audit_n = 0
}

/* Initial activation and asynchronous operation handoff. Retries run in Delivery. */
proctype Keeper(byte n) {
    d_step { ONLINE(n) -> reconcile(n); audit() };
#if OP_MASK
    do
    :: d_step {
        (ONLINE(n) && handoff[n]
#ifdef WAIT_OPERATION_BEFORE_HANDOFF
        && !op_current[n]
#endif
        ) ->
        if
        :: (!SCEPTER(n) || handoff_session[n] != local_session[SLOT(n, handoff[n])]
           || (subtree[n] & BIT(handoff[n]))) -> cancel_handoff(n)
        :: else ->
            if
            :: !handoff_ready[n] ->
                if
                :: op_current[n] -> witness(10)
                :: else -> skip
                fi;
#ifndef SKIP_OPERATION_FENCE
                fence_operations(n);
#endif
                handoff_ready[n] = true
            :: handoff_ready[n] ->
#if OP_WORKLOAD == 3
                if
                :: op_pending[n] -> witness(15)
                :: else -> skip
                fi;
#endif
                target = handoff[n];
                cancel_handoff(n);
                scepters = scepters & ~BIT(n);
                start_binding(n, target, local_session[SLOT(n, target)]);
                target = 0
            fi
        fi;
        reconcile(n); audit()
    }
    od
#endif
}

#if OP_MASK
/* Separate completion mailbox permits reordering with network events.
 * Workload 2 leaves no idle gap; workload 3 never completes. */
proctype Operations(byte n) {
    byte completed;
    byte previous_operation;
    byte previous_scepters; byte previous_binding; byte previous_root; byte previous_probe;
    byte previous_handoff; bool previous_ready;
    do
    :: d_step {
        (ONLINE(n) && (OP_MASK & BIT(n)) && SCEPTER(n) && !binding[n]
#ifndef ADMIT_WORK_DURING_HANDOFF
        && !handoff[n]
#endif
        && !op_current[n] && !op_pending[n]) ->
        start_operation(n); audit()
    }
#if OP_WORKLOAD != 3
    :: d_step {
        op_pending[n] && len(op_done[n]) < 2 ->
        op_done[n]!op_pending[n];
        op_pending[n] = 0
    }
#endif
    :: d_step {
        len(op_done[n]) ->
        op_done[n]?completed;
        previous_operation = op_current[n];
        previous_scepters = scepters; previous_binding = binding[n];
        previous_root = root[n]; previous_probe = probe[n];
        previous_handoff = handoff[n]; previous_ready = handoff_ready[n];
        if
#ifdef IGNORE_OPERATION_TOKEN
        :: true -> op_current[n] = 0
#else
        :: completed == op_current[n] -> op_current[n] = 0
        :: else -> skip
#endif
        fi;
        if
        :: completed != previous_operation ->
            assert(op_current[n] == previous_operation && scepters == previous_scepters
                   && binding[n] == previous_binding && root[n] == previous_root && probe[n] == previous_probe
                   && handoff[n] == previous_handoff && handoff_ready[n] == previous_ready);
            if
            :: binding[n] -> witness(11)
            :: else -> skip
            fi;
            if
            :: previous_operation -> witness(12)
            :: else -> skip
            fi
        :: else -> skip
        fi;
#if OP_WORKLOAD == 2
        if
        :: completed == previous_operation && SCEPTER(n) && !binding[n] && !handoff[n] && !op_pending[n] ->
            start_operation(n)
        :: else -> skip
        fi;
#endif
        reconcile(n); audit();
        completed = 0; previous_operation = 0;
        previous_scepters = 0; previous_binding = 0; previous_root = 0; previous_probe = 0;
        previous_handoff = 0; previous_ready = false
    }
    od
}
#endif

proctype Delivery(byte n) {
    mtype kind;
    byte peer; byte sid; byte c; byte value;
    bool obsolete;
    byte old_scepters; byte old_binding; byte old_root; byte old_probe;
    do
    :: d_step {
        ONLINE(n) && len(inbox[n]) ->
        inbox[n]?kind,peer,sid,c,value;
        obsolete = (kind == RootReply || kind == ReversePush || kind == Reject) && c != cookie[n];
        old_scepters = scepters; old_binding = binding[n]; old_root = root[n]; old_probe = probe[n];
        if
        :: kind == Wakeup ->
            wakeup_pending[n] = false;
            retry_ready[n] = true
        :: kind == ErrorTimeout ->
            assert(error_wait[n]);
            error_wait[n] = false;
            /* The 1.5s minimum error backoff exceeds the 1s bind retry delay. */
            retry_ready[n] = true
        :: kind == Disconnected ->
            if
            :: local_session[SLOT(n, peer)] == sid -> disconnect_peer(n, peer)
            :: else -> skip
            fi
        :: kind == Expired ->
            if
            :: (awaiting[n] && cookie[n] == c
                && ((probe[n] == peer && local_session[SLOT(n, peer)] == sid)
                    || (binding[n] == peer && binding_session[n] == sid))) ->
                printf("timeout node=%d peer=%d cookie=%d\n", n, peer, c);
                witness(1);
#if CHECK_EVENT == 5
                had_timeout = true;
#endif
                if
                :: probe[n] -> cancel_probe(n)
                :: else -> witness(6); abort_binding(n, true, true)
                fi
            :: else ->
                witness(3);
                if
                :: binding[n] == peer && cookie[n] == c && !awaiting[n] -> witness(9)
                :: else -> skip
                fi
            fi
        :: kind != Disconnected && kind != Expired && kind != Wakeup && kind != ErrorTimeout ->
            if
            :: ((local_session[SLOT(n, peer)] && sid != local_session[SLOT(n, peer)])
                || (!local_session[SLOT(n, peer)] && kind != Initial && kind != QueryRoot)) -> skip
            :: else ->
                use_session(n, peer, sid);
                if
                :: kind == QueryRoot ->
                    if
#ifdef YIELD_WORKING_ROOT
                    :: SCEPTER(n) -> send_message(n, peer, RootReply, sid, c, n)
#else
                    :: WORKING_ROOT(n) -> send_message(n, peer, RootReply, sid, c, n)
#endif
                    :: else -> send_message(n, peer, RootReply, sid, c, 0)
                    fi;
#if OP_MASK
                    if
                    :: WORKING_ROOT(n) && op_current[n] -> witness(13)
                    :: else -> skip
                    fi
#endif
                :: kind == RootReply ->
                    if
#ifdef IGNORE_PROBE_COOKIE
                    :: probe[n] == peer ->
#else
                    :: probe[n] == peer && cookie[n] == c ->
#endif
                        cancel_probe(n);
                        if
#ifdef YIELD_WORKING_ROOT
                        :: value == peer && !(subtree[n] & BIT(peer)) && SCEPTER(n) ->
#else
                        :: value == peer && !(subtree[n] & BIT(peer)) && SCEPTER(n) && !WORKING_CONFIG(n) ->
#endif
                            printf("root %d yields to %d\n", n, peer);
#if OP_MASK
                            assert(!handoff[n]);
                            handoff[n] = peer;
                            handoff_session[n] = sid;
#else
                            scepters = scepters & ~BIT(n);
                            start_binding(n, peer, sid);
#endif
#if CHECK_EVENT == 17
                            if
                            :: sid != session[EDGE(n, peer)] -> witness(17)
                            :: else -> skip
                            fi;
#endif
                        :: else -> skip
                        fi
                    :: else -> skip
                    fi
                :: kind == Initial && root[n] == peer ->
                    send_message(n, peer, Reject, sid, c, 0)
                :: (kind == Initial && root[n] != peer) || (kind == Push && child_cookie[SLOT(n, peer)]) ->
                    if
                    :: binding[n] == peer && peer < n -> abort_binding(n, true, true)
                    :: else -> skip
                    fi;
                    if
                    :: !child_cookie[SLOT(n, peer)] ->
                        child_cookie[SLOT(n, peer)] = c;
                        child_session[SLOT(n, peer)] = sid;
                        last_root[SLOT(n, peer)] = root[n];
                        send_message(n, peer, ReversePush, sid, c, root[n])
                    :: else -> assert(child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid)
                    fi;
                    replace_child(n, peer, value);
                    send_update(n);
#if CHECK_EVENT == 21
                    if
                    :: error_wait[n] && kind == Initial -> witness(21)
                    :: else -> skip
                    fi
#endif
                :: kind == Push && !child_cookie[SLOT(n, peer)] -> skip
                :: kind == ReversePush || kind == Reject ->
                    if
                    :: binding[n] == peer && cookie[n] == c && binding_session[n] == sid ->
                        if
                        :: kind == Reject -> abort_binding(n, false, true)
                        :: kind == ReversePush ->
                            awaiting[n] = false; timer_armed[n] = false; request_sent[n] = false;
                            if
                            :: value == n -> abort_binding(n, true, false)
                            :: else -> root[n] = value
                            fi;
                            fanout(n)
                        fi
                    :: else -> skip
                    fi
                :: kind == Unbind ->
                    if
                    :: child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid -> forget_child(n, peer)
                    :: else -> skip
                    fi
                fi
            fi
        fi;
        if
        :: obsolete && sid == local_session[SLOT(n, peer)] ->
            assert(scepters == old_scepters && binding[n] == old_binding && root[n] == old_root && probe[n] == old_probe);
            witness(2);
            if
            :: kind == ReversePush -> witness(7)
            :: else -> skip
            fi
        :: else -> skip
        fi;
        reconcile(n); audit();
#if CHECK_EVENT == 20
        if
        :: kind == ErrorTimeout && SCEPTER(n) -> witness(20)
        :: else -> skip
        fi;
#endif
        kind = 0; peer = 0; sid = 0; c = 0; value = 0;
        obsolete = false; old_scepters = 0; old_binding = 0; old_root = 0; old_probe = 0
    }
    od
}

/* Healthy requests time out only before stabilization; queued Expired events may arrive late. */
proctype Timer(byte n) {
    do
    :: d_step {
        ONLINE(n) && error_timer_armed[n] ->
        enqueue(n, n, ErrorTimeout, 0, 0, 0);
        error_timer_armed[n] = false
    }
    :: d_step {
        ONLINE(n) && !retry_ready[n] && !awaiting[n] && !wakeup_pending[n] && !error_wait[n] ->
        enqueue(n, n, Wakeup, 0, 0, 0);
        wakeup_pending[n] = true
    }
    :: d_step {
        (ONLINE(n) && awaiting[n] && timer_armed[n]
         && (!request_sent[n] || !SESSION_UP(n, REQUEST_PEER(n), local_session[SLOT(n, REQUEST_PEER(n))])
             || (!network_stable && timeouts_left))) ->
        target = probe[n];
        if
        :: !target -> target = binding[n]
        :: else -> skip
        fi;
        if
        :: !request_sent[n] || !SESSION_UP(n, target, local_session[SLOT(n, target)]) ->
            enqueue(target, n, Expired, local_session[SLOT(n, target)], cookie[n], 0);
            timer_armed[n] = false
        :: request_sent[n] && SESSION_UP(n, target, local_session[SLOT(n, target)]) && !network_stable && timeouts_left ->
            timeouts_left--;
            enqueue(target, n, Expired, local_session[SLOT(n, target)], cookie[n], 0);
            timer_armed[n] = false
        :: else -> skip
        fi;
        target = 0
    }
    od
}

inline cut_link(a, b) {
    printf("disconnect %d-%d\n", a, b);
    witness(4);
    if
    :: (binding[a] == b && awaiting[a]) || (binding[b] == a && awaiting[b]) -> witness(8)
    :: else -> skip
    fi;
#if OP_MASK
    if
    :: (handoff[a] == b && handoff_ready[a]) || (handoff[b] == a && handoff_ready[b]) -> witness(16)
    :: else -> skip
    fi;
#endif
    enqueue(b, a, Disconnected, session[EDGE(a, b)], 0, 0);
    enqueue(a, b, Disconnected, session[EDGE(a, b)], 0, 0);
    links = links & ~LINK(a, b);
    session[EDGE(a, b)]++;
    faults_left--
}

proctype Network() {
    do
    :: atomic {
        !network_stable && faults_left ->
        if
        :: UP(1, 2) -> cut_link(1, 2)
        :: UP(1, 3) -> cut_link(1, 3)
        :: UP(2, 3) -> cut_link(2, 3)
        :: else -> faults_left = 0
        fi
    }
    :: atomic {
        !network_stable ->
        links = 7;
        network_stable = true;
        faults_left = 0; timeouts_left = 0
    }
    :: network_stable -> break
    od
}

inline initialize_binding(parent, child) {
    binding[child] = parent;
    binding_session[child] = 1;
    cookie[child] = 1;
    root[child] = parent;
    child_cookie[SLOT(parent, child)] = 1;
    child_session[SLOT(parent, child)] = 1;
    child_nodes[SLOT(parent, child)] = BIT(child);
    last_root[SLOT(parent, child)] = parent;
    subtree[parent] = subtree[parent] | BIT(child);
    local_session[SLOT(parent, child)] = 1;
    local_session[SLOT(child, parent)] = 1
}

init {
    byte n; byte peer;
    byte bootstrap_root;
    atomic {
        session[0] = 1; session[1] = 1; session[2] = 1;
        for (n : 0 .. 2) {
            if
            :: !(INITIAL_LINKS & (1 << n)) -> session[n] = 2
            :: else -> skip
            fi
        };
        for (n : 1 .. 3) {
            root[n] = n;
            subtree[n] = BIT(n);
            retry_ready[n] = true
        };
#if INITIAL_TREE == 1
        bootstrap_root = BOOTSTRAP_ROOT;
        peer = 6 - STORAGE_NODE - bootstrap_root;
        if
        :: ONLINE(bootstrap_root) && ONLINE(peer) ->
            initialize_binding(bootstrap_root, peer)
        :: else -> skip
        fi;
#elif INITIAL_TREE == 2
        for (peer : 1 .. 3) {
            if
            :: peer != STORAGE_NODE && ONLINE(peer) ->
                initialize_binding(STORAGE_NODE, peer)
            :: else -> skip
            fi
        };
#endif
        for (n : 1 .. 3) {
            if
            :: ONLINE(n) -> reconcile_role(n)
            :: else -> skip
            fi
        };
        audit();
#if OP_MASK
        for (n : 1 .. 3) {
            if
            :: ONLINE(n) && (OP_MASK & BIT(n)) && SCEPTER(n) -> start_operation(n)
            :: else -> skip
            fi
        };
#endif
        for (n : 1 .. 3) {
            for (peer : 1 .. 3) {
                if
                :: n != peer && ONLINE(n) && local_session[SLOT(n, peer)] && !UP(n, peer) ->
                    enqueue(peer, n, Disconnected, 1, 0, 0)
                :: else -> skip
                fi
            }
        };
        n = 0; peer = 0; bootstrap_root = 0;
        run Keeper(1); run Keeper(2); run Keeper(3);
        run Delivery(1); run Delivery(2); run Delivery(3);
        run Timer(1); run Timer(2); run Timer(3);
#if OP_MASK
        run Operations(1); run Operations(2); run Operations(3);
#endif
        run Network()
    }
    do
    :: timeout -> assert(network_stable && CONVERGED)
    od
}

#ifndef NO_LTL
#if COMMITTED_MASK
ltl convergence { <> [] (network_stable && CONVERGED) }
#else
ltl convergence { <> [] (network_stable && tree_converged) }
#endif
ltl role_exclusion { [] ((!SCEPTER(1) || !binding[1]) && (!SCEPTER(2) || !binding[2]) && (!SCEPTER(3) || !binding[3])) }
#endif
