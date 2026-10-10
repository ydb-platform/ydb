#ifndef CONFIG_VERSIONS
#define CONFIG_VERSIONS 0
#endif
#ifndef CFG_WRITE_MODE
#define CFG_WRITE_MODE 0
#endif
#ifndef CFG_API_TRANSPORT
#define CFG_API_TRANSPORT (CFG_WRITE_MODE >= 2)
#endif
#if CFG_API_TRANSPORT && (!CONFIG_VERSIONS || CFG_WRITE_MODE == 0)
#error CFG_API_TRANSPORT requires CONFIG_VERSIONS and a write workload
#endif
#ifndef CFG_TARGET_SAMEGEN
#define CFG_TARGET_SAMEGEN 0
#endif
#ifndef CFG_GOAL_CLOSURE
#define CFG_GOAL_CLOSURE 0
#endif
#ifndef CFG_GOAL_REACHABILITY
#define CFG_GOAL_REACHABILITY 0
#endif
#if CFG_TARGET_SAMEGEN && (!CONFIG_VERSIONS || CFG_WRITE_MODE < 2 || !CFG_API_TRANSPORT)
#error CFG_TARGET_SAMEGEN requires CONFIG_VERSIONS, CFG_WRITE_MODE >= 2 and CFG_API_TRANSPORT
#endif
#ifndef STORAGE_NODE
#define STORAGE_NODE 1
#endif
#ifndef COMMITTED_MASK
#if CONFIG_VERSIONS
#define COMMITTED_MASK 7
#else
#define COMMITTED_MASK (1 << (STORAGE_NODE - 1))
#endif
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
#if CONFIG_VERSIONS
/* Nonzero bodies describe static groups; an empty node has the bootstrap layout. */
#define WORKING_CONFIG(n) (cfg_applied[n] != 0)
#else
#define WORKING_CONFIG(n) (COMMITTED_MASK & BIT(n))
#endif
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
#if CONFIG_VERSIONS
/* Each version describes the same three-node, two-vote physical quorum. */
#define QUORUM(n) MAJORITY(subtree[n])
#else
#define QUORUM(n) ((WORKING_CONFIG(n) && GROUP_QUORUM(n)) || (!WORKING_CONFIG(n) && BOOTSTRAP_QUORUM(n)))
#endif
#define SCEPTER(n) (scepters & BIT(n))
#define REQUEST_PEER(n) (probe[n] -> probe[n] : binding[n])
#define WORKING_ROOT(n) (SCEPTER(n) && WORKING_CONFIG(n))
#define ACCEPTED(n) (binding[n] && child_cookie[SLOT(binding[n], n)] == cookie[n] && child_session[SLOT(binding[n], n)] == binding_session[n])
#define ATTACHED(n, r) ((n) == (r) || (ACCEPTED(n) && (binding[n] == (r) || (ACCEPTED(binding[n]) && binding[binding[n]] == (r)))))
#define JOINED(n, r) (!ONLINE(n) || (root[n] == r && ATTACHED(n, r)))
#if CONFIG_VERSIONS
#define ASSEMBLY_ROOT root[1]
#define CONVERGED (ONLINE(ASSEMBLY_ROOT) && scepters == BIT(ASSEMBLY_ROOT) && JOINED(1, ASSEMBLY_ROOT) && JOINED(2, ASSEMBLY_ROOT) && JOINED(3, ASSEMBLY_ROOT))
#elif COMMITTED_MASK
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
#if INITIAL_TREE < 0 || INITIAL_TREE > 3 || (!CONFIG_VERSIONS && INITIAL_TREE > 2) || INITIAL_LINKS < 0 || INITIAL_LINKS > 7
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
#if CONFIG_VERSIONS
mtype = { CfgCollect, CfgGather, CfgPublish, CfgMetadata, CfgPropose, CfgVote, CfgFreshQuery, CfgFreshReply, CfgFreshTimeout };
#define CFG_MESSAGE(kind) ((kind) == CfgCollect || (kind) == CfgGather || (kind) == CfgPublish || (kind) == CfgMetadata || (kind) == CfgPropose || (kind) == CfgVote || (kind) == CfgFreshQuery || (kind) == CfgFreshReply || (kind) == CfgFreshTimeout)
chan inbox[4] = [QUEUE_CAPACITY] of { mtype, byte, byte, byte, byte, int };
#else
chan inbox[4] = [QUEUE_CAPACITY] of { mtype, byte, byte, byte, byte };
#endif

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

#if CONFIG_VERSIONS
/* Configuration versions: decl. */
/* Recovery and causal blank refill follow binding-tree edges. Finite
 * administrative writes use independent async reads and serialized writes.
 * This bounded slice does not claim all actor histories. Recipient mailboxes
 * retain the original FIFO assumption.
 * Each requester has one relay route and an explicit finite timeout budget. */
#define CFG_IDX(n, owner) (4 * (n) + (owner))
#define CFG_OWNER(key) ((key) & 3)
#define CFG_TOKEN(key) ((key) >> 2)
#define CFG_BIT(n) (1 << ((n) - 1))
#define CFG_C(data, n) (((data) >> (3 + 3 * ((n) - 1))) & 7)
#define CFG_P(data, n) (((data) >> (12 + 3 * ((n) - 1))) & 7)
#define CFG_CONFLICT (1 << 21)
#define CFG_ONE(n, c, p) (CFG_BIT(n) | ((c) << (3 + 3 * ((n) - 1))) | ((p) << (12 + 3 * ((n) - 1))))
#ifndef CFG_ASYNC_READ
#define CFG_ASYNC_READ 1
#endif
#ifndef CFG_LOCAL_TASK_COOKIE
#define CFG_LOCAL_TASK_COOKIE 1
#endif
#define CFG_USE_LOCAL_COOKIE (CFG_LOCAL_TASK_COOKIE && (CFG_ASYNC_READ || CFG_WRITE_MODE > 0))
#define CFG_LOCAL_TAG(data) (((data) >> 23) & 63)
#define CFG_LOCAL_TAG_MASK (63 << 23)
#if CFG_USE_LOCAL_COOKIE
byte cfg_task_local_key[16]; byte cfg_prop_busy_local_key[4]; byte cfg_task_wire_key;
#define CFG_CALLBACK_TAG(key) (CFG_TOKEN(key) << 23)
#define CFG_LOCAL_CALLBACK_VALID(n,key,data) (CFG_LOCAL_TAG(data) == CFG_TOKEN(cfg_task_local_key[CFG_IDX(n, CFG_OWNER(key))]))
#else
#define CFG_CALLBACK_TAG(key) 0
#define CFG_LOCAL_CALLBACK_VALID(n,key,data) 1
#endif
#if CFG_ASYNC_READ
#define CFG_LOCAL_LOADED (1 << 22)
#ifndef CFG_READ_CAPACITY
#define CFG_READ_CAPACITY 3
#endif
/* Read actors survive scatter cancellation. Physical snapshots and keeper
 * callbacks are distinct; writes remain serialized by the existing disk FIFO. */
#if CFG_USE_LOCAL_COOKIE
chan cfg_reads[16] = [CFG_READ_CAPACITY] of { byte, byte };
byte cfg_read_scan_local;
#else
chan cfg_reads[16] = [CFG_READ_CAPACITY] of { byte };
#endif
bool cfg_read_pending[16];
byte cfg_read_scan_slot; byte cfg_read_scan_count; byte cfg_read_scan_i; byte cfg_read_scan_key;
#endif
#define CFG_META(data, n) (((data) >> (3 + 3 * ((n) - 1))) & 7)
#ifndef CFG_HIGH_MASK
#define CFG_HIGH_MASK 0
#endif
#ifndef CFG_EMPTY_MASK
#define CFG_EMPTY_MASK 0
#endif
#ifndef CFG_IO_CAPACITY
#define CFG_IO_CAPACITY 3
#endif
#if CFG_TARGET_SAMEGEN && (INITIAL_TREE != 3 || ONLINE_MASK != 7 || INITIAL_LINKS != 7 || FAULT_BUDGET != 1 || TIMEOUT_BUDGET || OP_MASK || CFG_HIGH_MASK || CFG_EMPTY_MASK || QUEUE_CAPACITY < 8)
#error The targeted race requires the all-BASE online chain, all links up, one cut, no tree timeouts/operations, and queue capacity at least eight
#endif
#define cfg_disk_agreement (cfg_c[1] != 0 && cfg_c[1] == cfg_c[2] && cfg_c[2] == cfg_c[3] && (!cfg_expected || cfg_c[1] == cfg_expected))
#if CFG_WRITE_MODE == 0
#define cfg_converged cfg_disk_agreement
#elif CFG_WRITE_MODE == 1
#define cfg_converged (cfg_disk_agreement && cfg_client_sent[2] && cfg_client_done[2])
#elif CFG_WRITE_MODE == 2
#define cfg_converged (cfg_disk_agreement && cfg_client_sent[2] && cfg_client_done[2] && cfg_client_sent[3] && cfg_client_done[3])
#else
#define cfg_converged (cfg_disk_agreement && cfg_client_sent[2] && cfg_client_done[2] && cfg_client_sent[3] && cfg_client_done[3] && cfg_client_sent[4] && cfg_client_done[4])
#endif
/* Causal blank refill overlay. No ordinary publication carries a root-operation
 * epoch; its authority is still only the current parent binding. */
#ifndef CFG_OLD_ROOT_MASK
#define CFG_OLD_ROOT_MASK 0
#endif
#define CFG_NEW_ROOT(n) (!(CFG_OLD_ROOT_MASK & CFG_BIT(n)))
#define CFG_RPC_FRESH 8
#define CFG_RPC_ROOT(data) (((data) >> 4) & 3)
#define CFG_RPC_ERROR 64
/* Model actor destination kind: C++ addresses the helper and forwarder separately. */
#define CFG_RPC_RELAY 128
#define CFG_RPC_BODY(data) ((data) & 7)
#ifndef CFG_RPC_TIMEOUT_BUDGET
#define CFG_RPC_TIMEOUT_BUDGET 0
#endif
/* CfgFreshTimeout is a local helper deadline, not a tree packet. Add it to
 * CFG_MESSAGE and the explicit allocator queue-kind alternatives. */
#define CFG_QVALID(n, origin) (cfg_rpc_key[CFG_IDX(n, origin)] && cfg_rpc_peer[CFG_IDX(n, origin)] && cfg_rpc_peer[CFG_IDX(n, origin)] != n && SESSION_UP(n, cfg_rpc_peer[CFG_IDX(n, origin)], cfg_rpc_session[CFG_IDX(n, origin)]))
#define cfg_refill_enabled(n) (cfg_held[n] && !cfg_applied[n] && !cfg_legacy[n] && binding[n] && !awaiting[n] && root[n] != n && !cfg_refill_sent[n])
#define cfg_hold_ordinary(n, body) ((body) && cfg_generation[body] > 0 && !cfg_applied[n] && !cfg_legacy[n] && binding[n])
byte cfg_held[4]; bool cfg_legacy[4];
byte cfg_refill_actor[4]; bool cfg_refill_sent[4];
byte cfg_refill_parent[4]; byte cfg_refill_session[4]; byte cfg_refill_cookie[4]; byte cfg_refill_root[4];
byte cfg_rpc_key[16]; byte cfg_rpc_peer[16]; byte cfg_rpc_session[16]; byte cfg_rpc_cookie[16];
byte cfg_rpc_parent[16]; byte cfg_rpc_parent_session[16];
bool cfg_rpc_wait[16]; bool cfg_rpc_target_relay[16]; bool cfg_rpc_errors[4];
byte cfg_refill_timeout_key[4]; byte cfg_refill_timeouts_left[4];
/* cfg_init: cfg_refill_timeouts_left[n] = CFG_RPC_TIMEOUT_BUDGET.
 * Run CfgRefillTimer(n) for each keeper. */
/* Extend cfg_run_enabled(n) by cfg_refill_enabled(n) || cfg_rpc_errors[n]. */
#define cfg_run_enabled(n) (cfg_meta_dirty[n] || cfg_tasks_dirty[n] || (SCEPTER(n) && !binding[n] && cfg_need[n] && !cfg_actor[n]) || cfg_publish_dirty[n] || cfg_refill_enabled(n) || cfg_rpc_errors[n] || cfg_write_enabled(n))
#if CFG_WRITE_MODE > 0
/* Finite Replace workload. A config id names one immutable body; its generation is assigned once
 * by that body's fresh preflight, before the body enters any message or IO. */
#define CFG_TASK_READ 1
#define CFG_TASK_PROPOSE 2
#define CFG_PHASE_RECOVERY 1
#define CFG_PHASE_PREFLIGHT 2
#define CFG_PHASE_PROPOSE 3
#define CFG_PHASE_CONFIRM 4
#define CFG_CLIENT_REQUEST 256
#define CFG_CLIENT_ENDPOINT 1
#define CFG_CLIENT_FORWARDED 512
#define CFG_LOCAL_STORED 1024
#if CFG_API_TRANSPORT
/* Small InvokeQ and routed API results. Layer after finite write/vote mode1.
 * Client timeout ends the caller's wait, never erases an already routed server
 * command. Immutable YAML input version rejects obsolete commands at execution.
 * StorageConfig.Generation is still allocated by fresh max(c,p)+1 preflight. */
#define CFG_API_ENDPOINT 32768
#define CFG_API_REPLY 2048
#define CFG_API_OK 4096
#define CFG_API_DEADLINE 8192
#define CFG_API_FETCH 16384
#define CFG_API_FETCH_BODY(data) (((data) >> 3) & 7)
#define CFG_API_BODY(data) ((data) & 7)
#define CFG_API_REPLACE 1
#define CFG_API_GET_VERSION 2
#define CFG_PHASE_FETCH 5
#define CFG_API_IDX(n,body) (5 * (n) + (body))
#ifndef CFG_API_TIMEOUT_MASK
#define CFG_API_TIMEOUT_MASK 0
#endif
#define cfg_api_root_enabled(n) (cfg_api_abort_active[n] || cfg_api_abort_queue[n] || (SCEPTER(n) && !binding[n] && !cfg_actor[n] && !cfg_api_active_body[n] && len(cfg_invoke_q[n])))
chan cfg_invoke_q[4] = [2] of { byte, byte, byte };
byte cfg_api_actor[5]; byte cfg_api_endpoint[5]; byte cfg_api_phase[5];
byte cfg_api_deadline_key[5]; bool cfg_api_expired[5];
byte cfg_input_version[5]; byte cfg_logical_version[5];
byte cfg_api_route_key[20]; byte cfg_api_route_peer[20];
byte cfg_api_route_session[20]; bool cfg_api_route_endpoint[20];
byte cfg_api_route_parent[20]; byte cfg_api_route_parent_session[20];
byte cfg_api_active_body[4]; byte cfg_api_active_key[4]; byte cfg_api_active_kind[4];
bool cfg_api_abort_active[4]; bool cfg_api_abort_queue[4];
byte cfg_api_i; byte cfg_api_body; byte cfg_api_kind; byte cfg_api_key; int cfg_api_data; int cfg_api_target_data;
byte cfg_api_scan_body; byte cfg_api_scan_kind; byte cfg_api_scan_key;
#endif
#if CFG_API_TRANSPORT
#define cfg_write_enabled(n) (cfg_result_ready[n] || cfg_api_root_enabled(n))
#else
#define cfg_write_enabled(n) (cfg_result_ready[n] || (cfg_request_pending[n] && !cfg_actor[n] && SCEPTER(n) && !binding[n]))
#endif
byte cfg_task_kind[16]; byte cfg_task_body[16]; bool cfg_local_pending[16];
byte cfg_prop_busy_key[4]; byte cfg_prop_busy_body[4];
byte cfg_request_pending[4]; byte cfg_request[4]; byte cfg_request_base[4];
int cfg_result[4]; bool cfg_result_ready[4];
byte cfg_ok_by_generation[8]; bool cfg_client_done[5]; bool cfg_client_ok[5];
bool cfg_client_sent[5]; byte cfg_last_admin;
byte cfg_write_i; byte cfg_write_body; byte cfg_write_gen; byte cfg_write_count; byte cfg_write_key;
#else
#define cfg_write_enabled(n) false
#endif
byte cfg_generation[5];
byte cfg_c[4]; byte cfg_p[4];
byte cfg_memory_c[4]; byte cfg_memory_p[4];
byte cfg_applied[4]; byte cfg_published[4];
byte cfg_floor[4]; bool cfg_conflict[4];
byte cfg_meta[16]; byte cfg_order[16]; byte cfg_changed[4]; int cfg_wire[4];
byte cfg_order_in; byte cfg_order_out; byte cfg_order_count; byte cfg_order_i; byte cfg_order_peer;
byte cfg_tail_peer; byte cfg_tail_body;
byte cfg_meta_origin; byte cfg_meta_previous; byte cfg_meta_body;
int cfg_meta_old; int cfg_meta_new;
int cfg_child_report[9];
byte cfg_pub_sent[9];
byte cfg_actor[4]; byte cfg_phase[4];
bool cfg_need[4]; bool cfg_meta_dirty[4]; bool cfg_tasks_dirty[4]; bool cfg_publish_dirty[4];
byte cfg_task[16]; byte cfg_parent[16]; byte cfg_parent_cookie[16]; byte cfg_parent_session[16];
byte cfg_pending[16]; int cfg_records[16];
byte cfg_task_child_cookie[64]; byte cfg_task_child_session[64];
#if CFG_WRITE_MODE > 0
#if CFG_USE_LOCAL_COOKIE
chan cfg_writes[4] = [CFG_IO_CAPACITY] of { byte, byte, byte, byte, byte };
#else
chan cfg_writes[4] = [CFG_IO_CAPACITY] of { byte, byte, byte, byte };
#endif
#else
chan cfg_writes[4] = [CFG_IO_CAPACITY] of { byte, byte };
#endif
byte cfg_expected;
#if CFG_TARGET_SAMEGEN
/* A finite environment delay before the one-cut race; never used for routing. */
byte cfg_target_stage;
#define CFG_TARGET_DISK(n) ((n) == 1 || cfg_target_stage >= 3)
#define CFG_TARGET_DELIVERY(n) ((n) != 1 || cfg_target_stage == 0 || cfg_target_stage >= 4)
#define CFG_TARGET_KEEPER(n) (!((n) == 2 && cfg_target_stage == 3 && (cfg_prop_busy_key[2] || cfg_prop_busy_key[3])))
#else
#define CFG_TARGET_DISK(n) true
#define CFG_TARGET_DELIVERY(n) true
#define CFG_TARGET_KEEPER(n) true
#endif
/* Scratch is reset at the end of config handlers. */
byte cfg_a; byte cfg_b; byte cfg_o; byte cfg_x; byte cfg_t; byte cfg_key;
byte cfg_candidate; byte cfg_stateful; byte cfg_v; byte cfg_w;
byte cfg_copies[5]; byte cfg_committed[5];
int cfg_report;
bool cfg_bad;
byte cfg_used[64]; byte cfg_scan_n; byte cfg_scan_i; byte cfg_scan_count;
mtype cfg_scan_kind; byte cfg_scan_peer; byte cfg_scan_sid; byte cfg_scan_cookie; byte cfg_scan_value; int cfg_scan_data;
#endif

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
#if CONFIG_VERSIONS
int fresh_data;
#endif
mtype fresh_kind;

/* CHECK_EVENT deliberately fails at the selected witness:
 * 1: request timeout; 2: stale reply; 3: stale timer; 4: disconnect; 5: convergence after timeout;
 * 6: bind timeout; 7: stale bind reply; 8: disconnect during binding; 9: timer after confirmation;
 * 10: fencing a busy root; 11: stale completion while bound; 12: stale completion during a new operation;
 * 13: busy root replies; 14: operation during probe; 15: bind despite unfinished operation;
 * 16: disconnect after fencing; 17: accept queued positive reply after session loss;
 * 18: bind before observing disconnect; 19: quorum loss enters backoff;
 * 20: regain leadership after backoff; 21: binding message during backoff;
 * 22: overlapping roots; 23: X received OK; 24: later Z received OK;
 * 25: X/Y generation2; 27: X/Y generation2 followed by delivered Z OK. */
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

#if CONFIG_VERSIONS
/* Configuration versions: early. */
#if CFG_WRITE_MODE > 0
#if CFG_API_TRANSPORT
inline cfg_api_cancel_active(n) {
    if :: cfg_api_active_body[n] -> cfg_api_abort_active[n] = true :: else -> skip fi
}
inline cfg_api_cancel_root(n) {
    cfg_api_cancel_active(n);
    if :: len(cfg_invoke_q[n]) -> cfg_api_abort_queue[n] = true :: else -> skip fi
}
#endif
inline cfg_cancel_write(n) {
#if CFG_API_TRANSPORT
    cfg_api_cancel_active(n); cfg_request[n] = 0; cfg_request_pending[n] = 0;
#else
    if
    :: cfg_request[n] -> cfg_client_done[cfg_request[n]] = true; cfg_request[n] = 0
    :: else -> skip
    fi;
    if
    :: cfg_request_pending[n] -> cfg_client_done[cfg_request_pending[n]] = true; cfg_request_pending[n] = 0
    :: else -> skip
    fi;

#endif
    cfg_result[n] = 0; cfg_result_ready[n] = false; cfg_request_base[n] = 0
}
#endif
inline cfg_drop_task(drop_node, drop_owner) {
#if CFG_USE_LOCAL_COOKIE
    cfg_task_local_key[CFG_IDX(drop_node, drop_owner)] = 0;
#endif
#if CFG_ASYNC_READ
    cfg_read_pending[CFG_IDX(drop_node, drop_owner)] = false;
#endif
    cfg_task[CFG_IDX(drop_node, drop_owner)] = 0; cfg_parent[CFG_IDX(drop_node, drop_owner)] = 0;
    cfg_parent_cookie[CFG_IDX(drop_node, drop_owner)] = 0; cfg_parent_session[CFG_IDX(drop_node, drop_owner)] = 0;
#if CFG_WRITE_MODE > 0
    cfg_task_kind[CFG_IDX(drop_node, drop_owner)] = 0; cfg_task_body[CFG_IDX(drop_node, drop_owner)] = 0;
    cfg_local_pending[CFG_IDX(drop_node, drop_owner)] = false;
#endif
    cfg_pending[CFG_IDX(drop_node, drop_owner)] = 0; cfg_records[CFG_IDX(drop_node, drop_owner)] = 0;
    for (cfg_x : 1 .. 3) {
        cfg_task_child_cookie[4 * CFG_IDX(drop_node, drop_owner) + cfg_x] = 0;
        cfg_task_child_session[4 * CFG_IDX(drop_node, drop_owner) + cfg_x] = 0
    };
    cfg_x = 0
}
inline cfg_remember(n, body) {
    if
    :: cfg_generation[body] > cfg_generation[cfg_floor[n]] -> cfg_floor[n] = body; cfg_conflict[n] = false
    :: body && cfg_generation[body] == cfg_generation[cfg_floor[n]] && body != cfg_floor[n] -> cfg_conflict[n] = true
    :: else -> skip
    fi
}
inline cfg_order_ref(order_node, order_origin, order_peer, append_ref) {
    cfg_order_in = cfg_order[CFG_IDX(order_node, order_origin)];
    cfg_order_out = 0; cfg_order_count = 0;
    for (cfg_order_i : 0 .. 2) {
        cfg_order_peer = (cfg_order_in >> (2 * cfg_order_i)) & 3;
        if
        :: cfg_order_peer && cfg_order_peer != order_peer ->
            cfg_order_out = cfg_order_out | (cfg_order_peer << (2 * cfg_order_count)); cfg_order_count++
        :: else -> skip
        fi
    };
    if
    :: append_ref -> assert(cfg_order_count < 3); cfg_order_out = cfg_order_out | (order_peer << (2 * cfg_order_count))
    :: else -> skip
    fi;
    cfg_order[CFG_IDX(order_node, order_origin)] = cfg_order_out;
    cfg_order_in = 0; cfg_order_out = 0; cfg_order_count = 0; cfg_order_i = 0; cfg_order_peer = 0
}
inline cfg_tail_value(tail_node, tail_origin) {
    cfg_tail_peer = cfg_order[CFG_IDX(tail_node, tail_origin)];
    do :: cfg_tail_peer > 3 -> cfg_tail_peer = cfg_tail_peer >> 2 :: else -> break od;
    if
    :: cfg_tail_peer -> cfg_tail_body = CFG_META(cfg_child_report[SLOT(tail_node, cfg_tail_peer)], tail_origin)
    :: else -> cfg_tail_body = 0
    fi
}
inline cfg_metadata_input(meta_node, meta_peer, meta_nodes, meta_data, meta_delta) {
    d_step {
    cfg_meta_old = cfg_child_report[SLOT(meta_node, meta_peer)];
    cfg_meta_new = meta_nodes;
    for (cfg_meta_origin : 1 .. 3) {
        if
        :: meta_nodes & CFG_BIT(cfg_meta_origin) ->
            if
            :: meta_delta & CFG_BIT(cfg_meta_origin) -> cfg_meta_body = CFG_META(meta_data, cfg_meta_origin)
            :: else -> cfg_meta_body = CFG_META(cfg_meta_old, cfg_meta_origin)
            fi;
            cfg_meta_new = cfg_meta_new | (cfg_meta_body << (3 + 3 * (cfg_meta_origin - 1)))
        :: else -> skip
        fi
    };
    cfg_child_report[SLOT(meta_node, meta_peer)] = cfg_meta_new;
    for (cfg_meta_origin : 1 .. 3) {
        cfg_meta_previous = cfg_meta[CFG_IDX(meta_node, cfg_meta_origin)];
        if
        :: meta_nodes & CFG_BIT(cfg_meta_origin) ->
            if
            :: !(cfg_meta_old & CFG_BIT(cfg_meta_origin)) -> cfg_order_ref(meta_node, cfg_meta_origin, meta_peer, true)
            :: meta_delta & CFG_BIT(cfg_meta_origin) ->
                cfg_tail_value(meta_node, cfg_meta_origin);
                if
                :: cfg_generation[CFG_META(cfg_meta_new, cfg_meta_origin)] >= cfg_generation[cfg_tail_body] ->
                    cfg_order_ref(meta_node, cfg_meta_origin, meta_peer, true)
                :: else -> skip
                fi
            :: else -> skip
            fi;
            if
            :: meta_delta & CFG_BIT(cfg_meta_origin) ->
                if :: SCEPTER(meta_node) -> cfg_remember(meta_node, CFG_META(cfg_meta_new, cfg_meta_origin)) :: else -> skip fi
            :: else -> skip
            fi
        :: cfg_meta_old & CFG_BIT(cfg_meta_origin) -> cfg_order_ref(meta_node, cfg_meta_origin, meta_peer, false)
        :: else -> skip
        fi;
        cfg_tail_value(meta_node, cfg_meta_origin);
        cfg_meta[CFG_IDX(meta_node, cfg_meta_origin)] = cfg_tail_body;
        if
        :: cfg_meta_previous != cfg_tail_body -> cfg_changed[meta_node] = cfg_changed[meta_node] | CFG_BIT(cfg_meta_origin)
        :: else -> skip
        fi;
        if
        :: (SCEPTER(meta_node) && (meta_nodes & CFG_BIT(cfg_meta_origin))
            && (cfg_meta_previous != cfg_tail_body
                || (!(cfg_meta_old & CFG_BIT(cfg_meta_origin)) && !CFG_META(cfg_meta_new, cfg_meta_origin)))
            && (!CFG_META(cfg_meta_new, cfg_meta_origin)
                || cfg_generation[CFG_META(cfg_meta_new, cfg_meta_origin)] > cfg_generation[cfg_applied[meta_node]]
                || (cfg_generation[CFG_META(cfg_meta_new, cfg_meta_origin)] == cfg_generation[cfg_applied[meta_node]]
                    && CFG_META(cfg_meta_new, cfg_meta_origin) != cfg_applied[meta_node]))) ->
#if CFG_WRITE_MODE > 0
            cfg_cancel_write(meta_node);
#endif
            cfg_need[meta_node] = true;
            cfg_actor[meta_node] = 0; cfg_phase[meta_node] = 0; cfg_drop_task(meta_node, meta_node)
        :: else -> skip
        fi
    };
    cfg_wire[meta_node] = 0;
    for (cfg_meta_origin : 1 .. 3) {
        cfg_wire[meta_node] = (cfg_wire[meta_node]
                              | (cfg_meta[CFG_IDX(meta_node, cfg_meta_origin)] << (3 + 3 * (cfg_meta_origin - 1))))
    };
    if :: cfg_changed[meta_node] -> cfg_meta_dirty[meta_node] = true :: else -> skip fi;
    cfg_meta_origin = 0; cfg_meta_previous = 0; cfg_meta_body = 0; cfg_meta_old = 0; cfg_meta_new = 0;
    cfg_tail_peer = 0; cfg_tail_body = 0
    }
}

inline cfg_retire_refill(n) {
    /* Keep the ordinary body and any already queued captured timeout key. */
    if
    :: cfg_refill_timeout_key[n] == cfg_refill_actor[n] -> cfg_refill_timeout_key[n] = 0
    :: else -> skip
    fi;
    cfg_refill_actor[n] = 0; cfg_refill_sent[n] = false;
    cfg_refill_parent[n] = 0; cfg_refill_session[n] = 0;
    cfg_refill_cookie[n] = 0; cfg_refill_root[n] = 0
}
inline cfg_cancel_refill(n) {
    cfg_held[n] = 0; cfg_legacy[n] = false; cfg_retire_refill(n)
}
/* The helper fences its own captured binding/root. Generic forwarding actors
 * retain their saved caller and target across intermediate logical rebinding.
 * Physical target disconnection returns ERROR through the saved caller route. */

inline cfg_fence(n) {
#if CFG_API_TRANSPORT
    cfg_api_cancel_root(n);
#endif
#if CFG_WRITE_MODE > 0
    cfg_cancel_write(n);
#endif
    cfg_rpc_errors[n] = true;
    cfg_actor[n] = 0; cfg_phase[n] = 0; cfg_need[n] = true;
    cfg_drop_task(n, n);
    cfg_floor[n] = cfg_applied[n]; cfg_conflict[n] = false
}
inline cfg_binding_changed(n) {
    cfg_cancel_refill(n);
    for (cfg_o : 1 .. 3) {
        cfg_drop_task(n, cfg_o)
    };
    cfg_meta_dirty[n] = true; cfg_tasks_dirty[n] = true;
    cfg_o = 0
}
inline cfg_root_id_changed(n) {
    cfg_cancel_refill(n);
    cfg_meta_dirty[n] = true
}
inline cfg_become_root(n) {
    cfg_cancel_refill(n);
    cfg_need[n] = true;
    cfg_floor[n] = cfg_applied[n]; cfg_conflict[n] = false;
    for (cfg_o : 1 .. 3) { cfg_remember(n, cfg_meta[CFG_IDX(n, cfg_o)]) };
    cfg_o = 0
}
inline cfg_child_changed(n, peer) {
    if
    :: !child_cookie[SLOT(n, peer)] -> cfg_metadata_input(n, peer, 0, 0, 7); cfg_pub_sent[SLOT(n, peer)] = 0
    :: else -> cfg_pub_sent[SLOT(n, peer)] = 0
    fi;
    cfg_tasks_dirty[n] = true; cfg_publish_dirty[n] = true
}
#endif

inline fence_operations(n) {
#if CONFIG_VERSIONS
    cfg_fence(n);
#endif
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
#if CONFIG_VERSIONS
        q?fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value,fresh_data;
#else
        q?fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value;
#endif
        if
        :: ((fresh_peer == owner && (fresh_kind == Initial || fresh_kind == Push || fresh_kind == Unbind || fresh_kind == QueryRoot))
            || (receiver == owner && (fresh_kind == ReversePush || fresh_kind == Reject || fresh_kind == RootReply || fresh_kind == Expired))) ->
            fresh_used[fresh_c] = 1
        :: else -> skip
        fi;
#if CONFIG_VERSIONS
        if
        :: (fresh_kind == CfgGather || fresh_kind == CfgMetadata || fresh_kind == CfgVote || fresh_kind == CfgFreshQuery) && fresh_peer == owner -> fresh_used[fresh_c] = 1
        :: (fresh_kind == CfgCollect || fresh_kind == CfgPublish || fresh_kind == CfgPropose || fresh_kind == CfgFreshReply) && receiver == owner -> fresh_used[fresh_c] = 1
        :: else -> skip
        fi;
#endif
#if CONFIG_VERSIONS
        q!fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value,fresh_data;
#else
        q!fresh_kind,fresh_peer,fresh_sid,fresh_c,fresh_value;
#endif
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
#if CONFIG_VERSIONS
        ; fresh_data = 0
#endif
    }
}

inline enqueue(a, b, kind, sid, c, value) {
#if CONFIG_VERSIONS
    if
    :: len(inbox[b]) >= QUEUE_CAPACITY -> printf("MODEL_BOUND queue node=%d\n", b); assert(false)
    :: else -> inbox[b]!kind,a,sid,c,value,0
    fi
#else
    assert(len(inbox[b]) < QUEUE_CAPACITY);
    inbox[b]!kind,a,sid,c,value
#endif
}

inline send_message(a, b, kind, sid, c, value) {
    /* Once enqueued, a message has reached the Keeper's mailbox. */
    if
    :: SESSION_UP(a, b, sid) ->
#if CONFIG_VERSIONS
        if
        :: kind == Initial || kind == Push ->
            assert(len(inbox[b]) < QUEUE_CAPACITY);
            if
            :: kind == Initial -> inbox[b]!kind,a,sid,c,value,(cfg_wire[a] | subtree[a])
            :: kind == Push -> inbox[b]!kind,a,sid,c,value,(cfg_wire[a] | cfg_changed[a])
            fi;
            cfg_changed[a] = 0; cfg_meta_dirty[a] = false
        :: else -> enqueue(a, b, kind, sid, c, value)
        fi
#else
        enqueue(a, b, kind, sid, c, value)
#endif
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
    refresh_local(n);
#if CONFIG_VERSIONS
    cfg_child_changed(n, peer)
#endif
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
#if CONFIG_VERSIONS
        cfg_binding_changed(n);
#endif
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
#if CONFIG_VERSIONS
    cfg_fence(n); cfg_binding_changed(n);
#endif
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
#if CONFIG_VERSIONS
            ; cfg_become_root(n)
#endif
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
#if CFG_WRITE_MODE > 0 && CHECK_EVENT == 25
    if :: cfg_generation[2] == 2 && cfg_generation[3] == 2 -> witness(25) :: else -> skip fi;
#elif CFG_WRITE_MODE > 0 && CHECK_EVENT == 27
    if :: cfg_generation[2] == 2 && cfg_generation[3] == 2 && cfg_client_ok[4] -> witness(27) :: else -> skip fi;
#endif
#if CHECK_EVENT == 22
    if
    :: (SCEPTER(1) && SCEPTER(2)) || (SCEPTER(1) && SCEPTER(3)) || (SCEPTER(2) && SCEPTER(3)) -> witness(22)
    :: else -> skip
    fi;
#endif
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

#if CONFIG_VERSIONS
/* Configuration versions: inlines. */
inline cfg_clear_scratch() {
    d_step {
    cfg_a = 0; cfg_b = 0; cfg_o = 0; cfg_x = 0; cfg_t = 0; cfg_key = 0;
    cfg_candidate = 0; cfg_stateful = 0; cfg_v = 0; cfg_w = 0; cfg_report = 0; cfg_bad = false;
#if CFG_WRITE_MODE > 0
    cfg_write_i = 0; cfg_write_body = 0; cfg_write_gen = 0; cfg_write_count = 0; cfg_write_key = 0;
#if CFG_API_TRANSPORT
    cfg_api_i = 0; cfg_api_body = 0; cfg_api_kind = 0; cfg_api_key = 0; cfg_api_data = 0; cfg_api_target_data = 0;
    cfg_api_scan_body = 0; cfg_api_scan_kind = 0; cfg_api_scan_key = 0;
#endif
#endif
    for (cfg_a : 0 .. 4) { cfg_copies[cfg_a] = 0; cfg_committed[cfg_a] = 0 };
    cfg_a = 0
    }
}
inline cfg_send(n, peer, kind, sid, c, value, data) {
    if
    :: SESSION_UP(n, peer, sid) ->
        assert(len(inbox[peer]) < QUEUE_CAPACITY); inbox[peer]!kind,n,sid,c,value,data
    :: else -> skip
    fi
}
#if CFG_API_TRANSPORT
inline cfg_api_clear_route(n, body) {
    cfg_api_route_key[CFG_API_IDX(n, body)] = 0; cfg_api_route_peer[CFG_API_IDX(n, body)] = 0;
    cfg_api_route_session[CFG_API_IDX(n, body)] = 0; cfg_api_route_endpoint[CFG_API_IDX(n, body)] = false;
    cfg_api_route_parent[CFG_API_IDX(n, body)] = 0;
    cfg_api_route_parent_session[CFG_API_IDX(n, body)] = 0
}
inline cfg_api_save_route(n, body, peer, sid, key, data) {
    cfg_api_route_key[CFG_API_IDX(n, body)] = key; cfg_api_route_peer[CFG_API_IDX(n, body)] = peer;
    cfg_api_route_session[CFG_API_IDX(n, body)] = sid; cfg_api_route_endpoint[CFG_API_IDX(n, body)] = ((data & CFG_API_ENDPOINT) != 0);
    cfg_api_route_parent[CFG_API_IDX(n, body)] = binding[n];
    cfg_api_route_parent_session[CFG_API_IDX(n, body)] = binding_session[n]
}
inline cfg_api_reply(n, body, key, data) {
    cfg_api_target_data = (data & ~CFG_API_ENDPOINT) | CFG_API_REPLY | body;
    if :: cfg_api_route_endpoint[CFG_API_IDX(n, body)] -> cfg_api_target_data = cfg_api_target_data | CFG_API_ENDPOINT :: else -> skip fi;
    if
    :: cfg_api_route_key[CFG_API_IDX(n, body)] == key ->
        if
        :: cfg_api_route_peer[CFG_API_IDX(n, body)] == n ->
            assert(len(inbox[n]) < QUEUE_CAPACITY); inbox[n]!CfgVote,n,0,0,key,cfg_api_target_data
        :: cfg_api_route_peer[CFG_API_IDX(n, body)] && cfg_api_route_peer[CFG_API_IDX(n, body)] != n && SESSION_UP(n, cfg_api_route_peer[CFG_API_IDX(n, body)], cfg_api_route_session[CFG_API_IDX(n, body)]) ->
            cfg_send(n, cfg_api_route_peer[CFG_API_IDX(n, body)], CfgVote,
                     cfg_api_route_session[CFG_API_IDX(n, body)], 0,
                     key, cfg_api_target_data)
        :: else -> skip
        fi;
        cfg_api_clear_route(n, body)
    :: else -> skip
    fi;
    cfg_api_target_data = 0
}
inline cfg_api_finish_root(n, success, fetched_body) {
    cfg_api_data = 0;
    if :: success -> cfg_api_data = cfg_api_data | CFG_API_OK :: else -> skip fi;
    if :: cfg_api_active_kind[n] == CFG_API_GET_VERSION -> cfg_api_data = cfg_api_data | CFG_API_FETCH | (fetched_body << 3) :: else -> skip fi;
    cfg_api_reply(n, cfg_api_active_body[n], cfg_api_active_key[n], cfg_api_data);
    cfg_api_active_body[n] = 0; cfg_api_active_key[n] = 0; cfg_api_active_kind[n] = 0;
    cfg_api_abort_active[n] = false; cfg_api_data = 0
}
inline cfg_api_abort(n) {
    if :: cfg_api_abort_active[n] -> cfg_api_finish_root(n, false, 0) :: else -> skip fi;
    if
    :: cfg_api_abort_queue[n] ->
        do
        :: len(cfg_invoke_q[n]) ->
            cfg_invoke_q[n]?cfg_api_kind,cfg_api_body,cfg_api_key;
            if :: cfg_api_kind == CFG_API_GET_VERSION -> cfg_api_data = CFG_API_FETCH :: else -> cfg_api_data = 0 fi;
            cfg_api_reply(n, cfg_api_body, cfg_api_key, cfg_api_data)
        :: else -> break
        od;
        cfg_api_abort_queue[n] = false
    :: else -> skip
    fi;
    cfg_api_kind = 0; cfg_api_body = 0; cfg_api_key = 0; cfg_api_data = 0
}
inline cfg_api_receive_request(n, peer, sid, c, key, data) {
    cfg_api_body = CFG_API_BODY(data);
    /* No client_done check: a timed-out caller does not erase a routed command. */
    cfg_api_save_route(n, cfg_api_body, peer, sid, key, data);
    if
    :: SCEPTER(n) && !binding[n] ->
        if
        :: len(cfg_invoke_q[n]) < 2 ->
            if :: data & CFG_API_FETCH -> cfg_api_kind = CFG_API_GET_VERSION :: else -> cfg_api_kind = CFG_API_REPLACE fi;
            cfg_invoke_q[n]!cfg_api_kind,cfg_api_body,key
        :: else -> cfg_api_reply(n, cfg_api_body, key, data & CFG_API_FETCH)
        fi
    :: binding[n] -> cfg_send(n, binding[n], CfgPropose, binding_session[n], 0, key, data & ~CFG_API_ENDPOINT)
    :: else -> cfg_api_reply(n, cfg_api_body, key, data & CFG_API_FETCH)
    fi;
    cfg_api_kind = 0; cfg_api_body = 0
}
inline cfg_api_disconnect(n, peer, sid) {
    for (cfg_api_i : 2 .. 4) {
        if
        :: cfg_api_route_key[CFG_API_IDX(n, cfg_api_i)] && cfg_api_route_parent[CFG_API_IDX(n, cfg_api_i)] == peer && cfg_api_route_parent_session[CFG_API_IDX(n, cfg_api_i)] == sid ->
            cfg_api_reply(n, cfg_api_i, cfg_api_route_key[CFG_API_IDX(n, cfg_api_i)], 0)
        :: else -> skip
        fi
    };
    cfg_api_i = 0
}
#endif
inline cfg_clear_rpc_route(n, origin) {
    cfg_rpc_key[CFG_IDX(n, origin)] = 0;
    cfg_rpc_peer[CFG_IDX(n, origin)] = 0;
    cfg_rpc_session[CFG_IDX(n, origin)] = 0;
    cfg_rpc_cookie[CFG_IDX(n, origin)] = 0;
    cfg_rpc_parent[CFG_IDX(n, origin)] = 0; cfg_rpc_parent_session[CFG_IDX(n, origin)] = 0;
    cfg_rpc_wait[CFG_IDX(n, origin)] = false; cfg_rpc_target_relay[CFG_IDX(n, origin)] = false
}
inline cfg_reply_rpc(n, origin, data) {
    if
    :: CFG_QVALID(n, origin) ->
        cfg_send(n, cfg_rpc_peer[CFG_IDX(n, origin)], CfgFreshReply,
                 cfg_rpc_session[CFG_IDX(n, origin)], cfg_rpc_cookie[CFG_IDX(n, origin)],
                 cfg_rpc_key[CFG_IDX(n, origin)],
                 (((data) & ~CFG_RPC_RELAY) | (cfg_rpc_target_relay[CFG_IDX(n, origin)] * CFG_RPC_RELAY)))
    :: else -> skip
    fi;
    cfg_clear_rpc_route(n, origin)
}
inline cfg_rpc_disconnect(n, peer, sid) {
    for (cfg_o : 1 .. 3) {
        if
        :: (cfg_rpc_key[CFG_IDX(n, cfg_o)] && cfg_rpc_parent[CFG_IDX(n, cfg_o)] == peer
            && cfg_rpc_parent_session[CFG_IDX(n, cfg_o)] == sid) ->
            cfg_reply_rpc(n, cfg_o, CFG_RPC_ERROR | (n << 4))
        :: else -> skip
        fi
    };
    cfg_o = 0
}
inline cfg_answer_fresh(n, body) {
    for (cfg_o : 1 .. 3) {
        if
        :: cfg_rpc_wait[CFG_IDX(n, cfg_o)] -> cfg_reply_rpc(n, cfg_o, body | CFG_RPC_FRESH | (n << 4))
        :: else -> skip
        fi
    };
    cfg_o = 0
}
inline cfg_reply_errors(n) {
    for (cfg_o : 1 .. 3) {
        if
        :: cfg_rpc_wait[CFG_IDX(n, cfg_o)] -> cfg_reply_rpc(n, cfg_o, CFG_RPC_ERROR | (n << 4))
        :: else -> skip
        fi
    };
    cfg_rpc_errors[n] = false; cfg_o = 0
}
inline cfg_reset_publication(n) {
    for (cfg_b : 1 .. 3) {
        if :: cfg_b != n -> cfg_pub_sent[SLOT(n, cfg_b)] = 0 :: else -> skip fi
    };
    cfg_publish_dirty[n] = true; cfg_b = 0
}
/* Put cfg_begin_refill AFTER cfg_allocate's definition. */

inline cfg_allocate(n) {
    d_step {
    for (cfg_scan_i : 0 .. 63) { cfg_used[cfg_scan_i] = 0 };
    if :: cfg_actor[n] -> cfg_used[CFG_TOKEN(cfg_actor[n])] = 1 :: else -> skip fi;
#if CFG_USE_LOCAL_COOKIE
    if :: cfg_task_wire_key && CFG_OWNER(cfg_task_wire_key) == n -> cfg_used[CFG_TOKEN(cfg_task_wire_key)] = 1 :: else -> skip fi;
    for (cfg_scan_i : 0 .. 15) {
        if :: cfg_task_local_key[cfg_scan_i] && CFG_OWNER(cfg_task_local_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_task_local_key[cfg_scan_i])] = 1 :: else -> skip fi
    };
    for (cfg_scan_i : 1 .. 3) {
        if :: cfg_prop_busy_local_key[cfg_scan_i] && CFG_OWNER(cfg_prop_busy_local_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_prop_busy_local_key[cfg_scan_i])] = 1 :: else -> skip fi
    };
#endif
    for (cfg_scan_i : 0 .. 15) {
        if
        :: cfg_task[cfg_scan_i] && CFG_OWNER(cfg_task[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_task[cfg_scan_i])] = 1
        :: else -> skip
        fi
    };
/* Reserve these additional live actor names before allocating cfg_key. */
if :: cfg_refill_actor[n] -> cfg_used[CFG_TOKEN(cfg_refill_actor[n])] = 1 :: else -> skip fi;
if :: cfg_refill_timeout_key[n] -> cfg_used[CFG_TOKEN(cfg_refill_timeout_key[n])] = 1 :: else -> skip fi;
for (cfg_scan_i : 0 .. 15) {
    if
    :: cfg_rpc_key[cfg_scan_i] && CFG_OWNER(cfg_rpc_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_rpc_key[cfg_scan_i])] = 1
    :: else -> skip
    fi
};
/* Existing queue scan must explicitly include CfgFreshQuery/CfgFreshReply/CfgFreshTimeout.
 * Later proposal-IO jobs must also reserve their captured callback actor token. */
#if CFG_WRITE_MODE > 0
for (cfg_scan_i : 1 .. 3) {
    if
    :: cfg_prop_busy_key[cfg_scan_i] && CFG_OWNER(cfg_prop_busy_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_prop_busy_key[cfg_scan_i])] = 1
    :: else -> skip
    fi
};
#endif
#if CFG_API_TRANSPORT
for (cfg_scan_i : 2 .. 4) {
    if :: cfg_api_actor[cfg_scan_i] && CFG_OWNER(cfg_api_actor[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_api_actor[cfg_scan_i])] = 1 :: else -> skip fi;
    if :: cfg_api_deadline_key[cfg_scan_i] && CFG_OWNER(cfg_api_deadline_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_api_deadline_key[cfg_scan_i])] = 1 :: else -> skip fi
};
for (cfg_scan_i : 0 .. 19) {
    if :: cfg_api_route_key[cfg_scan_i] && CFG_OWNER(cfg_api_route_key[cfg_scan_i]) == n -> cfg_used[CFG_TOKEN(cfg_api_route_key[cfg_scan_i])] = 1 :: else -> skip fi
};
for (cfg_scan_n : 1 .. 3) {
    if :: cfg_api_active_key[cfg_scan_n] && CFG_OWNER(cfg_api_active_key[cfg_scan_n]) == n -> cfg_used[CFG_TOKEN(cfg_api_active_key[cfg_scan_n])] = 1 :: else -> skip fi;
    cfg_scan_count = len(cfg_invoke_q[cfg_scan_n]); cfg_scan_i = 0;
    do
    :: cfg_scan_i < cfg_scan_count ->
        cfg_invoke_q[cfg_scan_n]?cfg_api_scan_kind,cfg_api_scan_body,cfg_api_scan_key;
        if :: CFG_OWNER(cfg_api_scan_key) == n -> cfg_used[CFG_TOKEN(cfg_api_scan_key)] = 1 :: else -> skip fi;
        cfg_invoke_q[cfg_scan_n]!cfg_api_scan_kind,cfg_api_scan_body,cfg_api_scan_key;
        cfg_scan_i++
    :: else -> break
    od
};
cfg_api_scan_kind = 0; cfg_api_scan_body = 0; cfg_api_scan_key = 0;
#endif
#if CFG_ASYNC_READ
    for (cfg_read_scan_slot : 0 .. 15) {
        cfg_read_scan_count = len(cfg_reads[cfg_read_scan_slot]); cfg_read_scan_i = 0;
        do
        :: cfg_read_scan_i < cfg_read_scan_count ->
#if CFG_USE_LOCAL_COOKIE
            cfg_reads[cfg_read_scan_slot]?cfg_read_scan_key,cfg_read_scan_local;
            if :: CFG_OWNER(cfg_read_scan_local) == n -> cfg_used[CFG_TOKEN(cfg_read_scan_local)] = 1 :: else -> skip fi;
#else
            cfg_reads[cfg_read_scan_slot]?cfg_read_scan_key;
#endif
            if :: CFG_OWNER(cfg_read_scan_key) == n -> cfg_used[CFG_TOKEN(cfg_read_scan_key)] = 1 :: else -> skip fi;
#if CFG_USE_LOCAL_COOKIE
            cfg_reads[cfg_read_scan_slot]!cfg_read_scan_key,cfg_read_scan_local;
#else
            cfg_reads[cfg_read_scan_slot]!cfg_read_scan_key;
#endif
            cfg_read_scan_i++
        :: else -> break
        od
    };
    cfg_read_scan_slot = 0; cfg_read_scan_count = 0; cfg_read_scan_i = 0; cfg_read_scan_key = 0;
#if CFG_USE_LOCAL_COOKIE
    cfg_read_scan_local = 0;
#endif
#endif
    for (cfg_scan_n : 1 .. 3) {
        cfg_scan_count = len(inbox[cfg_scan_n]); cfg_scan_i = 0;
        do
        :: cfg_scan_i < cfg_scan_count ->
            inbox[cfg_scan_n]?cfg_scan_kind,cfg_scan_peer,cfg_scan_sid,cfg_scan_cookie,cfg_scan_value,cfg_scan_data;
            if
            :: (cfg_scan_kind == CfgCollect || cfg_scan_kind == CfgGather || cfg_scan_kind == CfgPropose || cfg_scan_kind == CfgVote || cfg_scan_kind == CfgFreshQuery || cfg_scan_kind == CfgFreshReply || cfg_scan_kind == CfgFreshTimeout) && cfg_scan_value > 3 && CFG_OWNER(cfg_scan_value) == n -> cfg_used[CFG_TOKEN(cfg_scan_value)] = 1
            :: else -> skip
            fi;
#if CFG_USE_LOCAL_COOKIE
            if
            :: (cfg_scan_n == n && cfg_scan_peer == n && !cfg_scan_sid && !cfg_scan_cookie
                && (0
#if CFG_WRITE_MODE > 0
                    || (cfg_scan_kind == CfgVote && (cfg_scan_data & CFG_LOCAL_STORED))
#endif
#if CFG_ASYNC_READ
                    || (cfg_scan_kind == CfgGather && (cfg_scan_data & CFG_LOCAL_LOADED))
#endif
                   )) -> cfg_used[CFG_LOCAL_TAG(cfg_scan_data)] = 1
            :: else -> skip
            fi;
#endif
            inbox[cfg_scan_n]!cfg_scan_kind,cfg_scan_peer,cfg_scan_sid,cfg_scan_cookie,cfg_scan_value,cfg_scan_data;
            cfg_scan_i++
        :: else -> break
        od
    };
    cfg_scan_i = 1;
    do :: cfg_scan_i < 63 && cfg_used[cfg_scan_i] -> cfg_scan_i++ :: else -> break od;
    assert(!cfg_used[cfg_scan_i]); cfg_key = (cfg_scan_i << 2) | n;
    for (cfg_scan_i : 0 .. 63) { cfg_used[cfg_scan_i] = 0 };
    cfg_scan_n = 0; cfg_scan_i = 0; cfg_scan_count = 0;
    cfg_scan_kind = 0; cfg_scan_peer = 0; cfg_scan_sid = 0; cfg_scan_cookie = 0; cfg_scan_value = 0; cfg_scan_data = 0
    }
}
#if CFG_API_TRANSPORT
inline cfg_api_retire(body) {
    if :: cfg_api_deadline_key[body] == cfg_api_actor[body] -> cfg_api_deadline_key[body] = 0 :: else -> skip fi;
    cfg_api_actor[body] = 0; cfg_api_phase[body] = 0
}
inline cfg_api_issue(body, phase) {
    cfg_allocate(cfg_api_endpoint[body]); cfg_api_actor[body] = cfg_key; cfg_api_phase[body] = phase;
    cfg_api_data = CFG_CLIENT_REQUEST | CFG_API_ENDPOINT | body;
    if :: phase == CFG_API_GET_VERSION -> cfg_api_data = cfg_api_data | CFG_API_FETCH :: else -> skip fi;
    assert(len(inbox[cfg_api_endpoint[body]]) < QUEUE_CAPACITY);
    inbox[cfg_api_endpoint[body]]!CfgPropose,cfg_api_endpoint[body],0,0,cfg_key,cfg_api_data;
    if :: CFG_API_TIMEOUT_MASK & CFG_BIT(body - 1) -> cfg_api_deadline_key[body] = cfg_key :: else -> skip fi;
    cfg_api_data = 0
}
inline cfg_api_check_main_version(body) {
    assert(body == 2 || !cfg_client_ok[2] || cfg_input_version[2] != cfg_input_version[body]);
    assert(body == 3 || !cfg_client_ok[3] || cfg_input_version[3] != cfg_input_version[body]);
    assert(body == 4 || !cfg_client_ok[4] || cfg_input_version[4] != cfg_input_version[body])
}
inline cfg_api_receive_result(n, key, data) {
    cfg_api_body = CFG_API_BODY(data);
    if
    :: (data & CFG_API_ENDPOINT) && n == cfg_api_endpoint[cfg_api_body] && key == cfg_api_actor[cfg_api_body] ->
        if
        :: (data & CFG_API_DEADLINE) ->
            cfg_api_expired[cfg_api_body] = true; cfg_client_done[cfg_api_body] = true;
            cfg_api_retire(cfg_api_body)
        :: !(data & CFG_API_DEADLINE) && !(data & CFG_API_OK) ->
            cfg_client_done[cfg_api_body] = true; cfg_api_retire(cfg_api_body)
        :: !(data & CFG_API_DEADLINE) && (data & CFG_API_OK) && cfg_api_phase[cfg_api_body] == CFG_API_GET_VERSION && (data & CFG_API_FETCH) ->
            if
            :: data & CFG_API_OK ->
                cfg_input_version[cfg_api_body] = cfg_logical_version[CFG_API_FETCH_BODY(data)] + 1;
                cfg_logical_version[cfg_api_body] = cfg_input_version[cfg_api_body];
                /* Keep old key reserved while allocating the replacement actor. */
                if :: cfg_api_deadline_key[cfg_api_body] == key -> cfg_api_deadline_key[cfg_api_body] = 0 :: else -> skip fi;
                cfg_api_issue(cfg_api_body, CFG_API_REPLACE)
            :: else -> cfg_client_done[cfg_api_body] = true; cfg_api_retire(cfg_api_body)
            fi
        :: !(data & CFG_API_DEADLINE) && (data & CFG_API_OK) && cfg_api_phase[cfg_api_body] == CFG_API_REPLACE && !(data & CFG_API_FETCH) ->
            if
            :: data & CFG_API_OK ->
                cfg_write_gen = cfg_generation[cfg_api_body]; assert(cfg_write_gen < 8);
                assert(!cfg_ok_by_generation[cfg_write_gen] || cfg_ok_by_generation[cfg_write_gen] == cfg_api_body);
                cfg_ok_by_generation[cfg_write_gen] = cfg_api_body;
                cfg_api_check_main_version(cfg_api_body);
                cfg_client_ok[cfg_api_body] = true;
                if :: cfg_api_body == 2 -> witness(23) :: cfg_api_body == 4 -> witness(24) :: else -> skip fi
            :: else -> skip
            fi;
            cfg_client_done[cfg_api_body] = true; cfg_api_retire(cfg_api_body)
        :: else -> skip
        fi
    :: !(data & CFG_API_ENDPOINT) && cfg_api_route_key[CFG_API_IDX(n, cfg_api_body)] == key ->
        cfg_api_reply(n, cfg_api_body, key, data & ~CFG_API_REPLY)
    :: else -> skip
    fi;
    cfg_api_body = 0
}
#endif
inline cfg_begin_refill(n) {
    if
    :: cfg_refill_enabled(n) ->
        if
        :: !cfg_refill_actor[n] ->
            cfg_allocate(n); cfg_refill_actor[n] = cfg_key;
            cfg_refill_parent[n] = binding[n]; cfg_refill_session[n] = binding_session[n];
            cfg_refill_cookie[n] = cookie[n]; cfg_refill_root[n] = root[n]
        :: else -> skip
        fi;
        cfg_send(n, binding[n], CfgFreshQuery, binding_session[n], cookie[n], cfg_refill_actor[n], 0);
        cfg_refill_sent[n] = true;
        if
        :: cfg_refill_timeouts_left[n] && !cfg_refill_timeout_key[n] ->
            cfg_refill_timeout_key[n] = cfg_refill_actor[n]; cfg_refill_timeouts_left[n]--
        :: else -> skip
        fi
    :: else -> skip
    fi
}

inline cfg_install(n, body) {
    if
    :: cfg_generation[body] > cfg_generation[cfg_applied[n]] ->
        cfg_applied[n] = body; cfg_memory_c[n] = body;
        assert(len(cfg_writes[n]) < CFG_IO_CAPACITY);
#if CFG_WRITE_MODE > 0
#if CFG_USE_LOCAL_COOKIE
        cfg_writes[n]!cfg_memory_c[n],cfg_memory_p[n],0,0,0;
#else
        cfg_writes[n]!cfg_memory_c[n],cfg_memory_p[n],0,0;
#endif
#else
        cfg_writes[n]!cfg_memory_c[n],cfg_memory_p[n];
#endif
        cfg_metadata_input(n, n, BIT(n), CFG_ONE(n, body, 0), BIT(n))
    :: cfg_generation[body] == cfg_generation[cfg_applied[n]] && body != cfg_applied[n] -> assert(false)
    :: else -> skip
    fi
}
inline cfg_rebuild_metadata(n) {
    if
    :: binding[n] && cfg_changed[n] -> push_dirty = true; send_update(n)
    :: else -> skip
    fi
}
inline cfg_merge(n, owner, data) {
    /* PerformCollect keeps the first reply from each node, including SelfNode. */
    for (cfg_a : 1 .. 3) {
        if
        :: (data & CFG_BIT(cfg_a)) && !(cfg_records[CFG_IDX(n, owner)] & CFG_BIT(cfg_a)) ->
            cfg_records[CFG_IDX(n, owner)] = (cfg_records[CFG_IDX(n, owner)]
                                            | (data & (CFG_BIT(cfg_a) | (7 << (3 + 3 * (cfg_a - 1)))
                                                       | (7 << (12 + 3 * (cfg_a - 1))))))
        :: else -> skip
        fi
    }
}
inline cfg_publish_validated(n) {
    for (cfg_b : 1 .. 3) {
        if
        :: cfg_b != n && child_cookie[SLOT(n, cfg_b)] && cfg_pub_sent[SLOT(n, cfg_b)] != cfg_published[n] && SESSION_UP(n, cfg_b, child_session[SLOT(n, cfg_b)]) ->
            cfg_send(n, cfg_b, CfgPublish, child_session[SLOT(n, cfg_b)], child_cookie[SLOT(n, cfg_b)], cfg_published[n], root[n]);
            cfg_pub_sent[SLOT(n, cfg_b)] = cfg_published[n]
        :: else -> skip
        fi
    };
    cfg_publish_dirty[n] = false
}
inline cfg_publish(n) {
    if
    :: cfg_published[n] && (binding[n] || !cfg_need[n]) && !cfg_conflict[n] && cfg_generation[cfg_published[n]] >= cfg_generation[cfg_floor[n]] -> cfg_publish_validated(n)
    :: else -> cfg_publish_dirty[n] = false
    fi
}
inline cfg_receive_ordinary(n, body) {
    if
    :: cfg_hold_ordinary(n, body) ->
        if :: cfg_generation[body] > cfg_generation[cfg_held[n]] -> cfg_held[n] = body :: else -> skip fi
    :: !cfg_hold_ordinary(n, body) && cfg_generation[body] >= cfg_generation[cfg_applied[n]] ->
        cfg_published[n] = body; cfg_reset_publication(n); cfg_publish(n); cfg_install(n, body)
    :: else -> skip
    fi
}
inline cfg_receive_query(n, peer, sid, c, key, data) {
    cfg_rpc_key[CFG_IDX(n, CFG_OWNER(key))] = key;
    cfg_rpc_peer[CFG_IDX(n, CFG_OWNER(key))] = peer;
    cfg_rpc_session[CFG_IDX(n, CFG_OWNER(key))] = sid;
    cfg_rpc_cookie[CFG_IDX(n, CFG_OWNER(key))] = c;
    cfg_rpc_wait[CFG_IDX(n, CFG_OWNER(key))] = false;
    cfg_rpc_target_relay[CFG_IDX(n, CFG_OWNER(key))] = ((data & CFG_RPC_RELAY) != 0);
    cfg_rpc_parent[CFG_IDX(n, CFG_OWNER(key))] = binding[n];
    cfg_rpc_parent_session[CFG_IDX(n, CFG_OWNER(key))] = binding_session[n];
    if
    :: SCEPTER(n) && !binding[n] ->
        if
#if CFG_WRITE_MODE > 0
        :: (cfg_request[n]
#if CFG_API_TRANSPORT
            || cfg_api_active_body[n]
#endif
           ) -> cfg_reply_rpc(n, CFG_OWNER(key), CFG_RPC_ERROR | (n << 4))
        :: (!cfg_request[n]
#if CFG_API_TRANSPORT
           && !cfg_api_active_body[n]
#endif
           && CFG_NEW_ROOT(n)) ->
#else
        :: CFG_NEW_ROOT(n) ->
#endif
            cfg_rpc_wait[CFG_IDX(n, CFG_OWNER(key))] = true;
            /* The prior read may have started BEFORE this request. Fence only
             * this root's current read; the replacement round starts afterward. */
#if CFG_WRITE_MODE > 0
            cfg_result[n] = 0; cfg_result_ready[n] = false;
#endif
            cfg_actor[n] = 0; cfg_phase[n] = 0; cfg_drop_task(n, n); cfg_need[n] = true
        :: else -> cfg_reply_rpc(n, CFG_OWNER(key), cfg_applied[n] | (n << 4))
        fi
    :: binding[n] -> cfg_send(n, binding[n], CfgFreshQuery, binding_session[n], cookie[n], key, CFG_RPC_RELAY)
    :: else -> cfg_reply_rpc(n, CFG_OWNER(key), CFG_RPC_ERROR | (n << 4))
    fi
}
inline cfg_receive_query_reply(n, key, data) {
    if
    :: (!(data & CFG_RPC_RELAY) && CFG_OWNER(key) == n && key == cfg_refill_actor[n]
       && binding[n] == cfg_refill_parent[n] && binding_session[n] == cfg_refill_session[n]
       && cookie[n] == cfg_refill_cookie[n] && root[n] == cfg_refill_root[n] && !cfg_applied[n]) ->
        if
        :: data & CFG_RPC_ERROR -> cfg_retire_refill(n)
        :: !(data & CFG_RPC_ERROR) && CFG_RPC_ROOT(data) == cfg_refill_root[n] ->
            if
            :: data & CFG_RPC_FRESH ->
                cfg_v = CFG_RPC_BODY(data);
                if :: cfg_v -> cfg_published[n] = cfg_v; cfg_reset_publication(n); cfg_publish(n); cfg_install(n, cfg_v) :: else -> skip fi
            :: else ->
                /* Capability only: do not install CFG_RPC_BODY(data). */
                cfg_legacy[n] = true;
                if :: cfg_held[n] -> cfg_published[n] = cfg_held[n]; cfg_reset_publication(n); cfg_publish(n); cfg_install(n, cfg_held[n]) :: else -> skip fi
            fi;
            cfg_retire_refill(n); cfg_held[n] = 0
        :: else -> cfg_retire_refill(n)
        fi
    :: (data & CFG_RPC_RELAY) && cfg_rpc_key[CFG_IDX(n, CFG_OWNER(key))] == key -> cfg_reply_rpc(n, CFG_OWNER(key), data)
    :: else -> skip
    fi
}

inline cfg_choose(n, records) {
    d_step {
    cfg_candidate = 0; cfg_stateful = 0; cfg_bad = cfg_conflict[n];
    for (cfg_a : 0 .. 4) { cfg_copies[cfg_a] = 0; cfg_committed[cfg_a] = 0 };
    for (cfg_a : 1 .. 3) {
        if
        :: records & CFG_BIT(cfg_a) ->
            cfg_v = CFG_C(records, cfg_a); cfg_w = CFG_P(records, cfg_a);
            if :: cfg_v || cfg_w -> cfg_stateful++ :: else -> skip fi;
            if :: cfg_v -> cfg_copies[cfg_v]++; cfg_committed[cfg_v]++ :: else -> skip fi;
            if :: cfg_w && cfg_w != cfg_v -> cfg_copies[cfg_w]++ :: else -> skip fi
        :: else -> skip
        fi
    };
    for (cfg_a : 1 .. 4) {
        if
        :: ((cfg_copies[cfg_a] >= 2 || cfg_committed[cfg_a]
             || (cfg_a == cfg_floor[n] && cfg_copies[cfg_a]))
            && cfg_generation[cfg_a] > cfg_generation[cfg_candidate]) -> cfg_candidate = cfg_a
        :: else -> skip
        fi
    };
    for (cfg_a : 1 .. 4) {
        if
        :: (cfg_a != cfg_candidate && cfg_generation[cfg_a] == cfg_generation[cfg_candidate]
            && (cfg_committed[cfg_a] || cfg_copies[cfg_a] >= 2)) -> cfg_bad = true
        :: else -> skip
        fi
    };
    }
}
inline cfg_select(n, records) {
    cfg_choose(n, records);
    cfg_actor[n] = 0; cfg_phase[n] = 0;
    if
    :: cfg_stateful >= 2 && cfg_candidate && !cfg_bad && cfg_generation[cfg_candidate] >= cfg_generation[cfg_floor[n]] ->
        cfg_published[n] = cfg_candidate; cfg_need[n] = false;
        cfg_reset_publication(n); cfg_publish(n); cfg_install(n, cfg_candidate);
        cfg_answer_fresh(n, cfg_candidate)
    :: else -> cfg_need[n] = true
    fi
}
#if CFG_WRITE_MODE > 0
inline cfg_finish_task(n, owner) {
    d_step {
    if
    :: (cfg_task[CFG_IDX(n, owner)] && !cfg_pending[CFG_IDX(n, owner)] && !cfg_local_pending[CFG_IDX(n, owner)]
#if CFG_ASYNC_READ
        && !cfg_read_pending[CFG_IDX(n, owner)]
#endif
       ) ->
        if
        :: cfg_parent[CFG_IDX(n, owner)] ->
            if
            :: cfg_task_kind[CFG_IDX(n, owner)] == CFG_TASK_PROPOSE ->
                cfg_send(n, cfg_parent[CFG_IDX(n, owner)], CfgVote,
                         cfg_parent_session[CFG_IDX(n, owner)], cfg_parent_cookie[CFG_IDX(n, owner)],
                         cfg_task[CFG_IDX(n, owner)], cfg_records[CFG_IDX(n, owner)])
            :: else ->
                cfg_send(n, cfg_parent[CFG_IDX(n, owner)], CfgGather,
                         cfg_parent_session[CFG_IDX(n, owner)], cfg_parent_cookie[CFG_IDX(n, owner)],
                         cfg_task[CFG_IDX(n, owner)], cfg_records[CFG_IDX(n, owner)])
            fi
        :: cfg_actor[n] == cfg_task[CFG_IDX(n, owner)] && SCEPTER(n) && !binding[n] ->
            /* The old context is dropped before the next phase allocates a new
             * scatter key. A late read reply cannot masquerade as a vote. */
            cfg_result[n] = cfg_records[CFG_IDX(n, owner)]; cfg_result_ready[n] = true
        :: else -> skip
        fi;
        cfg_drop_task(n, owner)
    :: else -> skip
    fi
    }
}
#else
inline cfg_finish_task(n, owner) {
    if
    :: (cfg_task[CFG_IDX(n, owner)] && !cfg_pending[CFG_IDX(n, owner)]
#if CFG_ASYNC_READ
        && !cfg_read_pending[CFG_IDX(n, owner)]
#endif
       ) ->
        if
        :: cfg_parent[CFG_IDX(n, owner)] ->
            cfg_send(n, cfg_parent[CFG_IDX(n, owner)], CfgGather,
                     cfg_parent_session[CFG_IDX(n, owner)], cfg_parent_cookie[CFG_IDX(n, owner)],
                     cfg_task[CFG_IDX(n, owner)], cfg_records[CFG_IDX(n, owner)])
        :: cfg_actor[n] == cfg_task[CFG_IDX(n, owner)] && SCEPTER(n) && !binding[n] -> cfg_select(n, cfg_records[CFG_IDX(n, owner)])
        :: else -> skip
        fi;
        cfg_drop_task(n, owner)
    :: else -> skip
    fi
}
#endif
inline cfg_sync_task(n, owner) {
    for (cfg_t : 1 .. 3) {
        if
        :: cfg_t != n && child_cookie[SLOT(n, cfg_t)] ->
            if
            :: cfg_task_child_cookie[4 * CFG_IDX(n, owner) + cfg_t] != child_cookie[SLOT(n, cfg_t)] || cfg_task_child_session[4 * CFG_IDX(n, owner) + cfg_t] != child_session[SLOT(n, cfg_t)] ->
                cfg_task_child_cookie[4 * CFG_IDX(n, owner) + cfg_t] = child_cookie[SLOT(n, cfg_t)];
                cfg_task_child_session[4 * CFG_IDX(n, owner) + cfg_t] = child_session[SLOT(n, cfg_t)];
                cfg_pending[CFG_IDX(n, owner)] = cfg_pending[CFG_IDX(n, owner)] | CFG_BIT(cfg_t);
                #if CFG_WRITE_MODE > 0
if
:: cfg_task_kind[CFG_IDX(n, owner)] == CFG_TASK_PROPOSE ->
    cfg_send(n, cfg_t, CfgPropose, child_session[SLOT(n, cfg_t)], child_cookie[SLOT(n, cfg_t)],
             cfg_task[CFG_IDX(n, owner)], cfg_task_body[CFG_IDX(n, owner)])
:: else ->
    cfg_send(n, cfg_t, CfgCollect, child_session[SLOT(n, cfg_t)], child_cookie[SLOT(n, cfg_t)],
             cfg_task[CFG_IDX(n, owner)], 0)
fi
#else
                cfg_send(n, cfg_t, CfgCollect, child_session[SLOT(n, cfg_t)], child_cookie[SLOT(n, cfg_t)], cfg_task[CFG_IDX(n, owner)], 0)
#endif
            :: else -> skip
            fi
        :: else ->
            cfg_pending[CFG_IDX(n, owner)] = cfg_pending[CFG_IDX(n, owner)] & ~CFG_BIT(cfg_t);
            cfg_task_child_cookie[4 * CFG_IDX(n, owner) + cfg_t] = 0; cfg_task_child_session[4 * CFG_IDX(n, owner) + cfg_t] = 0
        fi
    }
}
inline cfg_restart_task(n, wire_key) {
#if CFG_USE_LOCAL_COOKIE
    /* Every received/replayed scatter gets a new keeper-local cookie in C++.
     * It is separate from the parent's cookie retained in the gather response. */
    cfg_task_wire_key = wire_key;
    cfg_drop_task(n, CFG_OWNER(cfg_task_wire_key));
    cfg_allocate(n);
    cfg_task_local_key[CFG_IDX(n, CFG_OWNER(cfg_task_wire_key))] = cfg_key;
    cfg_key = cfg_task_wire_key; cfg_task_wire_key = 0
#else
    cfg_drop_task(n, CFG_OWNER(wire_key))
#endif
}
#if CFG_WRITE_MODE > 0
inline cfg_start_scatter(n, key, parent, sid, c, task_kind, body) {
    cfg_restart_task(n, key);
    d_step {
    cfg_task[CFG_IDX(n, CFG_OWNER(key))] = key;
    cfg_task_kind[CFG_IDX(n, CFG_OWNER(key))] = task_kind;
    cfg_task_body[CFG_IDX(n, CFG_OWNER(key))] = body;
    cfg_parent[CFG_IDX(n, CFG_OWNER(key))] = parent;
    cfg_parent_session[CFG_IDX(n, CFG_OWNER(key))] = sid; cfg_parent_cookie[CFG_IDX(n, CFG_OWNER(key))] = c;
    if
    :: task_kind == CFG_TASK_READ ->
#if CFG_ASYNC_READ
        cfg_records[CFG_IDX(n, CFG_OWNER(key))] = CFG_BIT(n);
        cfg_read_pending[CFG_IDX(n, CFG_OWNER(key))] = true;
        assert(len(cfg_reads[CFG_IDX(n, CFG_OWNER(key))]) < CFG_READ_CAPACITY);
#if CFG_USE_LOCAL_COOKIE
        cfg_reads[CFG_IDX(n, CFG_OWNER(key))]!key,cfg_task_local_key[CFG_IDX(n, CFG_OWNER(key))]
#else
        cfg_reads[CFG_IDX(n, CFG_OWNER(key))]!key
#endif
#else
        cfg_records[CFG_IDX(n, CFG_OWNER(key))] = CFG_ONE(n, cfg_c[n], cfg_p[n])
#endif
    :: else ->
        cfg_records[CFG_IDX(n, CFG_OWNER(key))] = 0;
        if
        :: (cfg_memory_c[n] || cfg_memory_p[n]) && !cfg_prop_busy_key[n] && cfg_generation[body] > cfg_generation[cfg_applied[n]] ->
            /* Existing uncommitted proposed is intentionally not a promise. */
            cfg_memory_p[n] = body; cfg_prop_busy_key[n] = key; cfg_prop_busy_body[n] = body;
            cfg_local_pending[CFG_IDX(n, CFG_OWNER(key))] = true;
            assert(len(cfg_writes[n]) < CFG_IO_CAPACITY);
#if CFG_USE_LOCAL_COOKIE
            cfg_prop_busy_local_key[n] = cfg_task_local_key[CFG_IDX(n, CFG_OWNER(key))];
            cfg_writes[n]!cfg_memory_c[n],cfg_memory_p[n],key,body,cfg_prop_busy_local_key[n]
#else
            cfg_writes[n]!cfg_memory_c[n],cfg_memory_p[n],key,body
#endif
        :: else -> skip
        fi
    fi;
    };
    cfg_sync_task(n, CFG_OWNER(key)); cfg_finish_task(n, CFG_OWNER(key))
}
inline cfg_start_task(n, key, parent, sid, c) {
    cfg_start_scatter(n, key, parent, sid, c, CFG_TASK_READ, 0)
}
#else
inline cfg_start_task(n, key, parent, sid, c) {
    cfg_restart_task(n, key);
    cfg_task[CFG_IDX(n, CFG_OWNER(key))] = key;
    cfg_parent[CFG_IDX(n, CFG_OWNER(key))] = parent;
    cfg_parent_session[CFG_IDX(n, CFG_OWNER(key))] = sid; cfg_parent_cookie[CFG_IDX(n, CFG_OWNER(key))] = c;
    cfg_pending[CFG_IDX(n, CFG_OWNER(key))] = 0;
#if CFG_ASYNC_READ
    cfg_records[CFG_IDX(n, CFG_OWNER(key))] = CFG_BIT(n);
    cfg_read_pending[CFG_IDX(n, CFG_OWNER(key))] = true;
    assert(len(cfg_reads[CFG_IDX(n, CFG_OWNER(key))]) < CFG_READ_CAPACITY);
#if CFG_USE_LOCAL_COOKIE
    cfg_reads[CFG_IDX(n, CFG_OWNER(key))]!key,cfg_task_local_key[CFG_IDX(n, CFG_OWNER(key))];
#else
    cfg_reads[CFG_IDX(n, CFG_OWNER(key))]!key;
#endif
#else
    cfg_records[CFG_IDX(n, CFG_OWNER(key))] = CFG_ONE(n, cfg_c[n], cfg_p[n]);
#endif
    for (cfg_t : 1 .. 3) {
        cfg_task_child_cookie[4 * CFG_IDX(n, CFG_OWNER(key)) + cfg_t] = 0;
        cfg_task_child_session[4 * CFG_IDX(n, CFG_OWNER(key)) + cfg_t] = 0
    };
    cfg_sync_task(n, CFG_OWNER(key)); cfg_finish_task(n, CFG_OWNER(key))
}
#endif
#if CFG_WRITE_MODE > 0
inline cfg_new_round(n, phase) {
    cfg_allocate(n); cfg_actor[n] = cfg_key; cfg_phase[n] = phase;
    cfg_result[n] = 0; cfg_result_ready[n] = false;
    if
    :: phase == CFG_PHASE_PROPOSE -> cfg_start_scatter(n, cfg_key, 0, 0, 0, CFG_TASK_PROPOSE, cfg_request[n])
    :: else -> cfg_start_task(n, cfg_key, 0, 0, 0)
    fi
}
inline cfg_commit_observer() {
    d_step {
    /* Observer only: never controls a keeper, vote, publication, or read. */
    for (cfg_write_body : 1 .. 4) {
        cfg_write_count = 0;
        for (cfg_write_i : 1 .. 3) {
            if :: cfg_c[cfg_write_i] == cfg_write_body -> cfg_write_count++ :: else -> skip fi
        };
        if
        :: cfg_write_count >= 2 && cfg_generation[cfg_write_body] > cfg_generation[cfg_expected] -> cfg_expected = cfg_write_body
        :: else -> skip
        fi
    };
    cfg_write_body = 0; cfg_write_i = 0; cfg_write_count = 0
    }
}
inline cfg_fail_request(n) {
#if CFG_API_TRANSPORT
    cfg_api_finish_root(n, false, 0);
#else
    if :: cfg_request[n] -> cfg_client_done[cfg_request[n]] = true :: else -> skip fi;
#endif
    cfg_request[n] = 0; cfg_request_base[n] = 0; cfg_actor[n] = 0; cfg_phase[n] = 0;
    cfg_result[n] = 0; cfg_result_ready[n] = false; cfg_need[n] = true
}
inline cfg_complete_request(n) {
#if CFG_API_TRANSPORT
    cfg_api_finish_root(n, true, 0);
#else
    cfg_write_body = cfg_request[n]; cfg_write_gen = cfg_generation[cfg_write_body];
    assert(cfg_write_gen < 8);
    assert(!cfg_ok_by_generation[cfg_write_gen] || cfg_ok_by_generation[cfg_write_gen] == cfg_write_body);
    cfg_ok_by_generation[cfg_write_gen] = cfg_write_body;
    cfg_client_done[cfg_write_body] = true; cfg_client_ok[cfg_write_body] = true;
    if
    :: cfg_write_gen > cfg_generation[cfg_expected] -> cfg_expected = cfg_write_body
    :: else -> skip
    fi;
    if :: cfg_write_body == 2 -> witness(23) :: cfg_write_body == 4 -> witness(24) :: else -> skip fi;
#endif
    cfg_request[n] = 0; cfg_request_base[n] = 0; cfg_actor[n] = 0; cfg_phase[n] = 0;
    cfg_result[n] = 0; cfg_result_ready[n] = false
}
inline cfg_process_result(n) {
    if
    :: cfg_phase[n] == CFG_PHASE_RECOVERY -> cfg_select(n, cfg_result[n]); cfg_result[n] = 0; cfg_result_ready[n] = false
    :: cfg_phase[n] == CFG_PHASE_PREFLIGHT ->
        /* cfg_choose is the existing cfg_select prefix through cfg_bad/floor
         * selection, without actor/phase reset or publication/install. Factor
         * that unchanged prefix into cfg_choose(n,records), used by both. */
        cfg_choose(n, cfg_result[n]);
        if
        :: cfg_stateful >= 2 && cfg_candidate && !cfg_bad && cfg_generation[cfg_candidate] >= cfg_generation[cfg_floor[n]] ->
            cfg_published[n] = cfg_candidate; cfg_need[n] = false;
            cfg_reset_publication(n);
            /* No actor may be cleared merely to publish: fanout is allowed for
             * this explicit validated operation despite its active read key. */
            cfg_publish_validated(n); cfg_install(n, cfg_candidate);
            if
            :: cfg_candidate != cfg_request_base[n] || cfg_applied[n] != cfg_request_base[n] ->
                /* Actual fresh preflight reports transient RACE after recovery
                 * changes its captured base; this API request does not continue
                 * automatically as a write on the newly recovered body. */
                cfg_fail_request(n)
            :: else ->
                cfg_write_gen = cfg_generation[cfg_applied[n]];
                for (cfg_write_i : 1 .. 3) {
                    if
                    :: cfg_result[n] & CFG_BIT(cfg_write_i) ->
                        cfg_write_body = CFG_C(cfg_result[n], cfg_write_i);
                        if :: cfg_generation[cfg_write_body] > cfg_write_gen -> cfg_write_gen = cfg_generation[cfg_write_body] :: else -> skip fi;
                        cfg_write_body = CFG_P(cfg_result[n], cfg_write_i);
                        if :: cfg_generation[cfg_write_body] > cfg_write_gen -> cfg_write_gen = cfg_generation[cfg_write_body] :: else -> skip fi
                    :: else -> skip
                    fi
                };
                assert(cfg_generation[cfg_request[n]] == 0);
                cfg_generation[cfg_request[n]] = cfg_write_gen + 1;
                cfg_new_round(n, CFG_PHASE_PROPOSE)
            fi
        :: else -> cfg_fail_request(n)
        fi
    :: cfg_phase[n] == CFG_PHASE_PROPOSE ->
        cfg_write_count = 0;
        for (cfg_write_i : 1 .. 3) {
            if :: cfg_result[n] & CFG_BIT(cfg_write_i) -> cfg_write_count++ :: else -> skip fi
        };
        if
        :: cfg_write_count >= 2 && !cfg_need[n] && !cfg_conflict[n] ->
            cfg_published[n] = cfg_request[n]; cfg_need[n] = false;
            cfg_reset_publication(n); cfg_publish_validated(n); cfg_install(n, cfg_request[n]);
            cfg_new_round(n, CFG_PHASE_CONFIRM)
        :: else -> cfg_fail_request(n)
        fi
    :: cfg_phase[n] == CFG_PHASE_CONFIRM ->
        cfg_choose(n, cfg_result[n]);
        cfg_write_count = 0;
        for (cfg_write_i : 1 .. 3) {
            if :: (cfg_result[n] & CFG_BIT(cfg_write_i)) && CFG_C(cfg_result[n], cfg_write_i) == cfg_request[n] -> cfg_write_count++ :: else -> skip fi
        };
        if
        :: cfg_stateful >= 2 && !cfg_bad && cfg_candidate == cfg_request[n] && cfg_write_count >= 2 && !cfg_need[n] -> cfg_complete_request(n)
        :: cfg_stateful >= 2 && !cfg_bad && cfg_candidate == cfg_request[n] && cfg_write_count < 2 && !cfg_need[n] -> cfg_new_round(n, CFG_PHASE_CONFIRM)
        :: else -> cfg_fail_request(n)
        fi
#if CFG_API_TRANSPORT
:: cfg_phase[n] == CFG_PHASE_FETCH ->
    cfg_choose(n, cfg_result[n]);
    if
    :: cfg_stateful >= 2 && cfg_candidate && !cfg_bad && cfg_generation[cfg_candidate] >= cfg_generation[cfg_floor[n]] ->
        cfg_published[n] = cfg_candidate; cfg_need[n] = false;
        cfg_reset_publication(n); cfg_publish_validated(n); cfg_install(n, cfg_candidate);
        cfg_api_finish_root(n, true, cfg_candidate)
    :: else -> cfg_api_finish_root(n, false, 0); cfg_need[n] = true
    fi;
    cfg_actor[n] = 0; cfg_phase[n] = 0; cfg_result[n] = 0; cfg_result_ready[n] = false
#endif
    :: else -> cfg_result[n] = 0; cfg_result_ready[n] = false
    fi
}
inline cfg_receive_client(n, body, data) {
    if
    :: cfg_client_done[body] -> skip
    :: !cfg_client_done[body] && SCEPTER(n) && !binding[n] && !cfg_request_pending[n] && !cfg_request[n] -> cfg_request_pending[n] = body
    :: !cfg_client_done[body] && binding[n] && !(data & CFG_CLIENT_FORWARDED) ->
        cfg_send(n, binding[n], CfgPropose, binding_session[n], cookie[n], 0, data | CFG_CLIENT_FORWARDED)
    :: !cfg_client_done[body] && binding[n] && (data & CFG_CLIENT_FORWARDED) ->
        /* For N=3 an admitted acyclic tree has at most two parent hops. */
        cfg_send(n, binding[n], CfgPropose, binding_session[n], cookie[n], 0, data)
    :: else -> cfg_client_done[body] = true
    fi
}
#endif
inline cfg_tick(n) {
    if
    :: cfg_meta_dirty[n] ->
        cfg_rebuild_metadata(n);
        cfg_meta_dirty[n] = false
    :: else -> skip
    fi;
    if
    :: cfg_tasks_dirty[n] ->
        for (cfg_o : 1 .. 3) {
            if :: cfg_task[CFG_IDX(n, cfg_o)] -> cfg_sync_task(n, cfg_o); cfg_finish_task(n, cfg_o) :: else -> skip fi
        };
        cfg_tasks_dirty[n] = false
    :: else -> skip
    fi;
#if CFG_WRITE_MODE > 0
if
:: cfg_result_ready[n] -> cfg_process_result(n)
:: else -> skip
fi;
#if CFG_API_TRANSPORT
if :: cfg_api_abort_active[n] || cfg_api_abort_queue[n] -> cfg_api_abort(n) :: else -> skip fi;
if
:: SCEPTER(n) && !binding[n] && !cfg_actor[n] && !cfg_api_active_body[n] && len(cfg_invoke_q[n]) ->
    cfg_invoke_q[n]?cfg_api_kind,cfg_api_body,cfg_api_key;
    cfg_api_active_body[n] = cfg_api_body; cfg_api_active_key[n] = cfg_api_key; cfg_api_active_kind[n] = cfg_api_kind;
    if
    :: cfg_api_kind == CFG_API_GET_VERSION -> cfg_new_round(n, CFG_PHASE_FETCH)
    :: else ->
        if
        :: cfg_input_version[cfg_api_body] == cfg_logical_version[cfg_applied[n]] + 1 ->
            cfg_request[n] = cfg_api_body; cfg_request_base[n] = cfg_applied[n]; cfg_new_round(n, CFG_PHASE_PREFLIGHT)
        :: else -> cfg_api_finish_root(n, false, 0)
        fi
    fi;
    cfg_api_kind = 0; cfg_api_body = 0; cfg_api_key = 0
:: else -> skip
fi;
#else
if
:: cfg_request_pending[n] && !cfg_actor[n] && SCEPTER(n) && !binding[n] ->
    cfg_request[n] = cfg_request_pending[n]; cfg_request_pending[n] = 0;
    cfg_request_base[n] = cfg_applied[n];
    cfg_new_round(n, CFG_PHASE_PREFLIGHT)
:: else -> skip
fi;
#endif
#endif
    if
    :: (SCEPTER(n) && !binding[n] && cfg_need[n] && !cfg_actor[n]
#if CFG_WRITE_MODE > 0
       && !cfg_request[n] && !cfg_request_pending[n]
#if CFG_API_TRANSPORT
       && !cfg_api_active_body[n] && !len(cfg_invoke_q[n])
#endif
#endif
       ) ->
        cfg_allocate(n); cfg_actor[n] = cfg_key; cfg_phase[n] = 1;
        cfg_start_task(n, cfg_key, 0, 0, 0)
    :: else -> skip
    fi;
    if :: cfg_publish_dirty[n] -> cfg_publish(n) :: else -> skip fi;
    if :: cfg_rpc_errors[n] -> cfg_reply_errors(n) :: else -> skip fi;
if :: cfg_refill_enabled(n) -> cfg_begin_refill(n) :: else -> skip fi;
    cfg_clear_scratch()
}
inline cfg_handle(n, kind, peer, sid, c, value, data) {
    if
#if CFG_ASYNC_READ
    :: kind == CfgGather && peer == n && !sid && !c && (data & CFG_LOCAL_LOADED) ->
        if
        :: cfg_task[CFG_IDX(n, CFG_OWNER(value))] == value && cfg_read_pending[CFG_IDX(n, CFG_OWNER(value))] && CFG_LOCAL_CALLBACK_VALID(n,value,data) ->
            cfg_records[CFG_IDX(n, CFG_OWNER(value))] = (cfg_records[CFG_IDX(n, CFG_OWNER(value))] & ~(7 << (3 + 3 * (n - 1))) & ~(7 << (12 + 3 * (n - 1)))) | (data & ~(CFG_LOCAL_LOADED | CFG_LOCAL_TAG_MASK));
            cfg_read_pending[CFG_IDX(n, CFG_OWNER(value))] = false;
            cfg_finish_task(n, CFG_OWNER(value))
        :: else -> skip
        fi
#endif
#if CFG_WRITE_MODE > 0
:: (kind == CfgVote && peer == n && !sid && !c && (data & CFG_LOCAL_STORED)
#if CFG_API_TRANSPORT
   && !(data & CFG_API_REPLY)
#endif
   ) ->
    assert(cfg_prop_busy_key[n] == value && cfg_prop_busy_body[n] == (data & 7));
#if CFG_USE_LOCAL_COOKIE
    assert(CFG_LOCAL_TAG(data) == CFG_TOKEN(cfg_prop_busy_local_key[n]));
    cfg_prop_busy_local_key[n] = 0;
#endif
    cfg_prop_busy_key[n] = 0; cfg_prop_busy_body[n] = 0;
    if
    :: cfg_task[CFG_IDX(n, CFG_OWNER(value))] == value && cfg_task_kind[CFG_IDX(n, CFG_OWNER(value))] == CFG_TASK_PROPOSE && CFG_LOCAL_CALLBACK_VALID(n,value,data) ->
        cfg_records[CFG_IDX(n, CFG_OWNER(value))] = cfg_records[CFG_IDX(n, CFG_OWNER(value))] | CFG_BIT(n);
        cfg_local_pending[CFG_IDX(n, CFG_OWNER(value))] = false;
        cfg_finish_task(n, CFG_OWNER(value))
    :: else -> skip
    fi
#if CFG_API_TRANSPORT
:: kind == CfgPropose && (data & CFG_CLIENT_REQUEST) && ((peer == n && !sid && !c) || (peer != n && sid)) -> cfg_api_receive_request(n, peer, sid, c, value, data)
:: kind == CfgVote && (data & CFG_API_REPLY) && (((data & CFG_API_ENDPOINT) && peer == n && !sid && !c) || (!(data & CFG_API_ENDPOINT) && peer != n && cfg_api_route_key[CFG_API_IDX(n, CFG_API_BODY(data))] == value && cfg_api_route_parent[CFG_API_IDX(n, CFG_API_BODY(data))] == peer && cfg_api_route_parent_session[CFG_API_IDX(n, CFG_API_BODY(data))] == sid && sid)) -> cfg_api_receive_result(n, value, data)
#else
:: kind == CfgPropose && (data & CFG_CLIENT_REQUEST) && ((peer == n && !sid && !c) || (child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid)) -> cfg_receive_client(n, data & 7, data)
#endif
:: kind == CfgPropose && !(data & CFG_CLIENT_REQUEST) && binding[n] == peer && binding_session[n] == sid && cookie[n] == c -> cfg_start_scatter(n, value, peer, sid, c, CFG_TASK_PROPOSE, data & 7)
:: (kind == CfgVote
#if CFG_API_TRANSPORT
   && !(data & CFG_API_REPLY)
#endif
   && peer != n && child_cookie[SLOT(n, peer)] && child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid && cfg_task[CFG_IDX(n, CFG_OWNER(value))] == value && cfg_task_kind[CFG_IDX(n, CFG_OWNER(value))] == CFG_TASK_PROPOSE) ->
    cfg_records[CFG_IDX(n, CFG_OWNER(value))] = cfg_records[CFG_IDX(n, CFG_OWNER(value))] | (data & 7);
    cfg_pending[CFG_IDX(n, CFG_OWNER(value))] = cfg_pending[CFG_IDX(n, CFG_OWNER(value))] & ~CFG_BIT(peer);
    cfg_finish_task(n, CFG_OWNER(value))
#endif
    :: kind == CfgCollect && binding[n] == peer && binding_session[n] == sid && cookie[n] == c -> cfg_start_task(n, value, peer, sid, c)
    :: (kind == CfgGather && peer != n && child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid && cfg_task[CFG_IDX(n, CFG_OWNER(value))] == value
#if CFG_WRITE_MODE > 0
       && cfg_task_kind[CFG_IDX(n, CFG_OWNER(value))] == CFG_TASK_READ
#endif
       ) ->
        cfg_merge(n, CFG_OWNER(value), data);
        cfg_pending[CFG_IDX(n, CFG_OWNER(value))] = cfg_pending[CFG_IDX(n, CFG_OWNER(value))] & ~CFG_BIT(peer);
        cfg_finish_task(n, CFG_OWNER(value))
    :: kind == CfgPublish && binding[n] == peer && binding_session[n] == sid && cookie[n] == c ->
        /* Root identity and body belong to the same ordinary ReversePush. */
        awaiting[n] = false; timer_armed[n] = false; request_sent[n] = false;
        if
        :: data == n -> abort_binding(n, true, false)
        :: else ->
            if :: root[n] != data -> cfg_root_id_changed(n) :: else -> skip fi;
            root[n] = data; fanout(n)
        fi;
        cfg_receive_ordinary(n, value)
    :: kind == CfgFreshQuery && peer != n && sid -> cfg_receive_query(n, peer, sid, c, value, data)
    :: (kind == CfgFreshReply
        && (((data & CFG_RPC_RELAY)
            && cfg_rpc_key[CFG_IDX(n, CFG_OWNER(value))] == value
            && cfg_rpc_parent[CFG_IDX(n, CFG_OWNER(value))] == peer
            && cfg_rpc_parent_session[CFG_IDX(n, CFG_OWNER(value))] == sid && peer != n && sid)
           || (!(data & CFG_RPC_RELAY) && binding[n] == peer && binding_session[n] == sid && cookie[n] == c))) ->
        cfg_receive_query_reply(n, value, data)
:: kind == CfgFreshTimeout && peer == n && value == cfg_refill_actor[n] && cfg_refill_sent[n] -> cfg_retire_refill(n)
    :: kind == CfgMetadata && child_cookie[SLOT(n, peer)] == c && child_session[SLOT(n, peer)] == sid ->
        cfg_metadata_input(n, peer, data & 7, data, data & 7)
    :: else -> skip
    fi;
    cfg_clear_scratch()
}
inline cfg_init() {
    cfg_generation[0] = 0; cfg_generation[1] = 1;
#if CFG_API_TRANSPORT
    cfg_input_version[1] = 1; cfg_logical_version[1] = 1;
    cfg_input_version[2] = 2; cfg_logical_version[2] = 2; cfg_api_endpoint[2] = CFG_CLIENT_ENDPOINT;
    cfg_input_version[3] = 2; cfg_logical_version[3] = 2; cfg_api_endpoint[3] = 3;
#if CFG_TARGET_SAMEGEN
    cfg_api_endpoint[4] = 3;
#else
    cfg_api_endpoint[4] = CFG_CLIENT_ENDPOINT;
#endif
#endif
#if CFG_WRITE_MODE > 0
    assert(!CFG_HIGH_MASK);
#else
    cfg_generation[2] = 2; cfg_generation[3] = 2; cfg_generation[4] = 3;
#endif
    for (cfg_a : 1 .. 3) {
        if
        :: CFG_EMPTY_MASK & CFG_BIT(cfg_a) -> cfg_c[cfg_a] = 0
        :: CFG_HIGH_MASK & CFG_BIT(cfg_a) -> cfg_c[cfg_a] = 2
        :: else -> cfg_c[cfg_a] = 1
        fi;
        cfg_memory_c[cfg_a] = cfg_c[cfg_a]; cfg_applied[cfg_a] = cfg_c[cfg_a];
        cfg_floor[cfg_a] = cfg_c[cfg_a];
        cfg_refill_timeouts_left[cfg_a] = CFG_RPC_TIMEOUT_BUDGET;
        cfg_meta[CFG_IDX(cfg_a, cfg_a)] = cfg_c[cfg_a]; cfg_order[CFG_IDX(cfg_a, cfg_a)] = cfg_a;
        cfg_child_report[SLOT(cfg_a, cfg_a)] = CFG_ONE(cfg_a, cfg_c[cfg_a], 0);
        cfg_wire[cfg_a] = cfg_c[cfg_a] << (3 + 3 * (cfg_a - 1)); cfg_changed[cfg_a] = BIT(cfg_a)
    };
    if :: (CFG_HIGH_MASK == 3 || CFG_HIGH_MASK == 5 || CFG_HIGH_MASK == 6 || CFG_HIGH_MASK == 7) -> cfg_expected = 2 :: else -> skip fi;
    cfg_clear_scratch()
}
#endif

/* Initial activation and asynchronous operation handoff. Retries run in Delivery. */
proctype Keeper(byte n) {
#if CONFIG_VERSIONS
    atomic { ONLINE(n) -> reconcile(n); audit() };
    do
    :: atomic {
        ONLINE(n) && cfg_run_enabled(n) && CFG_TARGET_KEEPER(n) -> cfg_tick(n); reconcile(n); audit()
    }
    od
#else
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
#if CONFIG_VERSIONS
    int data;
#endif
    bool obsolete;
    byte old_scepters; byte old_binding; byte old_root; byte old_probe;
    do
    ::
#if CONFIG_VERSIONS
       atomic {
#else
       d_step {
#endif
#if CONFIG_VERSIONS
        (ONLINE(n) && len(inbox[n]) && CFG_TARGET_DELIVERY(n)) ->
#else
        ONLINE(n) && len(inbox[n]) ->
#endif
#if CONFIG_VERSIONS
        inbox[n]?kind,peer,sid,c,value,data;
#else
        inbox[n]?kind,peer,sid,c,value;
#endif
        obsolete = (kind == RootReply || kind == ReversePush || kind == Reject) && c != cookie[n];
        old_scepters = scepters; old_binding = binding[n]; old_root = root[n]; old_probe = probe[n];
        if
#if CONFIG_VERSIONS
        :: CFG_MESSAGE(kind) ->
            cfg_handle(n, kind, peer, sid, c, value, data)
#endif
        :: kind == Wakeup ->
            wakeup_pending[n] = false;
            retry_ready[n] = true
        :: kind == ErrorTimeout ->
            assert(error_wait[n]);
            error_wait[n] = false;
            /* The 1.5s minimum error backoff exceeds the 1s bind retry delay. */
            retry_ready[n] = true
        :: kind == Disconnected ->
#if CONFIG_VERSIONS
            cfg_rpc_disconnect(n, peer, sid);
#endif
#if CFG_API_TRANSPORT
            cfg_api_disconnect(n, peer, sid);
#endif
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
        :: (kind != Disconnected && kind != Expired && kind != Wakeup && kind != ErrorTimeout
#if CONFIG_VERSIONS
            && !CFG_MESSAGE(kind)
#endif
            ) ->
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
#if CONFIG_VERSIONS
                    cfg_metadata_input(n, peer, value, data, data & 7);
                    if :: cfg_changed[n] && binding[n] -> push_dirty = true :: else -> skip fi;
#endif
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
                            :: else ->
#if CONFIG_VERSIONS
                                if
                                :: root[n] != value -> cfg_root_id_changed(n)
                                :: else -> skip
                                fi;
#endif
                                root[n] = value
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
#if CONFIG_VERSIONS
        data = 0;
#endif
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

#if CONFIG_VERSIONS
#if CFG_ASYNC_READ
proctype CfgReader(byte n; byte owner) {
    byte captured_key;
#if CFG_USE_LOCAL_COOKIE
    byte captured_local;
#endif
    do
    :: atomic {
        ONLINE(n) && len(cfg_reads[CFG_IDX(n, owner)]) ->
#if CFG_USE_LOCAL_COOKIE
        cfg_reads[CFG_IDX(n, owner)]?captured_key,captured_local;
#else
        cfg_reads[CFG_IDX(n, owner)]?captured_key;
#endif
        assert(len(inbox[n]) < QUEUE_CAPACITY);
        inbox[n]!CfgGather,n,0,0,captured_key,(CFG_LOCAL_LOADED | CFG_ONE(n, cfg_c[n], cfg_p[n]) | CFG_CALLBACK_TAG(captured_local));
#if CFG_USE_LOCAL_COOKIE
        captured_local = 0;
#endif
        captured_key = 0
    }
    od
}
#endif
/* Configuration versions: processes. */
#if CFG_WRITE_MODE > 0
proctype CfgDisk(byte n) {
    byte saved_c; byte saved_p; byte saved_key; byte saved_body;
#if CFG_USE_LOCAL_COOKIE
    byte saved_local;
#endif
    do
    :: atomic {
        ONLINE(n) && len(cfg_writes[n]) && CFG_TARGET_DISK(n) ->
#if CFG_USE_LOCAL_COOKIE
        cfg_writes[n]?saved_c,saved_p,saved_key,saved_body,saved_local;
#else
        cfg_writes[n]?saved_c,saved_p,saved_key,saved_body;
#endif
        assert(cfg_generation[saved_c] >= cfg_generation[cfg_c[n]]);
        cfg_c[n] = saved_c; cfg_p[n] = saved_p;
        cfg_commit_observer();
        if
        :: saved_key ->
            /* Durability and the keeper callback are separate transitions.
             * The busy proposal remains occupied until its callback arrives. */
            assert(len(inbox[n]) < QUEUE_CAPACITY);
            inbox[n]!CfgVote,n,0,0,saved_key,(CFG_LOCAL_STORED | saved_body | CFG_CALLBACK_TAG(saved_local))
        :: else -> skip
        fi;
#if CFG_USE_LOCAL_COOKIE
        saved_local = 0;
#endif
        saved_c = 0; saved_p = 0; saved_key = 0; saved_body = 0; cfg_clear_scratch()
    }
    od
}
#else
proctype CfgDisk(byte n) {
    byte saved_c; byte saved_p;
    do
    :: atomic {
        ONLINE(n) && len(cfg_writes[n]) && CFG_TARGET_DISK(n) ->
        cfg_writes[n]?saved_c,saved_p;
        assert(cfg_generation[saved_c] >= cfg_generation[cfg_c[n]]);
        cfg_c[n] = saved_c; cfg_p[n] = saved_p;
        saved_c = 0; saved_p = 0
    }
    od
}
#endif
#endif

#if CONFIG_VERSIONS
#if CFG_WRITE_MODE > 0
#if CFG_API_TRANSPORT
#if !CFG_TARGET_SAMEGEN
proctype CfgAdmin() {
    atomic { cfg_client_sent[2] = true; cfg_last_admin = 2; cfg_api_issue(2, CFG_API_REPLACE); cfg_clear_scratch() };
#if CFG_WRITE_MODE >= 2
    atomic { cfg_client_sent[3] = true; cfg_last_admin = 3; cfg_api_issue(3, CFG_API_REPLACE); cfg_clear_scratch() };
#endif
#if CFG_WRITE_MODE >= 3
    atomic {
        cfg_client_done[2] && cfg_client_done[3] ->
        /* Ordinary admin workflow: ask the reachable root for current config,
         * derive YAML version+1 from its response, then issue Z via the tree. */
        cfg_client_sent[4] = true; cfg_last_admin = 4;
        cfg_api_issue(4, CFG_API_GET_VERSION); cfg_clear_scratch()
    };
#endif
}
#endif
proctype CfgApiTimer(byte body) {
    do
    :: atomic {
        cfg_api_deadline_key[body] && len(inbox[cfg_api_endpoint[body]]) < QUEUE_CAPACITY ->
        inbox[cfg_api_endpoint[body]]!CfgVote,cfg_api_endpoint[body],0,0,cfg_api_deadline_key[body],(CFG_API_ENDPOINT | CFG_API_REPLY | CFG_API_DEADLINE | body);
        cfg_api_deadline_key[body] = 0
    }
    od
}
#else
proctype CfgAdmin() {
    byte admin_body;
    /* Endpoints are local API inputs; requests traverse actual parents. Root
     * selection is never made by consulting SCEPTERS or a global root id. */
    atomic {
        admin_body = 2; cfg_client_sent[2] = true; cfg_last_admin = 2;
        assert(len(inbox[CFG_CLIENT_ENDPOINT]) < QUEUE_CAPACITY);
        inbox[CFG_CLIENT_ENDPOINT]!CfgPropose,CFG_CLIENT_ENDPOINT,0,0,0,(CFG_CLIENT_REQUEST | 2);
        admin_body = 0
    };
#if CFG_WRITE_MODE >= 2
    atomic {
        cfg_client_sent[3] = true; cfg_last_admin = 3;
        assert(len(inbox[3]) < QUEUE_CAPACITY);
        inbox[3]!CfgPropose,3,0,0,0,(CFG_CLIENT_REQUEST | 3)
    };
#endif
#if CFG_WRITE_MODE >= 3
    atomic {
        cfg_client_done[2] && cfg_client_done[3] ->
        cfg_client_sent[4] = true; cfg_last_admin = 4;
        assert(len(inbox[CFG_CLIENT_ENDPOINT]) < QUEUE_CAPACITY);
        inbox[CFG_CLIENT_ENDPOINT]!CfgPropose,CFG_CLIENT_ENDPOINT,0,0,0,(CFG_CLIENT_REQUEST | 4)
    };
#endif
}
#endif
#endif
proctype CfgRefillTimer(byte n) {
    do
    :: atomic {
        cfg_refill_timeout_key[n] && len(inbox[n]) < QUEUE_CAPACITY ->
        inbox[n]!CfgFreshTimeout,n,0,0,cfg_refill_timeout_key[n],0;
        cfg_refill_timeout_key[n] = 0
    }
    od
}
#endif

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

#if CFG_TARGET_SAMEGEN
proctype Network() { false }

proctype CfgAdmin() {
    /* Targeted positive control. All delays below are a finite environment prefix. */
    atomic {
        cfg_client_sent[2] = true; cfg_last_admin = 2;
        cfg_api_issue(2, CFG_API_REPLACE); cfg_clear_scratch()
    };
    atomic {
        (cfg_generation[2] == 2 && !cfg_client_done[2] && cfg_prop_busy_key[2] && cfg_prop_busy_key[3]) || cfg_client_done[2] ->
        if
        :: cfg_generation[2] == 2 && !cfg_client_done[2] && cfg_prop_busy_key[2] && cfg_prop_busy_key[3] -> cut_link(1,2); cfg_target_stage = 1
        :: cfg_client_done[2] -> cfg_target_stage = 4
        fi
    };
    if
    :: cfg_target_stage == 1 ->
        atomic {
            (SCEPTER(2) && !binding[2] && binding[3] == 2) || cfg_client_done[2] ->
            if
            :: SCEPTER(2) && !binding[2] && binding[3] == 2 ->
                cfg_client_sent[3] = true; cfg_last_admin = 3;
                cfg_api_issue(3, CFG_API_REPLACE); cfg_clear_scratch();
                cfg_target_stage = 2
            :: !(SCEPTER(2) && !binding[2] && binding[3] == 2) && cfg_client_done[2] -> cfg_target_stage = 4
            fi
        };
        if
        :: cfg_target_stage == 2 ->
            atomic {
                (cfg_request[2] == 3 && cfg_phase[2] == CFG_PHASE_PREFLIGHT && cfg_result_ready[2]) || cfg_client_done[3] ->
                if
                :: cfg_request[2] == 3 && cfg_phase[2] == CFG_PHASE_PREFLIGHT && cfg_result_ready[2] -> cfg_target_stage = 3
                :: !(cfg_request[2] == 3 && cfg_phase[2] == CFG_PHASE_PREFLIGHT && cfg_result_ready[2]) && cfg_client_done[3] -> cfg_target_stage = 4
                fi
            };
            if
            :: cfg_target_stage == 3 ->
                atomic { cfg_generation[3] || cfg_client_done[3] -> cfg_target_stage = 4 }
            :: else -> skip
            fi
        :: else -> skip
        fi
    :: else -> skip
    fi;
    atomic { cfg_target_stage = 4; links = 7; network_stable = true; faults_left = 0 };
    if
    :: !cfg_client_sent[3] ->
        atomic {
            (binding[3] && !awaiting[3]) || SCEPTER(3) ->
            cfg_client_sent[3] = true; cfg_last_admin = 3;
            cfg_api_issue(3, CFG_API_REPLACE); cfg_clear_scratch()
        }
    :: else -> skip
    fi;
#if CFG_WRITE_MODE >= 3
    atomic {
        cfg_client_done[2] && cfg_client_done[3] && ((binding[3] && !awaiting[3]) || SCEPTER(3)) ->
        cfg_client_sent[4] = true; cfg_last_admin = 4;
        cfg_api_issue(4, CFG_API_GET_VERSION); cfg_clear_scratch()
    }
#endif
}
#else
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
#endif

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

#if CONFIG_VERSIONS && CFG_GOAL_CLOSURE
/* Read-only observer: an exhaustive safety search can schedule its first
 * step at any occurrence of the goal and detect any later departure. */
proctype CfgGoalClosure() {
    (network_stable && CONVERGED && cfg_converged);
    do
    :: assert(network_stable && CONVERGED && cfg_converged)
    od
}
#endif


init {
    byte n; byte peer;
    byte bootstrap_root;
    atomic {
#if CONFIG_VERSIONS
        cfg_init();
#endif
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
#elif INITIAL_TREE == 3
        initialize_binding(2, 3);
        initialize_binding(1, 2);
        child_nodes[SLOT(1, 2)] = BIT(2) | BIT(3);
        subtree[1] = 7;
        root[3] = 1;
        last_root[SLOT(2, 3)] = 1;
#endif
#if CONFIG_VERSIONS
        for (n : 1 .. 3) {
            for (peer : 1 .. 3) {
                if
                :: peer != n && child_cookie[SLOT(n, peer)] ->
                    cfg_report = child_nodes[SLOT(n, peer)];
                    for (cfg_a : 1 .. 3) {
                        if
                        :: cfg_report & BIT(cfg_a) -> cfg_report = cfg_report | (cfg_applied[cfg_a] << (3 + 3 * (cfg_a - 1)))
                        :: else -> skip
                        fi
                    };
                    cfg_metadata_input(n, peer, child_nodes[SLOT(n, peer)], cfg_report, 7)
                :: else -> skip
                fi
            }
        };
        cfg_clear_scratch();
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
#if CONFIG_VERSIONS
        run CfgDisk(1); run CfgDisk(2); run CfgDisk(3);
#if CFG_ASYNC_READ
        run CfgReader(1, 1); run CfgReader(1, 2); run CfgReader(1, 3);
        run CfgReader(2, 1); run CfgReader(2, 2); run CfgReader(2, 3);
        run CfgReader(3, 1); run CfgReader(3, 2); run CfgReader(3, 3);
#endif
#if CFG_WRITE_MODE > 0
        run CfgAdmin();
#if CFG_API_TRANSPORT && CFG_API_TIMEOUT_MASK > 0
        run CfgApiTimer(2); run CfgApiTimer(3); run CfgApiTimer(4);
#endif
#endif
#if CFG_RPC_TIMEOUT_BUDGET > 0
        run CfgRefillTimer(1); run CfgRefillTimer(2); run CfgRefillTimer(3);
#endif
#endif
#if OP_MASK
        run Operations(1); run Operations(2); run Operations(3);
#endif
#if CONFIG_VERSIONS && CFG_GOAL_CLOSURE
        run Network(); run CfgGoalClosure()
#else
        run Network()
#endif
    }
    do
    :: timeout -> assert(network_stable && CONVERGED
#if CONFIG_VERSIONS
                         && cfg_converged
#endif
                         )
    od
}

#ifndef NO_LTL
#if CONFIG_VERSIONS
#if CFG_GOAL_REACHABILITY
ltl config_convergence { <> (network_stable && CONVERGED && cfg_converged) }
#else
ltl config_convergence { <> [] (network_stable && CONVERGED && cfg_converged) }
#endif
#endif
#if COMMITTED_MASK
ltl convergence { <> [] (network_stable && CONVERGED) }
#else
ltl convergence { <> [] (network_stable && tree_converged) }
#endif
ltl role_exclusion { [] ((!SCEPTER(1) || !binding[1]) && (!SCEPTER(2) || !binding[2]) && (!SCEPTER(3) || !binding[3])) }
#endif
