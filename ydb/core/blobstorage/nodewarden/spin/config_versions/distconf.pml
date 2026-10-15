/*
 * Bounded model of the compatibility-friendly configuration protocol.
 * ALL_OLD selects the old-node reference; mixed nodes retain their old rules.
 * C++ port validation is reported separately.
 *
 * Fixed N=3/Q=2, nonzero BASE, one logical disk per node, direct publications
 * and selected two-hop refill RPC paths through an existing parent.
 * Writes, ACK delivery, commit delivery, disk reads and read replies are
 * separate transitions. Disk writes capture BOTH metadata fields, FIFO.
 * Root cancellation fences callbacks, without deleting replica I/O/commits.
 * Prefixes are explicit schedules; cases 11 and 19-20 vary bounded cuts.
 * Case 24 also delays applied metadata through reconnects and formatting.
 * Case 25 admits an unsolicited stale body as an explicit overapproximation.
 * After the prefix, disk/network workers and the coordinator run concurrently
 * under SPIN weak process fairness. No further failures or admin requests.
 * No bootstrap, bridge, changing membership, or full actor-tree refinement.
 */
#ifndef SCENARIO
#define SCENARIO 1
#endif
#ifndef CHECK_SAFETY
#define CHECK_SAFETY 1
#endif
#ifndef ALL_OLD
#define ALL_OLD 0
#endif
#ifndef LEGACY_REPAIR
#define LEGACY_REPAIR 1
#endif
#ifndef LEGACY_CLOSURE
#define LEGACY_CLOSURE 0
#endif
#ifndef LEGACY_REACH
#define LEGACY_REACH 0
#endif
#define LEGACY_CASE (SCENARIO >= 30 && SCENARIO <= 36)
#define N 3
#define Q 2
#define A 0
#define B 1
#define C 2
#define NONE 0
#define BASE 1
#define X 2
#define Y 3
#define Z 4
#define MAX_GEN 5
#define GEN(v) generation[v]
#if ALL_OLD
#define REFILL_TRIGGER(node) true
#else
#define REFILL_TRIGGER(node) (ordinary_body[node] != NONE && GEN(ordinary_body[node]) != 0)
#endif

byte generation[5];
byte disk_c[N];
byte disk_p[N];
byte memory_c[N];
byte memory_p[N];
byte applied[N];
bool online[N];
bool upgraded[N];
bool proposal_busy[N];
bool fatal[N];
byte known[N];
bool feedback[N];
/* Prefix-only delayed metadata delivery; the stable suffix delivers feedback. */
bool defer_feedback[N];

/* Complete metadata snapshots: committed, proposed, ACK value, ACK cookie. */
chan writes[N] = [3] of { byte, byte, byte, byte };
chan commits[N] = [2] of { byte };
chan acks = [6] of { byte, byte, byte };

/* A QueryConfig request/reply travels through the existing parent path. The
 * request cookie is local to a binding/incarnation; no durable epoch exists. */
byte binding_cookie[N];
bool refill_needed[N];
bool legacy_refill[N];
byte ordinary_body[N];
bool query_inflight[N];
byte query_stage[N];
byte query_cookie[N];
byte query_root_cookie[N];
byte query_via[N];
bool query_await_read[N];
chan query_reply_hops[N] = [3] of { byte, byte, bool };
chan query_replies[N] = [3] of { byte, byte, bool };
bool scratch_fresh;

byte root;
bool root_new = true;
byte root_epoch = 1;
byte operation;
byte op_cookie = 1;
byte votes;
bool voted[N];
bool learned;
bool replied;
bool retry_armed;
byte retry_epoch;
/* Volatile generation/fingerprint metadata, scoped to the current root cookie.
 * This is not a configuration body and cannot be published without a disk read. */
byte applied_floor;
bool applied_conflict;
bool repair_pending;
bool repair_restart;

bool read_requested[N];
bool read_pending[N];
bool received[N];
byte read_c[N];
byte read_p[N];
byte read_owner[N];
bool collecting;
bool recovery_needed;

/* Observer state never selects values or participates in quorums. */
byte ok_value[MAX_GEN + 1];
byte last_admin;
byte expected;
byte ok_count;
byte stale_acks;
byte violation;
byte max_bad;
byte observer_node;
byte observer_copies;
bool suffix;
bool done;
byte recovered;
bool blocked;
bool accepted;
byte observed_max;
#if SCENARIO == 19 || SCENARIO == 20
bool first_published;
bool same_generation_attempts;
byte fork_left;
byte fork_right;
#endif

#if LEGACY_CASE
bool legacy_granted;
bool legacy_attempted[N];
bool legacy_request_done;
#if LEGACY_CLOSURE
bool legacy_done_seen;
#endif
byte legacy_epoch;
byte legacy_fence;
byte legacy_saved_fence;
byte legacy_max;
byte legacy_top;
/* When false, the original coordinator turn only recomputes unchanged
 * observer state and clears dead scratch. Every useful trigger below stays
 * enabled until the coordinator consumes it. */
#define legacy_goal (suffix && legacy_request_done && disk_c[A] != NONE && disk_c[A] == disk_c[B] && disk_c[A] == disk_c[C] && !fatal[A] && !fatal[B] && !fatal[C] && (!expected || disk_c[A] == expected))
#define legacy_all_received (received[A] && received[B] && received[C])
#define legacy_retry_node(node) ((node) != root && online[node] && GEN(known[node]) < GEN(memory_c[root]) && len(commits[node]) < 2)
#define legacy_can_retry (root_new && retry_armed && retry_epoch == root_epoch && !applied_conflict && GEN(memory_c[root]) >= GEN(applied_floor) && !repair_pending && (legacy_retry_node(A) || legacy_retry_node(B) || legacy_retry_node(C)))
#define legacy_coordinator_enabled (operation == NONE || done || feedback[A] || feedback[B] || feedback[C] || len(acks) || (operation == Z && !learned && votes >= Q) || (collecting && legacy_all_received) || legacy_can_retry || done != legacy_goal)
#endif

byte i;
byte j;
byte n;
byte count;
byte stateful_count;
byte max_committed;
byte chosen;
byte copies[5];
byte committed_copies[5];
byte scratch_c;
byte scratch_p;
byte scratch_v;
byte scratch_cookie;
bool all_read;
bool agreement;
byte common;

inline require(condition, code) {
    if
    :: !(condition) -> violation = code;
#if CHECK_SAFETY
        assert(condition)
#else
        skip
#endif
    :: else -> skip
    fi
}

inline check_budget() {
    count = 0; j = 0;
    do
    :: j < N ->
        if :: !online[j] || (disk_c[j] == NONE && disk_p[j] == NONE) -> count++ :: else -> skip fi;
        j++
    :: else -> break
    od;
    if :: count > max_bad -> max_bad = count :: else -> skip fi;
    assert(count <= N - Q)
}

inline observe_committed_quorum(value) {
    if
    :: value != NONE && value == last_admin ->
        observer_copies = 0; observer_node = 0;
        do
        :: observer_node < N ->
            if :: disk_c[observer_node] == value -> observer_copies++ :: else -> skip fi;
            observer_node++
        :: else -> break
        od;
        if :: observer_copies >= Q -> expected = value :: else -> skip fi
    :: else -> skip
    fi
}

inline observe_committed_fork() {
#if SCENARIO == 19 || SCENARIO == 20
    /* Reachability diagnostic, stronger than the requested final agreement. */
    fork_left = 0;
    do
    :: fork_left < N ->
        fork_right = fork_left + 1;
        do
        :: fork_right < N ->
            require(disk_c[fork_left] == NONE || disk_c[fork_right] == NONE
                    || disk_c[fork_left] == disk_c[fork_right]
                    || GEN(disk_c[fork_left]) != GEN(disk_c[fork_right]), 4);
            fork_right++
        :: else -> break
        od;
        fork_left++
    :: else -> break
    od
#else
    skip
#endif
}

inline read_one(node) {
    assert(online[node]);
    read_c[node] = disk_c[node]; read_p[node] = disk_p[node];
    read_owner[node] = op_cookie; read_requested[node] = false; read_pending[node] = true
}

inline deliver_read(node) {
    assert(read_pending[node]);
    if :: read_owner[node] == op_cookie -> received[node] = true :: else -> skip fi;
    read_pending[node] = false
}

inline begin_collect() {
    i = 0;
    do
    :: i < N -> received[i] = false; read_pending[i] = false; read_requested[i] = online[i]; i++
    :: else -> break
    od;
    collecting = true
}

inline collect_mask(mask) {
    begin_collect(); i = 0;
    do
    :: i < N ->
        if
        :: online[i] && ((mask) & (1 << i)) -> read_one(i); deliver_read(i)
        :: else -> read_requested[i] = false
        fi;
        i++
    :: else -> break
    od;
    collecting = false
}

inline count_reads() {
    i = 0;
    do :: i < 5 -> copies[i] = 0; committed_copies[i] = 0; i++ :: else -> break od;
    count = 0; stateful_count = 0; max_committed = 0; observed_max = 0; i = 0;
    do
    :: i < N ->
        if
        :: received[i] ->
            count++;
            if :: read_c[i] != NONE || read_p[i] != NONE -> stateful_count++ :: else -> skip fi;
            if
            :: read_c[i] != NONE -> copies[read_c[i]]++; committed_copies[read_c[i]]++
            :: else -> skip
            fi;
            if :: read_p[i] != NONE && read_p[i] != read_c[i] -> copies[read_p[i]]++ :: else -> skip fi;
            if :: GEN(read_c[i]) > max_committed -> max_committed = GEN(read_c[i]) :: else -> skip fi;
            if :: GEN(read_c[i]) > observed_max -> observed_max = GEN(read_c[i]) :: else -> skip fi;
            if :: GEN(read_p[i]) > observed_max -> observed_max = GEN(read_p[i]) :: else -> skip fi
        :: else -> skip
        fi;
        i++
    :: else -> break
    od
}

inline remember_applied(value) {
#if LEGACY_CASE
    if
    :: ((GEN(value) > GEN(applied_floor)
         || (value != NONE && GEN(value) == GEN(applied_floor) && value != applied_floor && !applied_conflict))
        && (GEN(value) > GEN(applied[root])
            || (GEN(value) == GEN(applied[root]) && value != applied[root]))) ->
        assert(legacy_fence < 63); legacy_fence++;
        recovery_needed = true
    :: else -> skip
    fi;
#endif
#if ALL_OLD
    skip
#else
    if
    :: root_new && GEN(value) > GEN(applied_floor) -> applied_floor = value
    :: root_new && value != NONE && GEN(value) == GEN(applied_floor) && value != applied_floor ->
        applied_conflict = true
    :: else -> skip
    fi
#endif
}

inline offer_commit(node, value) {
    if :: online[node] && len(commits[node]) < 2 -> commits[node]!value :: else -> skip fi
}

inline install(node, value) {
    if
    :: GEN(value) > GEN(applied[node]) ->
        applied[node] = value; memory_c[node] = value;
        assert(len(writes[node]) < 3);
        writes[node]!memory_c[node],memory_p[node],NONE,0;
        feedback[node] = true
    :: GEN(value) == GEN(applied[node]) && value != applied[node] ->
        fatal[node] = true; require(false, 2)
    :: else -> skip
    fi
}

inline fanout(value) {
#if LEGACY_CASE
    assert(GEN(value) > 2);
#endif
#if !ALL_OLD
    assert(!root_new || (!applied_conflict && GEN(value) >= GEN(applied_floor)));
#endif
    i = 0;
    do
    :: i < N -> if :: i != root -> offer_commit(i, value) :: else -> skip fi; i++
    :: else -> break
    od;
    retry_armed = true; retry_epoch = root_epoch
}

inline apply_publication(value) {
    /* C++ fans out before applying the root's own committed record. */
    fanout(value); install(root, value)
}

inline persist_one(node) {
    assert(online[node] && len(writes[node]) != 0);
    writes[node]?scratch_c,scratch_p,scratch_v,scratch_cookie;
    assert(GEN(scratch_c) >= GEN(disk_c[node]));
    disk_c[node] = scratch_c; disk_p[node] = scratch_p;
    observe_committed_quorum(scratch_c);
    observe_committed_fork();
    if
    :: scratch_v != NONE -> proposal_busy[node] = false; acks!node,scratch_v,scratch_cookie
    :: else -> skip
    fi
}

inline drain_disk(node) {
    do :: len(writes[node]) != 0 -> persist_one(node) :: else -> break od
}

inline deliver_commit(node) {
    assert(online[node] && len(commits[node]) != 0);
    commits[node]?scratch_v;
#if ALL_OLD
    install(node, scratch_v)
#else
    if
    :: upgraded[node] && refill_needed[node] && !legacy_refill[node] && node != root ->
        /* A current-binding ordinary publication is held until capability is
         * known. The QueryConfig response body is never a legacy publication. */
        if :: GEN(scratch_v) > GEN(ordinary_body[node]) -> ordinary_body[node] = scratch_v :: else -> skip fi
    :: else -> install(node, scratch_v); refill_needed[node] = false
    fi
#endif
}

inline complete_commit(node) {
    do :: len(commits[node]) != 0 -> deliver_commit(node); drain_disk(node) :: else -> break od;
    drain_disk(node)
}

inline lose_commits(node) {
    do :: len(commits[node]) != 0 -> commits[node]?scratch_v :: else -> break od
}

inline deliver_ack() {
    acks?n,scratch_v,scratch_cookie;
    if
    :: scratch_cookie == op_cookie && scratch_v == operation && operation != NONE && !voted[n] ->
        require(disk_c[n] != NONE || disk_p[n] != NONE, 3);
        voted[n] = true; votes++
    :: else -> stale_acks++
    fi
}

inline drain_acks() {
    do :: len(acks) != 0 -> deliver_ack() :: else -> break od
}

inline propose(node) {
    assert(online[node]); accepted = false;
    if
    :: !proposal_busy[node] && GEN(operation) > GEN(applied[node]) ->
#if ALL_OLD
        accepted = true
#else
        if
        :: upgraded[node] && disk_c[node] == NONE && disk_p[node] == NONE -> skip
        :: else -> accepted = true
        fi
#endif
    :: else -> skip
    fi;
    if
    :: accepted ->
        /* Current replica acceptance ignores an existing uncommitted proposed. */
        memory_p[node] = operation; proposal_busy[node] = true;
        assert(len(writes[node]) < 3);
        writes[node]!memory_c[node],memory_p[node],operation,op_cookie
    :: else -> skip
    fi
}

inline complete_vote(node) {
    propose(node);
    if :: accepted -> drain_disk(node); deliver_ack() :: else -> skip fi
}

inline start_write(value) {
    assert(value > BASE); last_admin = value; expected = NONE;
    operation = value; op_cookie++; votes = 0; learned = false; replied = false;
    i = 0;
    do :: i < N -> voted[i] = false; i++ :: else -> break od;
#if ALL_OLD
    generation[value] = GEN(applied[root]) + 1
#else
    if
    :: root_new ->
        count_reads(); generation[value] = observed_max + 1;
        if :: GEN(applied[root]) >= generation[value] -> generation[value] = GEN(applied[root]) + 1 :: else -> skip fi;
        if :: GEN(applied_floor) >= generation[value] -> generation[value] = GEN(applied_floor) + 1 :: else -> skip fi
    :: else -> generation[value] = GEN(applied[root]) + 1
    fi
#endif
    assert(generation[value] <= MAX_GEN)
}

inline learn() {
#if LEGACY_CASE
    assert(votes >= Q && legacy_granted);
    assert(legacy_epoch == root_epoch && legacy_saved_fence == legacy_fence);
    assert(GEN(operation) > legacy_max && GEN(operation) > GEN(applied_floor));
    legacy_granted = false;
    recovery_needed = false; applied_conflict = false; applied_floor = operation;
#endif
    assert(votes >= Q); learned = true; apply_publication(operation)
}

inline reply_ok() {
    assert(operation != NONE && learned && !replied);
    if :: root_new -> count_reads(); assert(committed_copies[operation] >= Q) :: else -> assert(votes >= Q) fi;
    require(ok_value[GEN(operation)] == NONE || ok_value[GEN(operation)] == operation, 1);
    ok_value[GEN(operation)] = operation; ok_count++;
    if :: last_admin == operation -> expected = operation :: else -> skip fi;
    replied = true
}

inline abandon_request() {
    operation = NONE; op_cookie++; collecting = false
}

inline change_root(node, is_new) {
    assert(online[node] && disk_c[node] != NONE);
    root = node; root_new = is_new && !ALL_OLD; root_epoch++; retry_armed = false;
    abandon_request(); i = 0;
    recovery_needed = true;
    applied_floor = NONE; applied_conflict = false;
    repair_pending = false;
    do
    :: i < N -> known[i] = applied[i]; feedback[i] = false;
        remember_applied(known[i]);
        binding_cookie[i]++; query_inflight[i] = false;
        legacy_refill[i] = false; ordinary_body[i] = NONE;
        query_stage[i] = 0; query_await_read[i] = false; query_via[i] = node;
        read_requested[i] = false; read_pending[i] = false; received[i] = false; i++
    :: else -> break
    od
}

inline start_refill_query(node) {
    assert(online[node] && refill_needed[node] && !query_inflight[node] && node != root);
    assert(REFILL_TRIGGER(node));
    query_inflight[node] = true; query_cookie[node] = binding_cookie[node];
    query_root_cookie[node] = root_epoch; query_stage[node] = 1
}

inline forward_refill_query(node) {
    assert(query_stage[node] == 1 || query_stage[node] == 2);
    assert(online[node] && online[query_via[node]]);
    query_stage[node]++
}

inline send_refill_reply(node, value, fresh) {
    assert(len(query_reply_hops[node]) < 3);
    query_reply_hops[node]!query_cookie[node],value,fresh;
    query_stage[node] = 0; query_await_read[node] = false
}

inline forward_refill_reply(node) {
    assert(online[query_via[node]] && len(query_reply_hops[node]) != 0);
    query_reply_hops[node]?scratch_cookie,scratch_v,scratch_fresh;
    assert(len(query_replies[node]) < 3);
    query_replies[node]!scratch_cookie,scratch_v,scratch_fresh
}

inline deliver_refill_reply(node) {
    assert(online[node] && len(query_replies[node]) != 0);
    query_replies[node]?scratch_cookie,scratch_v,scratch_fresh;
    if
    :: refill_needed[node] && scratch_cookie == binding_cookie[node] ->
        query_inflight[node] = false;
#if ALL_OLD
        refill_needed[node] = false; install(node, scratch_v)
#else
        if
        :: scratch_fresh -> refill_needed[node] = false; ordinary_body[node] = NONE; install(node, scratch_v)
        :: else ->
            /* Ignore the queried local body. Only a received ordinary
             * publication can resume the existing legacy behavior. */
            legacy_refill[node] = true;
            if
            :: ordinary_body[node] != NONE ->
                install(node, ordinary_body[node]); ordinary_body[node] = NONE; refill_needed[node] = false
            :: else -> skip
            fi
        fi
#endif
    :: else -> skip
    fi
}

inline accept_refill_queries() {
#if ALL_OLD
    skip
#else
    i = 0;
    do
    :: i < N ->
        if
        :: query_stage[i] == 3 ->
            if
            :: query_root_cookie[i] != root_epoch -> query_stage[i] = 0; query_inflight[i] = false
            :: root_new ->
                query_stage[i] = 4; query_await_read[i] = true;
                repair_pending = true; recovery_needed = true; begin_collect()
            :: else -> send_refill_reply(i, memory_c[root], false)
            fi
        :: else -> skip
        fi;
        i++
    :: else -> break
    od
#endif
}

inline answer_refill_queries(value) {
#if ALL_OLD
    skip
#else
    i = 0;
    do
    :: i < N ->
        if
        :: query_await_read[i] && query_root_cookie[i] == root_epoch -> send_refill_reply(i, value, true)
        :: else -> skip
        fi;
        i++
    :: else -> break
    od
#endif
}

inline select_recovery() {
    count_reads(); chosen = NONE; blocked = count < Q;
    i = 1;
    do
    :: i < 5 -> if :: copies[i] >= Q && GEN(i) > GEN(chosen) -> chosen = i :: else -> skip fi; i++
    :: else -> break
    od;
#if !ALL_OLD
    if
    :: root_new ->
        blocked = stateful_count < Q;
        /* Retain quorum-backed proposed recovery; add minority committed. */
        i = 1;
        do
        :: i < 5 ->
            if :: committed_copies[i] != 0 && GEN(i) > GEN(chosen) -> chosen = i :: else -> skip fi;
            i++
        :: else -> break
        od
    :: else -> skip
    fi
#endif
#if !ALL_OLD
    if
    :: root_new ->
        /* A read body matching already published metadata can survive solely
         * as proposed. Metadata alone never supplies a body for recovery. */
        if :: copies[applied_floor] != 0 && GEN(applied_floor) > GEN(chosen) -> chosen = applied_floor :: else -> skip fi;
        if :: applied_conflict || GEN(chosen) < GEN(applied_floor) -> blocked = true :: else -> skip fi
    :: else -> skip
    fi;
#endif
    if :: chosen == NONE || GEN(chosen) < max_committed -> blocked = true :: else -> skip fi;
    /* The current patch rejects two quorum-backed fingerprints at the maximum.
     * Candidates also reject conflicting committed copies at that generation. */
    i = 1;
    do
    :: i < 5 ->
        if
        :: chosen != NONE && i != chosen && GEN(i) == GEN(chosen) ->
            if
            :: root_new && copies[i] >= Q -> blocked = true
#if !ALL_OLD
            :: root_new && committed_copies[i] != 0 -> blocked = true
#endif
            :: else -> skip
            fi
        :: else -> skip
        fi;
        i++
    :: else -> break
    od;
    if
    :: !blocked -> recovered = chosen; recovery_needed = false; repair_pending = false;
        apply_publication(chosen); answer_refill_queries(chosen)
    :: else -> recovered = NONE
    fi
}

inline stop_node(node) {
    online[node] = false; applied[node] = NONE;
    do :: len(writes[node]) != 0 -> writes[node]?scratch_c,scratch_p,scratch_v,scratch_cookie :: else -> break od;
    lose_commits(node);
    proposal_busy[node] = false; check_budget()
}

inline restart_node(node) {
    online[node] = true; applied[node] = disk_c[node];
    memory_c[node] = disk_c[node]; memory_p[node] = disk_p[node]; feedback[node] = true;
    binding_cookie[node]++; query_inflight[node] = false;
    legacy_refill[node] = false; ordinary_body[node] = NONE;
    query_stage[node] = 0; query_await_read[node] = false;
    refill_needed[node] = disk_c[node] == NONE && disk_p[node] == NONE;
    check_budget()
}

inline format_node(node) {
    stop_node(node); disk_c[node] = NONE; disk_p[node] = NONE; restart_node(node)
}

inline check_done() {
    agreement = true; common = NONE; i = 0;
    do
    :: i < N ->
        if
        :: online[i] ->
            if :: common == NONE -> common = disk_c[i] :: else -> skip fi;
            if :: disk_c[i] == NONE || disk_c[i] != common || fatal[i] -> agreement = false :: else -> skip fi;
        :: else -> skip
        fi;
        i++
    :: else -> break
    od;
    if :: expected != NONE && common != expected -> agreement = false :: else -> skip fi;
#if LEGACY_CASE
    done = suffix && agreement && legacy_request_done;
#if LEGACY_CLOSURE
    /* done changes only in this helper; an absorbing goal plus fair
     * reachability proves the original eventual permanent convergence. */
    assert(!legacy_done_seen || done);
    if :: done -> legacy_done_seen = true :: else -> skip fi
#endif
#else
    done = suffix && agreement
#endif
}

inline retry_committed() {
    if
#if ALL_OLD
    :: root_new && retry_armed && retry_epoch == root_epoch ->
#else
    :: root_new && retry_armed && retry_epoch == root_epoch && !applied_conflict && GEN(memory_c[root]) >= GEN(applied_floor) && !repair_pending ->
#endif
        i = 0;
        do
        :: i < N ->
            if :: i != root && GEN(known[i]) < GEN(memory_c[root]) -> offer_commit(i, memory_c[root]) :: else -> skip fi;
            i++
        :: else -> break
        od
    :: else -> skip
    fi
}

#if SCENARIO == 19 || SCENARIO == 20
inline optional_disk_completion(node) {
    if
    :: online[node] && len(writes[node]) != 0 -> persist_one(node)
    :: skip
    fi
}

inline commit_cut(node) {
    if
    :: online[node] ->
        if
        :: len(commits[node]) != 0 -> deliver_commit(node)
        :: complete_commit(node)
        :: skip
        fi
    :: else -> skip
    fi
}

inline read_with_disk_cut(node) {
    if
    :: online[node] ->
        optional_disk_completion(node); read_one(node);
        optional_disk_completion(node); deliver_read(node)
    :: else -> skip
    fi
}

inline vote_after_disk_cut(node) {
    if
    :: online[node] ->
        optional_disk_completion(node); propose(node);
        if :: accepted -> drain_disk(node) :: else -> skip fi
    :: else -> skip
    fi
}

inline candidate_race_prefix() {
    assert(!ALL_OLD && root_new);
    start_write(X); propose(A); propose(B);
    if
    :: first_published = true;
        drain_disk(A); drain_disk(B); drain_acks(); learn();
        commit_cut(A); commit_cut(B); commit_cut(C);
        collect_mask(7); count_reads();
        if
        :: committed_copies[X] >= Q -> if :: reply_ok() :: skip fi
        :: else -> skip
        fi;
        stop_node(A); change_root(C, true)
    :: first_published = false;
        /* Cancel before quorum ACKs; accepted replica IO remains pending. */
        change_root(C, true);
        if :: stop_node(B) :: skip fi
    fi;

    /* Each snapshot may precede or follow a queued metadata write. */
    begin_collect(); read_with_disk_cut(A); read_with_disk_cut(B); read_with_disk_cut(C);
    collecting = false; select_recovery(); require(!blocked, 5);
    start_write(Y); same_generation_attempts = GEN(X) == GEN(Y);
    if :: !online[B] -> restart_node(B) :: else -> skip fi;
    vote_after_disk_cut(A); vote_after_disk_cut(B); vote_after_disk_cut(C); drain_acks();
    if
    :: votes >= Q ->
        learn(); commit_cut(A); commit_cut(B); commit_cut(C);
        collect_mask(7); count_reads();
        if
        :: committed_copies[Y] >= Q -> if :: reply_ok() :: skip fi
        :: else -> skip
        fi
    :: else -> skip
    fi;
    if :: !online[A] -> restart_node(A) :: else -> skip fi;
    abandon_request(); check_budget();
    printf("RACE X=%d Y=%d same=%d published_X=%d OKs=%d max_bad=%d\n",
           GEN(X), GEN(Y), same_generation_attempts, first_published, ok_count, max_bad)
}
#endif

#if LEGACY_CASE
/* Explicit Replace may advance a stable legacy same-generation fork. It never
 * selects or publishes one of the disputed old bodies. Automatic recovery and
 * fresh QueryConfig retain select_recovery's conflict rejection. */
inline prepare_legacy_replace() {
    count_reads(); legacy_top = NONE; legacy_max = observed_max;
    if :: GEN(applied[root]) > legacy_max -> legacy_max = GEN(applied[root]) :: else -> skip fi;
    if :: GEN(applied_floor) > legacy_max -> legacy_max = GEN(applied_floor) :: else -> skip fi;
    i = 1;
    do
    :: i < 5 ->
        if
        :: (committed_copies[i] != 0 || copies[i] >= Q) && GEN(i) > GEN(legacy_top) -> legacy_top = i
        :: else -> skip
        fi;
        i++
    :: else -> break
    od;
    legacy_granted = false;
#if LEGACY_REPAIR
    if
    :: (root_new && stateful_count >= Q && copies[applied[root]] != 0
        && GEN(legacy_top) == GEN(applied[root]) && GEN(applied_floor) <= GEN(applied[root])) ->
        legacy_granted = true; legacy_epoch = root_epoch; legacy_saved_fence = legacy_fence
    :: else -> skip
    fi;
#endif
    if
    :: legacy_granted ->
        start_write(Z);
        assert(GEN(Z) > legacy_max && recovery_needed && applied_conflict)
    :: else -> legacy_request_done = true
    fi
}

/* These values have no suffix consumer across atomic transitions: each
 * helper initializes them before use. Reset preserves protocol states while
 * eliminating the history of which worker last used a scratch field. */
inline clear_legacy_scratch() {
    scratch_c = NONE; scratch_p = NONE; scratch_v = NONE; scratch_cookie = 0;
    n = 0; observer_node = 0; observer_copies = 0;
    i = 0; count = 0; stateful_count = 0; max_committed = 0; chosen = NONE;
    j = 0;
    do :: j < 5 -> copies[j] = 0; committed_copies[j] = 0; j++ :: else -> break od;
    j = 0; all_read = false; agreement = false; common = NONE; accepted = false
}

proctype LegacyProposer(byte node) {
    atomic {
        operation == Z ->
        /* Late proposal delivery may still enqueue IO before publication
         * reaches this replica; FIFO keeps it before that replica's commit. */
        legacy_attempted[node] = true; propose(node); clear_legacy_scratch()
    }
}

inline legacy_coordinator_turn() {
    if :: len(acks) != 0 -> deliver_ack() :: else -> skip fi;
    i = 0;
    do
    :: i < N ->
        if :: feedback[i] -> known[i] = applied[i]; feedback[i] = false :: else -> skip fi;
        remember_applied(known[i]); i++
    :: else -> break
    od;
    if
    :: operation == Z && !learned && votes >= Q ->
        learn(); begin_collect()
    :: collecting ->
        all_read = true; i = 0;
        do
        :: i < N -> if :: online[i] && !received[i] -> all_read = false :: else -> skip fi; i++
        :: else -> break
        od;
        if
        :: all_read ->
            collecting = false; count_reads();
            if
            :: committed_copies[Z] >= Q ->
#if SCENARIO == 33
                /* Caller timed out before the finite write. No delivered OK,
                 * while the server's accepted operation continues. */
                replied = true; legacy_request_done = true
#else
                reply_ok(); legacy_request_done = true
#endif
            :: else -> begin_collect()
            fi
        :: else -> skip
        fi
    :: else -> skip
    fi;
    retry_committed(); check_done()
}
#endif

proctype Disk(byte node) {
    do
#if LEGACY_CASE
    :: atomic { online[node] && !fatal[node] && len(writes[node]) != 0 -> persist_one(node); clear_legacy_scratch() }
#else
    :: atomic { online[node] && !fatal[node] && len(writes[node]) != 0 -> persist_one(node) }
#endif
    od
}

proctype Network(byte node) {
    do
#if LEGACY_CASE
    :: atomic { online[node] && !fatal[node] && len(commits[node]) != 0 && len(writes[node]) < 3 -> deliver_commit(node); clear_legacy_scratch() }
#else
    :: atomic { online[node] && !fatal[node] && len(commits[node]) != 0 && len(writes[node]) < 3 -> deliver_commit(node) }
#endif
    od
}

proctype Reader(byte node) {
    do
#if LEGACY_CASE
    :: atomic { online[node] && !fatal[node] && read_requested[node] -> read_one(node); clear_legacy_scratch() }
#else
    :: atomic { online[node] && !fatal[node] && read_requested[node] -> read_one(node) }
#endif
#if LEGACY_CASE
    :: atomic { read_pending[node] -> deliver_read(node); clear_legacy_scratch() }
#else
    :: atomic { read_pending[node] -> deliver_read(node) }
#endif
    od
}

proctype Refill(byte node) {
#if ALL_OLD
    skip
#else
    do
    :: atomic { online[node] && upgraded[node] && refill_needed[node] && !legacy_refill[node] && node != root && !query_inflight[node] && REFILL_TRIGGER(node) -> start_refill_query(node) }
    :: atomic { online[node] && online[query_via[node]] && (query_stage[node] == 1 || query_stage[node] == 2) -> forward_refill_query(node) }
    :: atomic { online[query_via[node]] && len(query_reply_hops[node]) != 0 && len(query_replies[node]) < 3 -> forward_refill_reply(node) }
    :: atomic { online[node] && len(query_replies[node]) != 0 && len(writes[node]) < 3 -> deliver_refill_reply(node) }
    od
#endif
}

inline coordinator_turn() {
    /* All actions run on each turn: fairness of the process therefore
     * cannot starve a particular retry branch in a nondeterministic loop. */
    if :: len(acks) != 0 -> deliver_ack() :: else -> skip fi;
    i = 0; repair_restart = false;
    do
    :: i < N ->
        if
        :: feedback[i] && upgraded[i] && !defer_feedback[i] ->
            known[i] = applied[i]; feedback[i] = false;
#if !ALL_OLD
            if :: root_new && known[i] == NONE && online[i] -> repair_restart = true :: else -> skip fi
#endif
        :: else -> skip
        fi;
        remember_applied(known[i]);
#if !ALL_OLD
        if :: root_new && (applied_conflict || GEN(applied_floor) > GEN(memory_c[root])) -> recovery_needed = true :: else -> skip fi;
#endif
        i++
    :: else -> break
    od;
    accept_refill_queries();
#if !ALL_OLD
    if
    :: repair_restart -> repair_pending = true; recovery_needed = true; begin_collect()
    :: else -> skip
    fi;
#endif
    check_done();
    retry_committed();
    if
    :: !collecting && recovery_needed -> begin_collect()
    :: collecting ->
        all_read = true; i = 0;
        do
        :: i < N -> if :: online[i] && !received[i] -> all_read = false :: else -> skip fi; i++
        :: else -> break
        od;
        if :: all_read -> collecting = false; select_recovery() :: else -> skip fi
    :: else -> skip
    fi
}

proctype Coordinator() {
    do
    #if LEGACY_CASE
    :: atomic { legacy_coordinator_enabled -> legacy_coordinator_turn(); clear_legacy_scratch() }
#else
    :: atomic { true -> coordinator_turn() }
#endif
    od
}

inline complete_refill(node) {
#if ALL_OLD
    skip
#else
    if
    :: upgraded[node] && refill_needed[node] ->
        complete_commit(node);
        if
        :: ordinary_body[node] == NONE && root_new ->
            /* The root's normal blank-metadata recovery publishes first;
             * receiving that body starts the extra causal confirmation. */
            coordinator_turn(); collect_mask(7); select_recovery(); assert(!blocked);
            complete_commit(node)
        :: else -> skip
        fi;
        if
        :: REFILL_TRIGGER(node) ->
            start_refill_query(node); forward_refill_query(node); forward_refill_query(node);
            coordinator_turn();
            if :: root_new -> collect_mask(7); select_recovery(); assert(!blocked) :: else -> skip fi;
            forward_refill_reply(node); deliver_refill_reply(node); complete_commit(node)
        :: else -> skip
        fi
    :: else -> skip
    fi
#endif
}

init {
    atomic {
        generation[BASE] = 1; i = 0;
        do
        :: i < N -> disk_c[i] = BASE; memory_c[i] = BASE; applied[i] = BASE;
            online[i] = true; upgraded[i] = !ALL_OLD; known[i] = BASE; i++
        :: else -> break
        od;
        root = A; root_new = !ALL_OLD; applied_floor = BASE; check_budget(); collect_mask(7);

#if LEGACY_CASE
        assert(!ALL_OLD);
        generation[X] = 2; generation[Y] = 2;
        root = B;
        disk_c[A] = X; disk_c[B] = Y; disk_c[C] = Y;
#if SCENARIO == 31
        disk_c[C] = BASE;
#elif SCENARIO == 32
        root = A;
#elif SCENARIO == 34
        /* Root memory's body is absent from every persistent read. */
        disk_c[B] = X; disk_c[C] = BASE;
#elif SCENARIO == 35
        /* Higher recoverable proposed quorum must take normal recovery. */
        generation[BASE] = 3; disk_c[A] = X; disk_c[B] = X; disk_c[C] = Y;
        disk_p[A] = BASE; disk_p[B] = BASE;
#endif
        i = 0;
        do
        :: i < N -> memory_c[i] = disk_c[i]; memory_p[i] = disk_p[i]; applied[i] = disk_c[i]; known[i] = applied[i]; i++
        :: else -> break
        od;
#if SCENARIO == 34
        memory_c[B] = Y; applied[B] = Y; known[B] = Y;
#endif
        applied_floor = applied[root]; applied_conflict = true;
        recovery_needed = true;
#if SCENARIO == 36
        collect_mask(2);
#else
        collect_mask(7);
#endif
        /* Normal recovery cannot publish an old disputed generation. */
#if SCENARIO != 35
        select_recovery(); assert(blocked && recovered == NONE);
#endif
        prepare_legacy_replace();
#if SCENARIO >= 34 || !LEGACY_REPAIR
        assert(!legacy_granted && operation == NONE && GEN(Z) == 0);
#else
        assert(legacy_granted && operation == Z && GEN(Z) == 3);
#endif
#elif SCENARIO == 1
        /* First delivery to C is lost; the active root must retry. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); collect_mask(3); reply_ok();
        commits[C]?scratch_v; assert(scratch_v == X); abandon_request();
#elif SCENARIO == 2 || SCENARIO == 8
        /* Return C BEFORE formatting B: never two unavailable/blank nodes. */
        stop_node(C); start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); collect_mask(3); reply_ok();
        restart_node(C); format_node(B);
#if SCENARIO == 8
        upgraded[C] = false; change_root(C, false);
#else
        change_root(C, true);
#endif
        assert(disk_c[A] == X && disk_c[B] == NONE && disk_c[C] == BASE);
#elif SCENARIO == 3
        /* Quorum proposal, one durable committed copy, no OK, failover. */
        start_write(X); complete_vote(A); complete_vote(B); learn(); complete_commit(A);
        stop_node(A); lose_commits(B); lose_commits(C);
        change_root(B, true); collect_mask(6); select_recovery();
        restart_node(A); assert(ok_count == 0);
#elif SCENARIO == 4
        /* A newer successful write must supersede an incomplete rollout. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); collect_mask(3); reply_ok();
        change_root(B, true); collect_mask(3); select_recovery();
        /* Retain X in C's queue, deliver it before the newer publication. */
        complete_commit(C); collect_mask(7);
        start_write(Y); complete_vote(B); complete_vote(C); learn();
        complete_commit(B); complete_commit(C); collect_mask(6); reply_ok(); abandon_request();
#elif SCENARIO == 5
        /* Two split proposals without decisions; a third request wins. */
        start_write(X); complete_vote(A); change_root(B, true);
        collect_mask(7); start_write(Y); complete_vote(B); change_root(C, true);
        collect_mask(7); select_recovery(); collect_mask(7);
        start_write(Z); complete_vote(A); complete_vote(B); learn();
        complete_commit(C); complete_commit(A); complete_commit(B); collect_mask(3); reply_ok(); abandon_request();
#elif SCENARIO == 6 || SCENARIO == 7
        /* The old root's early OK, or an all-new root with no first OK. */
#if SCENARIO == 6
        root_new = false; upgraded[A] = false;
#endif
        start_write(X); complete_vote(A); complete_vote(B); learn();
#if SCENARIO == 6
        reply_ok();
#endif
        complete_commit(A); stop_node(A); lose_commits(B); lose_commits(C); change_root(B, true);
        collect_mask(6); select_recovery(); collect_mask(6);
        start_write(Y); complete_vote(B); complete_vote(C); learn();
        complete_commit(B); complete_commit(C); collect_mask(6); reply_ok();
        restart_node(A); abandon_request();
#elif SCENARIO == 9
        /* A formatted participant must not vote before acquiring state. */
        format_node(B); collect_mask(5); start_write(X);
        propose(B);
        if
        :: accepted -> require(false, 3); drain_disk(B); deliver_ack()
        :: else -> skip
        fi;
        complete_vote(A); complete_vote(C); learn();
        complete_commit(A); complete_commit(C); collect_mask(5); reply_ok(); abandon_request();
#elif SCENARIO == 10
        /* Root cancellation invalidates ACKs, not queued replica writes. */
        start_write(X); propose(A); propose(B); change_root(C, true);
        drain_disk(A); drain_disk(B); deliver_ack(); deliver_ack();
        assert(stale_acks == 2 && ok_count == 0);
        collect_mask(7); select_recovery();
#elif SCENARIO == 11
        /* Bounded nondeterministic commit cut: all 3^2 disk/delivery choices. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        if :: complete_commit(A) :: skip fi;
        if :: complete_commit(B) :: deliver_commit(B) :: skip fi;
        if :: complete_commit(C) :: deliver_commit(C) :: skip fi;
        collect_mask(7); count_reads();
        if :: committed_copies[X] >= Q -> reply_ok() :: else -> skip fi;
        change_root(C, true);
#elif SCENARIO == 12
        /* Confirmation snapshots span a format, while the root stays alive. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); lose_commits(C);
        begin_collect(); read_one(B); deliver_read(B);
        format_node(B); read_one(A); deliver_read(A); reply_ok(); abandon_request();
#elif SCENARIO == 13
        /* Two publications at reused g2, neither requester got OK. */
        start_write(X); complete_vote(A); complete_vote(B); learn(); complete_commit(A);
        stop_node(A); lose_commits(B); lose_commits(C); change_root(C, true);
        collect_mask(6); select_recovery(); collect_mask(6);
        start_write(Y); complete_vote(B); complete_vote(C); learn(); complete_commit(C);
        restart_node(A); abandon_request(); assert(ok_count == 0);
#elif SCENARIO == 14
        /* Higher applied feedback can precede its committed disk write.
         * A single re-collect may read the old disk; retry until it catches up. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        lose_commits(B); lose_commits(C); format_node(B); change_root(C, true);
        assert(disk_c[A] == BASE && applied[A] == X && disk_c[B] == NONE);
        assert(ok_count == 0);
#elif SCENARIO == 15
        /* Ordinary upgrade, downgrade and another write on the old root. */
        root_new = false; upgraded[A] = false;
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); complete_commit(C); collect_mask(7); reply_ok();
        stop_node(B); restart_node(B); upgraded[B] = !ALL_OLD; change_root(B, true);
        collect_mask(7); select_recovery();
        stop_node(A); restart_node(A); upgraded[A] = false; change_root(A, false);
        collect_mask(7); select_recovery(); collect_mask(7);
        complete_commit(A); complete_commit(B); complete_commit(C);
        start_write(Y); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); complete_commit(C); reply_ok(); abandon_request();
#elif SCENARIO == 16
        /* Rollback with a committed quorum and unfinished IO on the third node. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); deliver_commit(C); collect_mask(3); reply_ok();
        assert(disk_c[C] == BASE && applied[C] == X);
        stop_node(C); restart_node(C); upgraded[C] = false; change_root(C, false);
#elif SCENARIO == 17
        /* Rollback cancels an operation; queued replica proposal IO survives. */
        start_write(X); propose(A); propose(B);
        stop_node(A); restart_node(A); upgraded[A] = false;
        change_root(C, false); upgraded[C] = false; assert(ok_count == 0);
#elif SCENARIO == 18
        /* A durable quorum decides the winner even if its OK is never sent. */
        stop_node(C); start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B);
        assert(ok_count == 0 && expected == X);
        restart_node(C); format_node(B); change_root(C, true);
        assert(disk_c[A] == X && disk_c[B] == NONE && disk_c[C] == BASE);
#elif SCENARIO == 19 || SCENARIO == 20
        candidate_race_prefix();
#if SCENARIO == 20
        /* The third request follows an actual race produced by this candidate. */
        drain_disk(A); drain_disk(B); drain_disk(C); drain_acks();
        change_root(A, true); collect_mask(7); select_recovery(); require(!blocked, 5);
        collect_mask(7); start_write(Z);
        require(GEN(Z) > GEN(X) && GEN(Z) > GEN(Y), 6);
        vote_after_disk_cut(A); vote_after_disk_cut(B); vote_after_disk_cut(C); drain_acks();
        require(votes >= Q, 6); learn();
        /* Clear earlier deliveries, then exercise the existing retry rule. */
        complete_commit(A); complete_commit(B); complete_commit(C); retry_committed();
        complete_commit(A); complete_commit(B); collect_mask(3); reply_ok(); abandon_request();
        assert(expected == Z);
#endif
#elif SCENARIO == 21
        /* A lagging new root must not refill a blank node with BASE while a
         * newer published value is known but its recovery is still pending. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); deliver_commit(B); lose_commits(C);
        assert(disk_c[A] == X && disk_c[B] == BASE && applied[B] == X);
        stop_node(A); change_root(C, true); collect_mask(6); select_recovery();
        assert(!blocked && memory_c[root] == X && copies[X] != 0 && applied_floor == X);
        drain_disk(B); assert(expected == X && ok_count == 0);
        restart_node(A); format_node(B);
        coordinator_turn();
        assert(known[A] == X && memory_c[root] == X && applied_floor == X);
        /* The blank notification invalidates the previous recovery snapshot. */
        assert(repair_pending && collecting && len(commits[B]) == 0);
        collect_mask(5); select_recovery();
        deliver_commit(B); drain_disk(B); complete_refill(B);
        assert(disk_c[A] == X && disk_c[B] == X);
        format_node(A);
        assert(disk_c[A] == NONE && disk_c[B] == X && max_bad == 1);
#elif SCENARIO == 22
        /* Published metadata survives while its only remaining disk body is
         * a minority proposed. Formatting clears the live metadata source. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); complete_vote(B); learn();
        deliver_commit(B); lose_commits(C);
        stop_node(A); change_root(C, true);
        assert(applied_floor == X && known[B] == X && disk_c[B] == BASE);
        restart_node(A); format_node(B); coordinator_turn();
        assert(collecting && applied_floor == X && known[B] == NONE && memory_c[root] == BASE);
        assert(len(commits[B]) == 0);
        assert(disk_p[A] == X && disk_p[B] == NONE && disk_c[A] == BASE && disk_c[C] == BASE);
        collect_mask(5); select_recovery();
        require(!blocked && recovered == X && copies[X] == 1, 7);
#elif SCENARIO == 23
        /* An unpublished minority proposal is not an applied watermark and
         * must not permanently prevent recovery of BASE. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); change_root(C, true);
        collect_mask(7); select_recovery();
        require(!blocked && recovered == BASE && applied_floor == BASE, 7);
#elif SCENARIO == 24
        /* Older metadata is delivered first; current applied reports are
         * delayed until after both formats. Bodies are read from disk. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); deliver_commit(B); lose_commits(C);
        stop_node(A); change_root(C, true);
        known[A] = NONE; known[B] = BASE; applied_floor = BASE;
        feedback[B] = true; defer_feedback[B] = true;
        collect_mask(6); select_recovery(); assert(!blocked && memory_c[root] == BASE);
        drain_disk(B); assert(expected == X && ok_count == 0);
        restart_node(A); defer_feedback[A] = true;
        format_node(B); defer_feedback[B] = false;
        coordinator_turn();
        assert(repair_pending && collecting && len(commits[B]) == 0);
        collect_mask(5); select_recovery();
        assert(!blocked && recovered == X && memory_c[root] == X);
        complete_commit(B); complete_refill(B); assert(disk_c[B] != NONE);
        format_node(A); defer_feedback[A] = false;
        assert(max_bad == 1);
#elif SCENARIO == 25
        /* Overapproximation: an intermediary may have an old body queued for
         * its child even after the root has noticed that the child is blank.
         * This is not claimed to refine a concrete C++ session history. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); lose_commits(C);
        change_root(C, true); format_node(B); query_via[B] = A;
        offer_commit(B, BASE); deliver_commit(B); drain_disk(B);
        require(disk_c[B] == NONE && refill_needed[B], 8);
        complete_refill(B); assert(disk_c[B] == X);
        format_node(A); assert(max_bad == 1);
#elif SCENARIO == 26
        /* A valid old reply is in the intermediary before a new successful
         * write; restarting the blank recipient changes its binding cookie. */
        assert(!ALL_OLD && root_new);
        format_node(B); query_via[B] = C;
        collect_mask(5); select_recovery(); deliver_commit(B);
        start_refill_query(B); forward_refill_query(B); forward_refill_query(B);
        coordinator_turn(); collect_mask(5); select_recovery();
        assert(len(query_reply_hops[B]) == 1 && refill_needed[B]);
        complete_commit(C);
        start_write(X); complete_vote(A); complete_vote(C); learn();
        complete_commit(A); complete_commit(C); lose_commits(B);
        collect_mask(5); reply_ok(); abandon_request();
        stop_node(B); restart_node(B);
        forward_refill_reply(B); deliver_refill_reply(B);
        require(disk_c[B] == NONE && applied[B] == NONE && refill_needed[B], 8);
        complete_refill(B); assert(disk_c[B] == X);
#elif SCENARIO == 27
        /* Leadership changes with a blank node's fresh query still pending.
         * The replacement root must read afresh for the new binding cookie. */
        assert(!ALL_OLD && root_new);
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); lose_commits(C);
        format_node(C); query_via[C] = B;
        offer_commit(C, X); deliver_commit(C);
        start_refill_query(C); forward_refill_query(C); forward_refill_query(C);
        coordinator_turn(); assert(query_await_read[C] && collecting);
        change_root(B, true); query_via[C] = A;
        require(!query_inflight[C] && !query_await_read[C], 8);
        complete_refill(C); assert(disk_c[C] == X);
#elif SCENARIO == 28
        /* The new recipient's optional query marker is absent in an old
         * root's reply. Keep the old body's compatibility fallback. */
        root_new = false; upgraded[A] = false;
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); lose_commits(C);
        format_node(C); query_via[C] = B;
#if ALL_OLD
        offer_commit(C, X); complete_commit(C)
#else
        if
        :: upgraded[C] ->
            /* The old root's normal binding response is held by the guard. */
            offer_commit(C, X); deliver_commit(C);
            start_refill_query(C); forward_refill_query(C); forward_refill_query(C);
            coordinator_turn();
            query_reply_hops[C]?scratch_cookie,scratch_v,scratch_fresh;
            require(!scratch_fresh && scratch_v == X, 8);
            query_replies[C]!scratch_cookie,scratch_v,scratch_fresh;
            deliver_refill_reply(C); complete_commit(C)
        :: else -> offer_commit(C, X); complete_commit(C)
        fi
#endif
        assert(disk_c[C] == X);
#elif SCENARIO == 29
        /* Accepting the legacy QueryConfig body must not bypass an old
         * root's failed recovery and make a second format possible. */
        start_write(X); complete_vote(A); complete_vote(B); learn();
        complete_commit(A); complete_commit(B); lose_commits(C);
        format_node(B); upgraded[C] = false; change_root(C, false);
        collect_mask(7); select_recovery(); assert(blocked && memory_c[root] == BASE);
#if !ALL_OLD
        complete_refill(B);
#endif
        if
        :: disk_c[B] != NONE ->
            format_node(A);
            require(disk_c[A] == X || disk_c[B] == X || disk_c[C] == X, 9)
        :: else -> assert(disk_c[A] == X && disk_c[B] == NONE && disk_c[C] == BASE)
        fi;
        assert(max_bad == 1);
#else
#error Unknown SCENARIO
#endif

        suffix = true;
        /* No outstanding client request is retried implicitly. Recovery and
         * dissemination must converge using persisted/queued protocol state. */
#if !LEGACY_CASE
        operation = NONE;
#endif
        run Disk(A); run Disk(B); run Disk(C);
        run Network(A); run Network(B); run Network(C);
        run Reader(A); run Reader(B); run Reader(C);
#if !LEGACY_CASE
        run Refill(A); run Refill(B); run Refill(C);
#endif
        run Coordinator()
#if LEGACY_CASE
        /* No legacy fixture ever sets refill_needed. Finite workers are
         * created last so SPIN can remove their terminated processes. */
        ; run LegacyProposer(A); run LegacyProposer(B); run LegacyProposer(C)
#endif
    }
}

#if LEGACY_CASE && LEGACY_REACH
ltl convergence { <> done }
#else
ltl convergence { <> [] done }
#endif
