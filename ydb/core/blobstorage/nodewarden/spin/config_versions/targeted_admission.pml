/* One disk per member in one DC; finite failure prefixes.
 * MODE: 0 = old guard, 1 = PrevConfig-only exception, 2 = root challenge. */
#ifndef SCENARIO
#define SCENARIO 1
#endif
#ifndef MODE
#define MODE 2
#endif
#ifndef OLD_N
#define OLD_N 1
#endif
#ifndef TOTAL_N
#define TOTAL_N 2
#endif
#ifndef CHECK_SAFETY
#define CHECK_SAFETY 1
#endif
#ifndef ROOT_FENCE
#define ROOT_FENCE 1
#endif
#ifndef ACTOR_FENCE
#define ACTOR_FENCE 1
#endif
#ifndef CONFIRM_RECOVERY
#define CONFIRM_RECOVERY 1
#endif
#define N TOTAL_N
#define ADDED_N (TOTAL_N - OLD_N)
#define PROPOSED 2
#define LATEST 3
#define REQUEST_SCATTER 11
#define REQUEST_TASK 21

byte disk_c[N];
byte disk_p[N];
bool has_local_metadata[N];
bool root_pending[N];
byte actor_life[N];
byte helper_cookie[N];
byte grant_life[N];
byte grant_cookie[N];
bool grant_was_valid[N];
bool admitted[N];
byte done;
byte accepted = OLD_N;
byte root_current = PROPOSED;
byte root_scatter = REQUEST_SCATTER;
byte root_task = REQUEST_TASK;
bool root_active = true;
bool root_recovery;
bool advances_conflict;
byte root_recovery_generation = 1;
byte proposition_recovery_generation = 1;
bool root_ok;
bool complete;
bool failed_safety;

#if SCENARIO == 12
byte published_body;
bool joining_members_bound;
bool replace_holds_invoke;
bool queued_recovery;
bool confirmation_revalidated;
byte last_committed_snapshot;
#endif

/* Node, actor incarnation, local cookie, root scatter cookie, task id, config meta. */
chan challenges = [N] of {byte, byte, byte, byte, byte, byte};
chan replies[N] = [2] of {byte, byte, bool};

inline require(ok) {
    if
    :: ok -> skip
    :: else -> failed_safety = true;
#if CHECK_SAFETY
        assert(false)
#endif
    fi
}

proctype ValidateOnOriginRoot() {
    byte node;
    byte life;
    byte cookie;
    byte scatter;
    byte task;
    byte meta;
    bool confirmed;
    bool valid;
    do
    :: challenges?node,life,cookie,scatter,task,meta ->
        atomic {
            valid = (root_active && (!root_recovery || advances_conflict) && root_pending[node]
                && scatter == root_scatter && task == root_task && meta == root_current
                && root_recovery_generation == proposition_recovery_generation);
#if ROOT_FENCE
            confirmed = valid;
#else
            confirmed = true;
#endif
            if
            :: confirmed -> grant_life[node] = life; grant_cookie[node] = cookie;
                            grant_was_valid[node] = valid
            :: else -> skip
            fi;
            replies[node]!life,cookie,confirmed
        }
    :: done == ADDED_N -> break
    od
}

proctype AddedNode(byte node) {
    byte reply_life;
    byte reply_cookie;
    bool reply_ok;
    bool permitted;
    bool authorized;
    if
    :: SCENARIO == 5 -> permitted = false
    :: else ->
#if MODE == 0
        permitted = has_local_metadata[node]
#elif MODE == 1
        permitted = true
#else
        if
        :: SCENARIO == 4 -> skip
        :: else -> challenges!node,actor_life[node],helper_cookie[node],
                              REQUEST_SCATTER,REQUEST_TASK,PROPOSED
        fi;
        do
        :: replies[node]?reply_life,reply_cookie,reply_ok ->
            if
#if ACTOR_FENCE
            :: reply_life == actor_life[node] && reply_cookie == helper_cookie[node] ->
#else
            :: true ->
#endif
                permitted = reply_ok;
                authorized = (permitted && grant_was_valid[node]
                    && grant_life[node] == actor_life[node]
                    && grant_cookie[node] == helper_cookie[node]);
                break
            :: else -> skip
            fi
        od
#endif
    fi;
    if
    :: permitted ->
        admitted[node] = true;
        require(SCENARIO == 1 || authorized);
        require(disk_c[node] == 0);
        disk_p[node] = PROPOSED;
        has_local_metadata[node] = true;
        require(has_local_metadata[node] && disk_c[node] == 0);
        atomic { accepted++; done++ }
    :: else -> atomic { done++ }
    fi
}

#if SCENARIO == 12
proctype ConfirmCommittedQuorum() {
    byte node;
    byte committed;
    byte matching_stateful;
    bool committed_quorum;
    joining_members_bound;
    do
    :: !root_ok ->
        atomic {
            node = 0;
            committed = 0;
            matching_stateful = 0;
            do
            :: node < N ->
                if
                :: disk_c[node] == published_body -> committed++
                :: else -> skip
                fi;
                if
                :: disk_c[node] == published_body || disk_p[node] == published_body ->
                    matching_stateful++
                :: else -> skip
                fi;
                node++
            :: else -> break
            od;
            last_committed_snapshot = committed;
            committed_quorum = committed > N / 2
        };
        if
#if CONFIRM_RECOVERY
        :: (committed_quorum || root_recovery) && matching_stateful > N / 2 ->
#else
        :: committed_quorum && matching_stateful > N / 2 ->
#endif
            require(published_body == PROPOSED && published_body == root_current);
            confirmation_revalidated = true;
            root_recovery = false
        :: else -> skip
        fi;
        if
        :: committed_quorum ->
            require(confirmation_revalidated && last_committed_snapshot > N / 2);
            replace_holds_invoke = false;
            root_ok = true
        :: else -> skip
        fi
    :: root_ok -> break
    od
}

proctype PropagatePublishedConfig() {
    byte node = OLD_N;
    joining_members_bound;
    do
    :: node < N && !root_recovery ->
        require(published_body == PROPOSED);
        disk_c[node] = published_body;
        has_local_metadata[node] = true;
        node++
    :: node == N -> break
    od
}

proctype QueuedConfigRecovery() {
    do
    :: queued_recovery && !replace_holds_invoke ->
        root_recovery = false;
        queued_recovery = false;
        break
    :: complete -> break
    od
}
#endif

proctype PropositionCoordinator() {
    byte node;
    done == ADDED_N;
#if SCENARIO == 12
    require(accepted > N / 2 && OLD_N > OLD_N / 2);
    published_body = PROPOSED;
    root_active = false;
    node = 0;
    do
    :: node < OLD_N -> disk_c[node] = PROPOSED; node++
    :: else -> break
    od;
    atomic {
        node = OLD_N;
        do
        :: node < N ->
            require(disk_c[node] == 0 && disk_p[node] == PROPOSED);
            node++
        :: else -> break
        od;
        require(replace_holds_invoke);
        root_recovery = true;
        queued_recovery = true;
        joining_members_bound = true
    };
    root_ok;
    require(last_committed_snapshot > N / 2 && !replace_holds_invoke);
#else
    if
    :: (SCENARIO == 1 || SCENARIO == 10) && accepted > N / 2 && root_active ->
        require(accepted > N / 2 && OLD_N > OLD_N / 2);
        root_ok = true;
        root_recovery = false;
        node = 0;
        do
        :: node < N ->
            disk_c[node] = PROPOSED;
            has_local_metadata[node] = true;
            node++
        :: else -> break
        od
    :: else -> skip
    fi;
    if
    :: SCENARIO == 1 || SCENARIO == 10 -> skip
    :: SCENARIO == 4 ->
        require(!root_ok);
        node = OLD_N;
        do
        :: node < N -> require(disk_p[node] == PROPOSED && disk_c[node] == 0); node++
        :: else -> break
        od
    :: SCENARIO == 9 ->
        require(!root_ok);
        node = OLD_N;
        do
        :: node < N ->
            require(!admitted[node] && disk_p[node] == LATEST && disk_c[node] == 0);
            node++
        :: else -> break
        od
    :: else ->
        require(!root_ok);
        node = OLD_N;
        do
        :: node < N -> require(disk_p[node] == 0 && disk_c[node] == 0); node++
        :: else -> break
        od
    fi;
#endif
    complete = true
}

init {
    byte node;
    atomic {
        require(OLD_N > 0 && TOTAL_N > OLD_N);
        node = 0;
        do
        :: node < OLD_N ->
            disk_c[node] = 1;
            disk_p[node] = PROPOSED;
            has_local_metadata[node] = true;
            node++
        :: else -> break
        od;
#if SCENARIO == 2 || SCENARIO == 9
        root_current = LATEST;
        root_scatter = REQUEST_SCATTER + 1;
        root_task = REQUEST_TASK + 1;
#elif SCENARIO == 10 || SCENARIO == 11
        root_recovery = true;
        advances_conflict = true;
#if SCENARIO == 11
        root_recovery_generation = 2;
#endif
#elif SCENARIO == 6
        root_recovery_generation = 2;
#elif SCENARIO == 8
        root_recovery = true;
#elif SCENARIO == 3 || SCENARIO == 4
        root_current = LATEST;
        root_active = false;
#endif
        node = OLD_N;
        do
        :: node < N ->
            actor_life[node] = 1;
            helper_cookie[node] = 1;
            root_pending[node] = SCENARIO != 7;
#if SCENARIO == 9
            disk_p[node] = LATEST;
            has_local_metadata[node] = true;
#endif
#if SCENARIO == 3 || SCENARIO == 4
            grant_was_valid[node] = true;
            grant_life[node] = 1;
            grant_cookie[node] = 1;
            replies[node]!1,1,true;
#endif
#if SCENARIO == 3
            actor_life[node] = 2;
#endif
            run AddedNode(node);
            node++
        :: else -> break
        od;
        run ValidateOnOriginRoot();
        run PropositionCoordinator();
#if SCENARIO == 12
        replace_holds_invoke = true;
        run ConfirmCommittedQuorum();
        run PropagatePublishedConfig();
        run QueuedConfigRecovery()
#endif
    }
}

#if !CHECK_SAFETY
#if SCENARIO == 1 || SCENARIO == 10 || SCENARIO == 12
ltl convergence { <> (complete && root_ok) }
#else
ltl convergence { <> complete }
#endif
#endif
