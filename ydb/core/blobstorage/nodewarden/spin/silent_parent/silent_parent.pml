#ifndef NODES
#define NODES 3
#endif
#ifndef ROOT_HUNG
#define ROOT_HUNG 0
#endif
#ifndef FALSE_SUSPICION
#define FALSE_SUSPICION 0
#endif
#ifndef ENABLE_DISCONNECT
#define ENABLE_DISCONNECT 1
#endif
#ifndef COOLDOWN
#define COOLDOWN 1
#endif
#ifndef SESSION_FENCE
#define SESSION_FENCE 1
#endif
#ifndef ABORT_OLD_TASK
#define ABORT_OLD_TASK 1
#endif
#ifndef LATE_RECOVERY
#define LATE_RECOVERY 0
#endif
#ifndef MISSING_PILE
#define MISSING_PILE 0
#endif

/* Three nodes: R(0)->M(1)->C(2).
 * Six nodes: R->M->C->{D(3),E(4)}, R->F(5).
 * Piles are {R,M,C} and {D,E,F}; abstract quorum requires 2 in each.
 * Parent-hung case: C monitors M; root-hung case: M monitors R.
 */
bool rootService = !ROOT_HUNG || FALSE_SUSPICION;
bool middleService = ROOT_HUNG || FALSE_SUSPICION;
bool oldICConnected = true;
byte oldSession = 1;
byte currentSession = 1;
bool oldBindingPresent = true;
bool disconnectIssued;
bool childDisconnectedPending;
bool parentDisconnectedPending;
bool bindRequest;
bool newBinding;
bool cooldownActive;
bool childStillHasOldTask = true;
bool oldWriteDone;
bool lateOldVoteAccepted;
bool oldCallbackIgnored;
bool oldSessionDropped;
bool heartbeatReplyRejected;
bool recoveredMiddleRejoined;
bool followerFRejoined;
byte leader = 0;
byte misses;
byte available;
byte pile0;
byte pile1;
bool requestActive;
bool success;
bool allHealthyReached;

active proctype ParentHealthMonitor() {
    do
    :: misses < 3 -> misses++
    :: else -> break
    od;
#if ENABLE_DISCONNECT
    atomic {
        disconnectIssued = true;
        cooldownActive = COOLDOWN;
        /* No local handoff on a session that the old parent still owns. */
        assert(oldBindingPresent);
        oldICConnected = false;
        currentSession++;
        childDisconnectedPending = true;
        parentDisconnectedPending = true
    }
#endif
}

active proctype ChildDisconnectHandler() {
    childDisconnectedPending;
    atomic {
        assert(!oldICConnected);
        oldBindingPresent = false;
        childDisconnectedPending = false;
#if ABORT_OLD_TASK
        childStillHasOldTask = false;
#endif
    };
#if ROOT_HUNG
    /* M retains healthy C/{C,D,E}; its actual physical quorum can elect it.
     * Its root update precedes recovery collect on surviving child sessions.
     */
    atomic {
        leader = 1;
        newBinding = true
    }
#else
    do
    :: !newBinding ->
        if
        :: cooldownActive -> bindRequest = true; newBinding
        :: else ->
            /* Negative control: deterministically pick the same live-IC,
             * hung-keeper peer after every failed attempt. */
            skip
        fi
    :: else -> break
    od
#endif
}

active proctype WorkingRootService() {
    bindRequest;
    atomic {
        assert(rootService);
        assert(!oldBindingPresent && !oldICConnected);
        newBinding = true
    }
}

active proctype FollowerF() {
#if NODES == 6 && ROOT_HUNG
    leader == 1;
    /* F independently detects the same root's missing keeper responses. */
    followerFRejoined = true
#else
    followerFRejoined = true
#endif
}

active proctype LateOldWrite() {
    disconnectIssued;
    atomic {
        /* The physical operation is not canceled by binding/scatter timeout. */
        oldWriteDone = true;
        if
        :: childStillHasOldTask ->
            if
            :: oldICConnected && oldSession == currentSession -> lateOldVoteAccepted = true
            :: else -> oldSessionDropped = true
            fi;
#if !SESSION_FENCE
            lateOldVoteAccepted = true;
#endif
        :: else -> oldCallbackIgnored = true
        fi;
        assert(!lateOldVoteAccepted)
    }
}

active proctype LateHeartbeatReply() {
    disconnectIssued;
    atomic {
        /* An old outstanding QueryWorkingRoot reply cannot validate a new
         * binding: both original binding identity and probe cookie matter. */
        assert(oldSession != currentSession);
        heartbeatReplyRejected = true
    }
}

active proctype RecoverMiddle() {
#if LATE_RECOVERY && !ROOT_HUNG
    newBinding;
    atomic { middleService = true };
    parentDisconnectedPending;
    atomic {
        /* Its old child refs are removed by the actual disconnected session. */
        parentDisconnectedPending = false;
        recoveredMiddleRejoined = true
    }
#else
    skip
#endif
}

active proctype Requests() {
#if FALSE_SUSPICION
    disconnectIssued;
#endif
    do
    :: !allHealthyReached ->
        requestActive = true;
        atomic {
            available = 0;
            if
            :: leader == 0 && rootService ->
                available = 1;
#if NODES == 6
                available = available | 32;
#endif
                if
                :: middleService -> available = available | 2
                :: else -> skip
                fi;
                if
                :: oldICConnected && middleService ->
                    available = available | 6;
#if NODES == 6
                    available = available | 24;
#endif
                :: else -> skip
                fi;
                if
                :: newBinding ->
                    available = available | 4;
#if NODES == 6
                    available = available | 24;
#endif
                :: else -> skip
                fi;
                if
                :: recoveredMiddleRejoined -> available = available | 2
                :: else -> skip
                fi
            :: leader == 1 && middleService ->
                available = 6;
#if NODES == 6
                available = available | 24;
                if
                :: followerFRejoined -> available = available | 32
                :: else -> skip
                fi;
#endif
            :: else -> skip
            fi;
#if MISSING_PILE
            available = available & 7;
#endif
            pile0 = ((available & 1) != 0) + ((available & 2) != 0) + ((available & 4) != 0);
#if NODES == 6
            pile1 = ((available & 8) != 0) + ((available & 16) != 0) + ((available & 32) != 0);
            if
            :: pile0 >= 2 && pile1 >= 2 -> success = true
            :: else ->
                skip
            fi;
#else
            if
            :: pile0 >= 2 -> success = true
            :: else ->
                skip
            fi;
#endif
            if
            :: success ->
#if ROOT_HUNG && !FALSE_SUSPICION
#if NODES == 6
                allHealthyReached = available == 62;
#else
                allHealthyReached = available == 6;
#endif
#elif FALSE_SUSPICION || LATE_RECOVERY
#if NODES == 6
                allHealthyReached = available == 63;
#else
                allHealthyReached = available == 7;
#endif
#else
#if NODES == 6
                allHealthyReached = available == 61;
#else
                allHealthyReached = available == 5;
#endif
#endif
            :: else -> skip
            fi;
            requestActive = false
        }
    :: else -> break
    od
}

ltl eventual_success { <> success }
ltl eventual_new_binding { <> newBinding }
ltl eventual_all_healthy_reached { <> allHealthyReached }
ltl request_terminates { [] (requestActive -> <> !requestActive) }
ltl old_write_completes { <> oldWriteDone }
ltl reach_session_drop { [] !oldSessionDropped }
ltl reach_old_callback_ignore { [] !oldCallbackIgnored }
ltl reach_heartbeat_rejection { [] !heartbeatReplyRejected }
ltl reach_recovered_middle { [] !recoveredMiddleRejoined }

ltl session_fencing { [] !lateOldVoteAccepted }
