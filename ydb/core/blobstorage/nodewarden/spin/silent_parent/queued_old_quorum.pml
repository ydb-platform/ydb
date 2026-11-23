#ifndef USE_OBSERVED_FLOOR
#define USE_OBSERVED_FLOOR 1
#endif
#ifndef RETAIN_PARTIAL_FLOOR
#define RETAIN_PARTIAL_FLOOR 1
#endif
#ifndef REQUIRE_FRESH_GUARD
#define REQUIRE_FRESH_GUARD 1
#endif
#ifndef FIRST_READ_MASK
#define FIRST_READ_MASK 0
#endif
#define N 8
#define OLD_Q 51
#define HEALTHY 254
mtype = { NONE, A, B };
byte proposedGeneration[N];
mtype proposedBody[N];
byte committedGeneration[N];
mtype committedBody[N];
byte oldCertificate;
byte observedGeneration;
byte newGeneration;
byte currentSession = 2;
byte oldSession = 1;
bool rootScepter = true;
bool oldTaskActive = true;
bool newPublished;
bool oldCompleted;
bool disconnected;
bool oldQueuedCompletionBeforeDisconnect;
bool conflictingPublication;
bool fingerprintAbort;
bool allNewest;
bool partialProposalSeen;
bool partialProposalUnseen;
bool partialFloorLost;
bool partialReadGuarded;
bool freshReadCompleted;
/* Same fixture geometry: each pile has DC1(single), DC2(single),
 * DC3(two drives); static mirror groups use 1,2,3 and 5,6,7.
 * 4 and 8 contribute drive/DC quorum, not static-group votes. */
inline quorum(mask, ok) {
    ok = (((((mask & 1) != 0) + ((mask & 2) != 0) + ((mask & 12) == 12)) >= 2)
       && ((((mask & 16) != 0) + ((mask & 32) != 0) + ((mask & 192) == 192)) >= 2)
       && ((((mask & 1) != 0) + ((mask & 2) != 0) + ((mask & 4) != 0)) >= 2)
       && ((((mask & 16) != 0) + ((mask & 32) != 0) + ((mask & 64) != 0)) >= 2))
}
proctype OldQueuedCompletion() {
    bool q;
    newPublished;
    atomic {
        if
        :: rootScepter && oldTaskActive ->
            quorum(oldCertificate, q); assert(q);
            oldQueuedCompletionBeforeDisconnect = !disconnected;
            committedGeneration[0] = 2;
            committedBody[0] = A;
            conflictingPublication = newPublished && newGeneration == 2;
            assert(!conflictingPublication)
        :: else -> skip
        fi;
        oldCompleted = true
    }
}
proctype RealDisconnectHandler() {
    atomic {
        rootScepter = false;
        oldTaskActive = false;
        disconnected = true
    }
}
proctype RejoinWorkingRoot() {
    byte k;
    oldCompleted && disconnected;
    atomic {
        assert(currentSession != oldSession);
        if
        :: committedGeneration[0] < newGeneration ->
            committedGeneration[0] = newGeneration;
            committedBody[0] = B
        :: committedGeneration[0] == newGeneration ->
            fingerprintAbort = committedBody[0] != B
        :: else -> skip
        fi;
        allNewest = !fingerprintAbort;
        k = 0;
        do
        :: k < N ->
            if
            :: committedGeneration[k] != newGeneration || committedBody[k] != B -> allNewest = false
            :: else -> skip
            fi;
            k++
        :: else -> break
        od
    }
}
inline read_config(node, generation) {
    if
    :: committedGeneration[node] > generation -> generation = committedGeneration[node]
    :: else -> skip
    fi;
    if
    :: proposedGeneration[node] > generation -> generation = proposedGeneration[node]
    :: else -> skip
    fi
}
inline remember_floor(generation) {
    if
    :: generation > observedGeneration -> observedGeneration = generation
    :: else -> skip
    fi
}
proctype NewRoot() {
    byte k;
    byte replies;
    byte collectedGeneration = 1;
    bool q;
    bool receive;
    atomic {
        /* Recovery receives any subset before its deadline, including no A2 copy. */
        do
        :: k < N ->
            if
            :: (HEALTHY & (1 << k)) != 0 ->
#if FIRST_READ_MASK
                receive = (FIRST_READ_MASK & (1 << k)) != 0;
#else
                if :: receive = true :: receive = false fi;
#endif
                if
                :: receive ->
                    replies = replies | (1 << k);
                    read_config(k, collectedGeneration)
                :: else -> skip
                fi
            :: else -> skip
            fi;
            k++
        :: else -> break
        od;
        quorum(replies, q);
#if RETAIN_PARTIAL_FLOOR
        remember_floor(collectedGeneration);
#else
        if
        :: q -> remember_floor(collectedGeneration)
        :: else -> skip
        fi;
#endif
        partialProposalSeen = !q && collectedGeneration == 2;
        partialProposalUnseen = !q && collectedGeneration == 1;
        partialFloorLost = partialProposalSeen && observedGeneration < 2;

#if REQUIRE_FRESH_GUARD
        if
        :: !q ->
            /* The command returns no quorum; it cannot publish. A retry after
             * the finite fault prefix gets all healthy metadata responses. */
            partialReadGuarded = true;
            assert(!newPublished);
            k = 0;
            replies = 0;
            collectedGeneration = 1;
            do
            :: k < N ->
                if
                :: (HEALTHY & (1 << k)) != 0 ->
                    replies = replies | (1 << k);
                    read_config(k, collectedGeneration)
                :: else -> skip
                fi;
                k++
            :: else -> break
            od;
            quorum(replies, q); assert(q);
            remember_floor(collectedGeneration)
        :: else -> skip
        fi;
        freshReadCompleted = true;
#endif
#if USE_OBSERVED_FLOOR
        newGeneration = observedGeneration + 1;
#else
        newGeneration = 2;
#endif
        k = 0;
        do
        :: k < N ->
            if
            :: (HEALTHY & (1 << k)) != 0 ->
                proposedGeneration[k] = newGeneration; proposedBody[k] = B;
                committedGeneration[k] = newGeneration; committedBody[k] = B
            :: else -> skip
            fi;
            k++
        :: else -> break
        od;
        assert(proposedBody[1] == B && (oldCertificate & 2) != 0);
        newPublished = true
    }
}
init {
    byte k;
    bool q;
    atomic {
        k = 0;
        do
        :: k < N ->
            committedGeneration[k] = 1;
            committedBody[k] = NONE;
            if
            :: (OLD_Q & (1 << k)) != 0 -> proposedGeneration[k] = 2; proposedBody[k] = A
            :: else -> skip
            fi;
            k++
        :: else -> break
        od;
        oldCertificate = OLD_Q;
        quorum(oldCertificate, q); assert(q);
        quorum(HEALTHY, q); assert(q);
        quorum(OLD_Q & HEALTHY, q); assert(!q);
        observedGeneration = 1;
        run NewRoot();
        run OldQueuedCompletion();
        run RealDisconnectHandler();
        run RejoinWorkingRoot()
    }
}
ltl no_same_generation_fork { [] !conflictingPublication }
ltl newest_after_resume { <> allNewest }
ltl reach_old_queued_completion { [] !oldQueuedCompletionBeforeDisconnect }
ltl partial_floor_preserved { [] !partialFloorLost }
ltl mutation_requires_fresh_read { [] (newPublished -> freshReadCompleted) }
ltl reach_partial_proposal_seen { [] !partialProposalSeen }
ltl reach_partial_proposal_unseen { [] !partialProposalUnseen }
ltl reach_partial_read_guard { [] !partialReadGuarded }
