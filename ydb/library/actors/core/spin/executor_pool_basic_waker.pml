/*
 * Finite-state model of dynamic thread-count changes in TBasicExecutorPool
 * waker. All processes run indefinitely.
 *
 * Deliberately modeled:
 *  - SuggestedThreadCount updates by the controller;
 *  - ThreadCount and CheckToSleepWorkers updates owned by the waker;
 *  - workers preferring a reduction request over taking an activation;
 *  - independent infinite producers keeping the queue bounded by MAX_QUEUE;
 *  - None -> Blocking -> Sleep and immediate parking;
 *  - waker-local ActiveMask and sleeping-stack membership;
 *  - SleepingCount containing active sleeping workers only;
 *  - coalesced RequestWaker notifications.
 *
 * Abstracted away:
 *  - actor/mailbox identity, affinity, harmonizer, metrics and memory orders;
 *  - stack order (membership is sufficient for these properties);
 *  - the spin duration (Spin is a state from which the waker may decide).
 */

#define N 2
#define PRODUCERS 2
#define MAX_QUEUE N
#define NONE 0
#define SPIN 1
#define SLEEP 2
#define WORK 3
#define BLOCKING 4

byte state[N];
bool worker_enabled[N];
bool in_sleeping_stack[N];
bool counted_sleeping[N];

byte suggested_thread_count = N;
byte thread_count = N;
byte active_count = N;
byte sleeping_count = 0;
byte reductions = 0;
byte activation_credits = 0;
byte queued_activations = 0;
bool waker_pending = false;
bool work_epoch = false;

inline request_waker() {
    atomic {
        if
        :: !waker_pending -> waker_pending = true
        :: else -> skip
        fi
    }
}

inline check_safety() {
    atomic {
        assert(suggested_thread_count >= 1 && suggested_thread_count <= N);
        assert(thread_count >= 1 && thread_count <= N);
        assert(active_count <= N);
        assert(sleeping_count <= active_count);
        assert(reductions <= N);
        assert(activation_credits <= MAX_QUEUE);
        assert(queued_activations <= activation_credits)
    }
}

proctype Worker(byte id) {
    do
    :: worker_enabled[id] && state[id] == WORK ->
        atomic {
            state[id] = NONE;
            work_epoch = !work_epoch
        }

    :: worker_enabled[id] && state[id] == NONE ->
        if
        :: atomic {
            reductions > 0 ->
            reductions--
        };
        state[id] = BLOCKING;
        request_waker()

        /* Reduction has priority over looking in the activation queue. Once
         * the worker observes no reduction, the waker may publish one before
         * the worker completes Pop, SetWork and the credit decrement. */
        :: reductions == 0 ->
            if
            :: atomic {
                queued_activations > 0 ->
                queued_activations--
            };
                state[id] = WORK;
                atomic {
                    activation_credits > 0 ->
                    activation_credits--
                }
            :: queued_activations == 0 && activation_credits > 0 ->
                /* A producer published a credit but has not pushed yet, or
                 * another worker popped the corresponding activation. */
                skip
            :: activation_credits == 0 ->
                state[id] = SPIN;
                request_waker()
            fi
        fi

    /* BLOCKING and SLEEP are parked; only the waker changes them. */
    od
}

proctype Producer() {
    do
    :: atomic {
        activation_credits < MAX_QUEUE ->
        activation_credits++
    };
        queued_activations++;
        if
        :: sleeping_count > 0 -> request_waker()
        :: else -> skip
        fi
    od
}

proctype Controller() {
    byte next;

    do
    :: true ->
        if
        :: next = 1
        :: next = N
        fi;
        if
        :: atomic {
            next != suggested_thread_count ->
            suggested_thread_count = next
        };
            request_waker()
        :: else -> skip
        fi;
        check_safety()
    od
}

proctype Waker() {
    byte desired;
    byte i;
    byte searching;
    byte budget;

    do
    :: waker_pending ->
        /* Matches clearing WakerPending before loading ThreadCount. */
        waker_pending = false;
        desired = suggested_thread_count;

        /* Unclaimed tokens belong to an older reconciliation pass. A worker
         * which claimed one is represented explicitly by BLOCKING. */
        reductions = 0;

        /* Resolve workers which have already claimed a reduction token. */
        i = 0;
        do
        :: i < N ->
            if
            :: worker_enabled[i] && state[i] == BLOCKING ->
                atomic {
                    if
                    :: active_count > desired ->
                        state[i] = SLEEP;
                        in_sleeping_stack[i] = true;
                        worker_enabled[i] = false;
                        active_count--
                    :: else -> state[i] = NONE
                    fi
                }
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Retire additional already idle workers. No reduction token is
         * needed: only the waker mutates its local active set. */
        i = 0;
        do
        :: i < N && active_count > desired ->
            if
            :: worker_enabled[i] && state[i] == SLEEP ->
                counted_sleeping[i] = false;
                sleeping_count--;
                worker_enabled[i] = false;
                active_count--
            :: worker_enabled[i] && state[i] == SPIN ->
                state[i] = SLEEP;
                in_sleeping_stack[i] = true;
                worker_enabled[i] = false;
                active_count--
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Publish requests only for workers which could not be retired by the
         * waker because they are currently in Work or None. */
        atomic {
            if
            :: active_count > desired -> reductions = active_count - desired
            :: else -> reductions = 0
            fi
        };

        /* Re-enable disabled sleeping workers after a count increase. */
        i = 0;
        do
        :: i < N && active_count < desired ->
            if
            :: !worker_enabled[i] && in_sleeping_stack[i] ->
                worker_enabled[i] = true;
                active_count++;
                counted_sleeping[i] = true;
                sleeping_count++
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        thread_count = active_count;

        searching = 0;
        i = 0;
        do
        :: i < N ->
            if
            :: worker_enabled[i] && state[i] == NONE -> searching++
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;
        if
        :: activation_credits > searching -> budget = activation_credits - searching
        :: else -> budget = 0
        fi;

        /* Resolve ordinary spinners using the current activation budget. */
        i = 0;
        do
        :: i < N ->
            if
            :: worker_enabled[i] && state[i] == SPIN && budget > 0 ->
                state[i] = NONE;
                budget--
            :: worker_enabled[i] && state[i] == SPIN && budget == 0 ->
                state[i] = SLEEP;
                in_sleeping_stack[i] = true;
                counted_sleeping[i] = true;
                sleeping_count++
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Reload credits after publishing sleepers, then wake as many active
         * sleepers as are needed for the remaining work. */
        searching = 0;
        i = 0;
        do
        :: i < N ->
            if
            :: worker_enabled[i] && state[i] == NONE -> searching++
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;
        if
        :: activation_credits > searching -> budget = activation_credits - searching
        :: else -> budget = 0
        fi;

        i = 0;
        do
        :: i < N ->
            if
            :: worker_enabled[i] && state[i] == SLEEP && budget > 0 ->
                state[i] = NONE;
                in_sleeping_stack[i] = false;
                counted_sleeping[i] = false;
                sleeping_count--;
                budget--
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        check_safety();
        i = 0;
        do
        :: i < N ->
            assert(!counted_sleeping[i] || (worker_enabled[i] && state[i] == SLEEP));
            assert(worker_enabled[i] || state[i] == SLEEP);
            i++
        :: else -> break
        od
    od
}

#define target_one (suggested_thread_count == 1)
#define target_max (suggested_thread_count == N)
#define reconciled_one (thread_count == 1 && active_count == 1 && reductions == 0)
#define reconciled_max (thread_count == N && active_count == N && reductions == 0)
ltl live_reconcile_stable {
    ((<>[] target_one) ->
        ((<>[] reconciled_one) && ([]<> work_epoch) && ([]<> !work_epoch))) &&
    ((<>[] target_max) ->
        ((<>[] reconciled_max) && ([]<> work_epoch) && ([]<> !work_epoch)))
}

init {
    atomic {
        byte i = 0;
        do
        :: i < N ->
            state[i] = WORK;
            worker_enabled[i] = true;
            run Worker(i);
            i++
        :: else -> break
        od;
        i = 0;
        do
        :: i < PRODUCERS ->
            run Producer();
            i++
        :: else -> break
        od;
        run Controller();
        run Waker()
    }
}
