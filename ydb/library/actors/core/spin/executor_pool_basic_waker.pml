/*
 * Finite-state model of dynamic thread-count changes in TBasicExecutorPool
 * waker. All processes run indefinitely.
 *
 * Deliberately modeled:
 *  - SuggestedThreadCount updates by the controller;
 *  - ThreadCount and reduction-token updates owned by the waker;
 *  - workers preferring a reduction request over taking an activation;
 *  - independent infinite producers keeping the queue bounded by MAX_QUEUE;
 *  - None -> Blocking -> Sleep and immediate parking;
 *  - SleepingStack abstracted as the set of all workers in Sleep;
 *  - SleepingCount as the number of sleepers currently eligible for wakeup;
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
    :: state[id] == WORK ->
        atomic {
            state[id] = NONE;
            work_epoch = !work_epoch
        }

    :: state[id] == NONE ->
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
                    assert(activation_credits > 0);
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
    byte delta;
    byte converted;
    byte remaining_reductions;
    byte previous_reductions;
    byte taken_tokens_to_sleep;
    byte taken_tokens_to_wakeup;
    byte previous_sleeping_count;
    byte activations;
    byte sleep_workers;
    bool found;

    do
    :: waker_pending ->
        /* SleepingCount is temporarily owned by the waker. Producers which
         * publish during this window deliberately observe zero. */
        atomic {
            previous_sleeping_count = sleeping_count;
            sleeping_count = 0
        };

        waker_pending = false;
        desired = suggested_thread_count;

        /* Exchange the published reduction tokens. The difference from the
         * previous publication was claimed by workers which are either
         * BLOCKING already or are between the claim and that state change. */
        atomic {
            remaining_reductions = reductions;
            reductions = 0;
            assert(previous_reductions >= remaining_reductions);
            taken_tokens_to_sleep = (taken_tokens_to_sleep
                + previous_reductions) - remaining_reductions;
            previous_reductions = 0
        };

        /* The waker owns all mutable values in this reconciliation block.
         * thread_count is the previously accepted target, active_count is the
         * number of slots still eligible to execute, and sleep_workers is the
         * total number of workers in Sleep. */
        atomic {
            if
            :: desired > thread_count ->
                delta = desired - thread_count;

                if
                :: taken_tokens_to_sleep < delta -> converted = taken_tokens_to_sleep
                :: else -> converted = delta
                fi;
                taken_tokens_to_sleep = taken_tokens_to_sleep - converted;
                taken_tokens_to_wakeup = taken_tokens_to_wakeup + converted;
                delta = delta - converted;

                if
                :: remaining_reductions < delta -> converted = remaining_reductions
                :: else -> converted = delta
                fi;
                remaining_reductions = remaining_reductions - converted;
                delta = delta - converted;

                /* Growth makes existing sleepers eligible for wakeup without
                 * assigning that eligibility to concrete worker identities. */
                assert(sleep_workers >= previous_sleeping_count + delta);
                previous_sleeping_count = previous_sleeping_count + delta;
                active_count = active_count + delta;
                delta = 0

            :: desired < thread_count ->
                delta = thread_count - desired;
                if
                :: taken_tokens_to_wakeup < delta -> converted = taken_tokens_to_wakeup
                :: else -> converted = delta
                fi;
                taken_tokens_to_wakeup = taken_tokens_to_wakeup - converted;
                taken_tokens_to_sleep = taken_tokens_to_sleep + converted;
                delta = delta - converted;

                /* Retire already sleeping slots before asking an awake worker
                 * to claim a new reduction token. */
                if
                :: previous_sleeping_count < delta -> converted = previous_sleeping_count
                :: else -> converted = delta
                fi;
                previous_sleeping_count = previous_sleeping_count - converted;
                active_count = active_count - converted;
                delta = delta - converted;
                remaining_reductions = remaining_reductions + delta;
                delta = 0

            :: else -> skip
            fi;
            thread_count = desired;
            converted = 0
        };

        activations = activation_credits;

        /* Resolve every active worker once. activations is a local budget: a
         * NONE worker, or a worker which leaves SPIN by itself, consumes one
         * unit without reserving the corresponding queue item. */
        i = 0;
        do
        :: i < N ->
            if
            :: state[i] == NONE ->
                if
                :: activations > 0 -> activations--
                :: else -> skip
                fi

            :: state[i] == SPIN ->
                if
                :: activations > 0 ->
                    atomic {
                        if
                        :: state[i] == SPIN -> state[i] = NONE
                        :: else ->
                            assert(state[i] == NONE || state[i] == WORK)
                        fi
                    };
                    activations--

                :: activations == 0 ->
                    atomic {
                        if
                        :: state[i] == SPIN ->
                            state[i] = SLEEP;
                            previous_sleeping_count++;
                            sleep_workers++
                        :: else ->
                            assert(state[i] == NONE || state[i] == WORK)
                        fi
                    }
                fi

            :: state[i] == BLOCKING ->
                if
                :: taken_tokens_to_wakeup > 0 ->
                    if
                    :: activations > 0 ->
                        atomic {
                            state[i] = NONE;
                            taken_tokens_to_wakeup--
                        };
                        activations--
                    :: activations == 0 ->
                        atomic {
                            state[i] = SLEEP;
                            previous_sleeping_count++;
                            sleep_workers++;
                            taken_tokens_to_wakeup--
                        }
                    fi

                :: taken_tokens_to_wakeup == 0 ->
                    assert(taken_tokens_to_sleep > 0);
                    atomic {
                        state[i] = SLEEP;
                        active_count--;
                        sleep_workers++;
                        taken_tokens_to_sleep--
                    }
                fi

            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Only the waker mutates SLEEP. Any sleeping worker may consume an
         * eligible sleeping slot because worker identities are symmetric. */
        i = 0;
        do
        :: activations > 0 && previous_sleeping_count > 0 ->
            found = false;
            i = 0;
            do
            :: i < N && !found ->
                if
                :: state[i] == SLEEP ->
                    atomic {
                        state[i] = NONE;
                        previous_sleeping_count--;
                        sleep_workers--;
                        activations--;
                        found = true
                    }
                :: else -> skip
                fi;
                i++
            :: else -> break
            od;
            if
            :: found -> skip
            :: else -> break
            fi
        :: else -> break
        od;

        /* Publish both the remaining reductions and the baseline used to
         * detect claims on the next pass. The baseline is waker-local. */
        assert(active_count == thread_count
            + remaining_reductions + taken_tokens_to_sleep);
        atomic {
            reductions = remaining_reductions;
            previous_reductions = remaining_reductions
        };
        sleeping_count = previous_sleeping_count;

        /* Deliberately omitted for now so Spin can expose the missed-wakeup
         * race this reload is intended to close:
         *
         * previous_activations = activations; // before scanning workers
         * if (activation_credits > previous_activations)
         *     request_waker();
         */

        check_safety();
        atomic {
            assert(previous_sleeping_count <= sleep_workers);
            assert(active_count == N - sleep_workers + previous_sleeping_count)
        }
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
