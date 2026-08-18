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
byte awake_workers = N;
byte sleeping_count = 0;
byte reductions = 0;
byte activation_credits = 0;
byte queued_activations = 0;
bool waker_pending = false;

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
        assert(awake_workers <= N);
        assert(awake_workers + sleeping_count <= N);
        assert(reductions <= N);
        assert(activation_credits <= MAX_QUEUE);
        assert(queued_activations <= activation_credits)
    }
}

proctype Worker(byte id) {
    do
    :: state[id] == WORK ->
        state[id] = NONE

    :: state[id] == NONE ->
        if
        :: atomic {
            reductions > 0 ->
            reductions--
        };
            state[id] = BLOCKING;
            request_waker();

            /* WaitForWaker parks while the state is BLOCKING or SLEEP. Once
             * the waker releases this worker, continue with the queue rather
             * than attempting to claim another reduction. */
            state[id] != BLOCKING && state[id] != SLEEP

        /* Reduction has priority over looking in the activation queue. Once
         * the worker observes no reduction, the waker may publish one before
         * the worker completes Pop, SetWork and the credit decrement. */
        :: reductions == 0 -> skip
        fi;

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

    :: state[id] == SPIN && activation_credits > 0 ->
        atomic {
            if
            :: state[id] == SPIN && activation_credits > 0 -> state[id] = NONE
            :: else -> skip
            fi
        }

    /* BLOCKING and SLEEP are parked; only the waker changes them. */
    od
}

proctype Producer() {
    do
    :: atomic {
        activation_credits < MAX_QUEUE ->
        activation_credits++
    };
        atomic {
            queued_activations++
        };
        if
        :: sleeping_count > 0 -> request_waker()
        :: else -> skip
        fi
    od
}

proctype Controller() {
    bit counter = 0;

    do
    :: counter == 0 ->
        awake_workers == suggested_thread_count;
        check_safety();
        counter = 1

    :: counter == 1 ->
        check_safety();
        atomic {
            if
            :: suggested_thread_count == 1 -> suggested_thread_count = N
            :: suggested_thread_count == N -> suggested_thread_count = 1
            fi;
            counter = 0
        };
        request_waker()
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
    byte eligible_sleepers;
    byte previous_activations;
    byte activations;
    byte sleep_workers;
    bool found;

    do
    :: waker_pending ->
        /* SleepingCount is withdrawn while the waker recomputes it. Producers
         * which publish during this window deliberately observe zero. */
        sleeping_count = 0;

        waker_pending = false;
        desired = suggested_thread_count;

        /* Exchange the published reduction tokens. The difference from the
         * previous publication was claimed by workers which are either
         * BLOCKING already or are between the claim and that state change. */
        atomic {
            remaining_reductions = reductions;
            reductions = 0;
            assert(remaining_reductions <= N);
            assert(previous_reductions <= N);
            assert(taken_tokens_to_sleep <= N);
            assert(taken_tokens_to_wakeup <= N);
            assert(previous_reductions >= remaining_reductions);
            assert(taken_tokens_to_sleep + previous_reductions <= N);
            taken_tokens_to_sleep = (taken_tokens_to_sleep
                + previous_reductions) - remaining_reductions;
            assert(taken_tokens_to_sleep <= N);
            previous_reductions = 0
        };

        /* The waker owns all mutable values in this reconciliation block.
         * thread_count is the previously accepted target, awake_workers is
         * the number of workers outside Sleep, and sleep_workers is the total
         * number of workers in Sleep. */
        atomic {
            assert((thread_count + remaining_reductions)
                + taken_tokens_to_sleep >= awake_workers);
            eligible_sleepers = ((thread_count + remaining_reductions)
                + taken_tokens_to_sleep) - awake_workers;

            if
            :: desired > thread_count ->
                delta = desired - thread_count;

                if
                :: taken_tokens_to_sleep < delta -> converted = taken_tokens_to_sleep
                :: else -> converted = delta
                fi;
                taken_tokens_to_sleep = taken_tokens_to_sleep - converted;
                assert(taken_tokens_to_wakeup + converted <= N);
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
                delta = 0

            :: desired < thread_count ->
                delta = thread_count - desired;
                if
                :: taken_tokens_to_wakeup < delta -> converted = taken_tokens_to_wakeup
                :: else -> converted = delta
                fi;
                taken_tokens_to_wakeup = taken_tokens_to_wakeup - converted;
                assert(taken_tokens_to_sleep + converted <= N);
                taken_tokens_to_sleep = taken_tokens_to_sleep + converted;
                delta = delta - converted;

                /* Retire already sleeping slots before asking an awake worker
                 * to claim a new reduction token. */
                if
                :: eligible_sleepers < delta -> converted = eligible_sleepers
                :: else -> converted = delta
                fi;
                delta = delta - converted;
                assert(remaining_reductions + delta <= N);
                remaining_reductions = remaining_reductions + delta;
                delta = 0

            :: else -> skip
            fi;
            thread_count = desired;
            assert((thread_count + remaining_reductions)
                + taken_tokens_to_sleep >= awake_workers);
            assert(((thread_count + remaining_reductions)
                + taken_tokens_to_sleep) - awake_workers <= sleep_workers);
            assert(taken_tokens_to_sleep <= N);
            assert(taken_tokens_to_wakeup <= N);
            assert(remaining_reductions <= N);
            assert(sleep_workers <= N);
            assert(awake_workers <= N);
            converted = 0;
            eligible_sleepers = 0
        };

        previous_activations = activation_credits;
        activations = previous_activations;

        /* Resolve workers which claimed reductions published by an earlier
         * pass. Reductions for the new target are not visible yet, so a
         * worker which becomes BLOCKING after this loop is deferred until the
         * next waker pass, matching the ordering in WakerLoop(). */
        i = 0;
        do
        :: i < N ->
            if
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
                            assert(awake_workers > 0);
                            assert(sleep_workers < N);
                            state[i] = SLEEP;
                            awake_workers--;
                            sleep_workers++;
                            taken_tokens_to_wakeup--
                        }
                    fi

                :: taken_tokens_to_wakeup == 0 ->
                    assert(taken_tokens_to_sleep > 0);
                    atomic {
                        assert(awake_workers > 0);
                        assert(sleep_workers < N);
                        state[i] = SLEEP;
                        awake_workers--;
                        sleep_workers++;
                        taken_tokens_to_sleep--
                    }
                fi

            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* C++ publishes CheckToSleepWorkers before it computes the activation
         * budget and scans SPIN workers. Claims made from this publication are
         * intentionally handled by the next pass. */
        atomic {
            reductions = remaining_reductions;
            previous_reductions = remaining_reductions
        };

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
                        :: state[i] == SPIN ->
                            state[i] = NONE;
                            activations--
                        :: else ->
                            assert(state[i] == NONE || state[i] == WORK || state[i] == BLOCKING);
                            if
                            :: state[i] == NONE || state[i] == WORK -> activations--
                            :: state[i] == BLOCKING -> skip
                            fi
                        fi
                    }

                :: activations == 0 ->
                    atomic {
                        if
                        :: state[i] == SPIN ->
                            assert(awake_workers > 0);
                            assert(sleep_workers < N);
                            state[i] = SLEEP;
                            awake_workers--;
                            sleep_workers++
                        :: else ->
                            assert(state[i] == NONE || state[i] == WORK || state[i] == BLOCKING)
                        fi
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
        :: activations > 0 && ((thread_count + remaining_reductions)
                + taken_tokens_to_sleep) > awake_workers ->
            atomic {
                found = false;
                i = 0;
                do
                :: i < N && !found ->
                    if
                    :: state[i] == SLEEP ->
                        assert(awake_workers < N);
                        assert(sleep_workers > 0);
                        state[i] = NONE;
                        awake_workers++;
                        sleep_workers--;
                        activations--;
                        found = true
                    :: else -> skip
                    fi;
                    i++
                :: else -> break
                od;
                assert(found)
            }
        :: else -> break
        od;

        /* Recompute the number of eligible sleepers. Reductions were already
         * published before the worker scan; previous_reductions remains the
         * baseline used to detect claims on the next pass. */
        atomic {
            assert((thread_count + remaining_reductions)
                + taken_tokens_to_sleep >= awake_workers);
            eligible_sleepers = ((thread_count + remaining_reductions)
                + taken_tokens_to_sleep) - awake_workers;
            assert(eligible_sleepers <= sleep_workers)
        };
        atomic {
            sleeping_count = eligible_sleepers;
            eligible_sleepers = 0
        };

        if
        :: activation_credits > previous_activations -> request_waker()
        :: else -> skip
        fi;

        check_safety();
        atomic {
            assert(awake_workers == N - sleep_workers)
        }
    od
}

/* With MAX_QUEUE == 2, express queue progress directly without an observer
 * epoch: one queued activation must eventually become zero or two, while a
 * full queue must eventually lose an activation. */
ltl live_queue_changes {
    ([] (queued_activations == 1 ->
        <> (queued_activations == 0 || queued_activations == 2))) &&
    ([] (queued_activations == 2 -> <> (queued_activations == 1)))
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
