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
bool active_mask[N];
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

        /* thread_count is the target accepted by the previous waker pass,
         * while active_count is the physical ActiveWorkers size. Claimed and
         * unclaimed reduction tokens account for their temporary difference. */
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

            /* Any growth left after cancelling reductions must reactivate a
             * concrete inactive sleeper. Reactivation does not unpark it. */
            i = 0;
            do
            :: i < N && delta > 0 ->
                if
                :: !active_mask[i] && in_sleeping_stack[i] && state[i] == SLEEP ->
                    atomic {
                        active_mask[i] = true;
                        active_count++;
                        counted_sleeping[i] = true;
                        previous_sleeping_count++;
                        delta--
                    }
                :: else -> skip
                fi;
                i++
            :: else -> break
            od;
            assert(delta == 0)

        :: desired < thread_count ->
            delta = thread_count - desired;
            if
            :: taken_tokens_to_wakeup < delta -> converted = taken_tokens_to_wakeup
            :: else -> converted = delta
            fi;
            taken_tokens_to_wakeup = taken_tokens_to_wakeup - converted;
            taken_tokens_to_sleep = taken_tokens_to_sleep + converted;
            delta = delta - converted;
            remaining_reductions = remaining_reductions + delta

        :: else -> skip
        fi;
        thread_count = desired;

        activations = activation_credits;

        /* Resolve every active worker once. activations is a local budget: a
         * NONE worker, or a worker which leaves SPIN by itself, consumes one
         * unit without reserving the corresponding queue item. */
        i = 0;
        do
        :: i < N ->
            if
            :: active_mask[i] && state[i] == NONE ->
                if
                :: activations > 0 -> activations--
                :: else -> skip
                fi

            :: active_mask[i] && state[i] == SPIN ->
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
                            in_sleeping_stack[i] = true;
                            counted_sleeping[i] = true;
                            previous_sleeping_count++
                        :: else ->
                            assert(state[i] == NONE || state[i] == WORK)
                        fi
                    }
                fi

            :: active_mask[i] && state[i] == BLOCKING ->
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
                            in_sleeping_stack[i] = true;
                            counted_sleeping[i] = true;
                            previous_sleeping_count++;
                            taken_tokens_to_wakeup--
                        }
                    fi

                :: taken_tokens_to_wakeup == 0 ->
                    assert(taken_tokens_to_sleep > 0);
                    atomic {
                        state[i] = SLEEP;
                        in_sleeping_stack[i] = true;
                        counted_sleeping[i] = false;
                        active_mask[i] = false;
                        active_count--;
                        taken_tokens_to_sleep--
                    }
                fi

            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Only the waker mutates SLEEP. Waking therefore needs no CAS, but it
         * must update stack membership, the local count and activation budget
         * together with the state transition. */
        i = 0;
        do
        :: activations > 0 && previous_sleeping_count > 0 ->
            found = false;
            i = 0;
            do
            :: i < N && !found ->
                if
                :: active_mask[i] && in_sleeping_stack[i] && state[i] == SLEEP ->
                    atomic {
                        state[i] = NONE;
                        in_sleeping_stack[i] = false;
                        counted_sleeping[i] = false;
                        previous_sleeping_count--;
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
        i = 0;
        do
        :: i < N ->
            assert(!counted_sleeping[i]
                || (active_mask[i] && state[i] == SLEEP));
            assert(active_mask[i] || state[i] == SLEEP);
            if
            :: !active_mask[i] -> assert(in_sleeping_stack[i])
            :: else -> skip
            fi;
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
            active_mask[i] = true;
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
