/*
 * Model of a BasicExecutorPool where a worker temporarily owns the waker.
 * worker_state is the only public per-worker atomic. NEED_FROM_* preserves
 * the logical source state during election. The owner keeps that state in a
 * private resume_state while its public state is WAKER.
 */

#define N 2
#define PRODUCERS 1
#define CONTROLLER_ROUNDS 2
#define MAX_QUEUE N

#define NONE 0
#define SPIN 1
#define SLEEP 2
#define WORK 3
#define BLOCKING 4
#define NEED 5
#define NEED_FROM_SPIN 6
#define NEED_FROM_SLEEP 7
#define NEED_FROM_BLOCKING 8
#define WAKER 9

#define IS_NEED(s) ((s) == NEED || (s) == NEED_FROM_SPIN || \
    (s) == NEED_FROM_SLEEP || (s) == NEED_FROM_BLOCKING)
#define IS_SPIN(s) ((s) == SPIN || (s) == NEED_FROM_SPIN)
#define IS_SLEEP(s) ((s) == SLEEP || (s) == NEED_FROM_SLEEP)
#define IS_BLOCKING(s) ((s) == BLOCKING || (s) == NEED_FROM_BLOCKING)
#define IS_PARKED(s) ((s) == SLEEP || (s) == BLOCKING)
#define ALL_WORKERS_PARKED \
    (worker_parked[0] && IS_PARKED(worker_state[0]) && \
        worker_parked[1] && IS_PARKED(worker_state[1]))
#define ALL_WORKERS_SLEEPING \
    (worker_parked[0] && worker_state[0] == SLEEP && \
        worker_parked[1] && worker_state[1] == SLEEP)

#define INVALID_WAKER N
#define REDUCTION_WAKER_BIT 4
#define REDUCTION_COUNT_MASK (REDUCTION_WAKER_BIT - 1)

byte worker_state[N];
bool worker_parked[N];
byte activation_credits = 0;
byte queued_activations = 0;
byte sleeping_count = 0;
byte awake_workers = N;
byte sleep_workers = 0;

byte suggested_thread_count = N;
byte thread_count = N;
byte reductions = 0;
byte previous_reductions = 0;
byte taken_tokens_to_sleep = 0;
byte taken_tokens_to_wakeup = 0;

byte waker_worker_id = INVALID_WAKER;
bool waker_pending = false;
bool producer_epoch = false;
bool controller_epoch = false;
bool producer_done = false;
bool controller_done = false;

/* Single persistent waker scratch space. */
byte waker_i = 0;
byte waker_budget = 0;
byte waker_previous_activations = 0;
byte waker_previous_sleeping = 0;
byte waker_desired = 0;
byte waker_delta = 0;
byte waker_converted = 0;
byte waker_remaining_reductions = 0;

/* One non-blocking attempt to notify or select a waker. */
inline try_request_waker(candidate, notified, require_sleepers) {
    if
    :: atomic { waker_worker_id != INVALID_WAKER ->
        notified = true
    }
    :: atomic { waker_worker_id == INVALID_WAKER && (!require_sleepers || sleeping_count > 0) ->
        notified = false;
        candidate = 0;
    }
        do
        :: atomic { true ->
            if
            :: worker_state[candidate] == WAKER || IS_NEED(worker_state[candidate]) ->
                notified = true
            :: worker_state[candidate] == SPIN ->
                worker_state[candidate] = NEED_FROM_SPIN;
                notified = true
            :: worker_state[candidate] == SLEEP ->
                worker_state[candidate] = NEED_FROM_SLEEP;
                notified = true
            :: worker_state[candidate] == BLOCKING ->
                worker_state[candidate] = NEED_FROM_BLOCKING;
                notified = true
            :: else -> skip
            fi
            if
            :: candidate + 1 == N || notified ->
                candidate = 0;
                break;
            :: else ->
                candidate++
            fi
        };
        od
    :: atomic { else ->
        notified = false;
    }
    fi
}

/* Reconcile the resize counters captured by one waker pass. */
inline reconcile_thread_count() {
    waker_desired = suggested_thread_count;
    atomic {
        if
        :: waker_desired < thread_count ->
            waker_delta = thread_count - waker_desired;
            if
            :: taken_tokens_to_wakeup < waker_delta ->
                waker_converted = taken_tokens_to_wakeup
            :: else -> waker_converted = waker_delta
            fi;
            taken_tokens_to_wakeup = taken_tokens_to_wakeup - waker_converted;
            taken_tokens_to_sleep = taken_tokens_to_sleep + waker_converted;
            waker_delta = waker_delta - waker_converted;
            if
            :: waker_previous_sleeping < waker_delta ->
                waker_converted = waker_previous_sleeping
            :: else -> waker_converted = waker_delta
            fi;
            waker_previous_sleeping = waker_previous_sleeping - waker_converted;
            waker_delta = waker_delta - waker_converted;
            assert(waker_remaining_reductions + waker_delta <= N);
            waker_remaining_reductions = waker_remaining_reductions + waker_delta;
            thread_count = waker_desired
        :: waker_desired > thread_count ->
            waker_delta = waker_desired - thread_count;
            if
            :: taken_tokens_to_sleep < waker_delta ->
                waker_converted = taken_tokens_to_sleep
            :: else -> waker_converted = waker_delta
            fi;
            taken_tokens_to_sleep = taken_tokens_to_sleep - waker_converted;
            taken_tokens_to_wakeup = taken_tokens_to_wakeup + waker_converted;
            waker_delta = waker_delta - waker_converted;
            if
            :: waker_remaining_reductions < waker_delta ->
                waker_converted = waker_remaining_reductions
            :: else -> waker_converted = waker_delta
            fi;
            waker_remaining_reductions = waker_remaining_reductions - waker_converted;
            waker_delta = waker_delta - waker_converted;
            assert(waker_previous_sleeping + waker_delta <= N);
            waker_previous_sleeping = waker_previous_sleeping + waker_delta;
            thread_count = waker_desired
        :: else -> skip
        fi;
        waker_delta = 0;
        waker_converted = 0
        waker_desired = 0
    }
}

/* Observe each non-owner worker once, as WakerLoop does in C++. */
inline reconcile_workers(id, resume_state) {
    waker_i = 0;
    do
    :: atomic { true ->
        if
        :: (waker_i != id && (worker_state[waker_i] == NEED || worker_state[waker_i] == NONE))->
            worker_state[waker_i] = NONE;
            if
            :: waker_budget > 0 -> waker_budget--
            :: else -> skip
            fi
        :: (waker_i == id && (resume_state == NEED || resume_state == NONE)) ->
            resume_state = NONE
        :: waker_i != id && IS_SPIN(worker_state[waker_i]) ->
            if
            :: waker_remaining_reductions > 0 ->
                worker_state[waker_i] = SLEEP;
                waker_remaining_reductions--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            :: waker_remaining_reductions == 0 && waker_budget > 0 ->
                worker_state[waker_i] = NONE;
                waker_budget--
            :: waker_remaining_reductions == 0 && waker_budget == 0 ->
                worker_state[waker_i] = SLEEP;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++;
                waker_previous_sleeping++
            fi
        :: waker_i == id && IS_SPIN(resume_state) ->
            if
            :: waker_remaining_reductions > 0 ->
                resume_state = SLEEP;
                waker_remaining_reductions--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            :: waker_remaining_reductions == 0 && waker_budget > 0 ->
                resume_state = NONE;
            :: waker_remaining_reductions == 0 && waker_budget == 0 ->
                resume_state = SLEEP;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++;
                waker_previous_sleeping++
            fi
        :: waker_i != id && IS_BLOCKING(worker_state[waker_i]) ->
            if
            :: taken_tokens_to_wakeup > 0 && waker_budget > 0 ->
                worker_state[waker_i] = NONE;
                taken_tokens_to_wakeup--;
                waker_budget--
            :: taken_tokens_to_wakeup > 0 && waker_budget == 0 ->
                worker_state[waker_i] = SLEEP;
                taken_tokens_to_wakeup--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++;
                waker_previous_sleeping++
            :: taken_tokens_to_wakeup == 0 && taken_tokens_to_sleep > 0 ->
                worker_state[waker_i] = SLEEP;
                taken_tokens_to_sleep--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            fi
        :: waker_i == id && IS_BLOCKING(resume_state) ->
            assert(taken_tokens_to_wakeup != 0 || taken_tokens_to_sleep != 0);
            if
            :: taken_tokens_to_wakeup > 0 && waker_budget > 0 ->
                resume_state = NONE;
                taken_tokens_to_wakeup--;
            :: taken_tokens_to_wakeup > 0 && waker_budget == 0 ->
                resume_state = SLEEP;
                taken_tokens_to_wakeup--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++;
                waker_previous_sleeping++
            :: taken_tokens_to_wakeup == 0 && taken_tokens_to_sleep > 0 ->
                resume_state = SLEEP;
                taken_tokens_to_sleep--;
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            fi
        :: else -> skip
        fi
        if
        :: waker_i < N - 1 ->
            waker_i++;
        :: else ->
            waker_i = 0;
            break;
        fi
    }
    od
}

inline wake_sleeping_workers(id, resume_state) {
    do
    :: atomic { waker_budget > 0 && waker_previous_sleeping > 0 ->
        waker_i = 0;
        do
        :: waker_i < N && waker_budget > 0 && waker_previous_sleeping > 0->
            if
            :: waker_i != id && IS_SLEEP(worker_state[waker_i]) ||
                   waker_i == id && IS_SLEEP(resume_state) ->
                if
                :: waker_i != id -> worker_state[waker_i] = NONE;
                :: else -> resume_state = NONE;
                fi
                assert(sleep_workers > 0);
                assert(waker_previous_sleeping > 0);
                sleep_workers--;
                waker_previous_sleeping--;
                awake_workers++;
                if
                :: waker_i != id -> waker_budget--;
                :: else ->
                fi
            :: else -> waker_i++
            fi
        :: else -> break
        od;
        waker_i = 0;
        if
        :: waker_budget == 0 || waker_previous_sleeping == 0 ->
            waker_budget = 0;
            break
        :: else -> skip
        fi
    }
    :: atomic { else ->
        waker_budget = 0
        break
    }
    od
}

/* One call corresponds to one C++ WakerLoop invocation. */
inline run_waker_pass(id, resume_state) {
    atomic { // exchange sleeping_count
        assert(waker_i == 0);
        assert(waker_budget == 0);
        assert(waker_previous_activations == 0);
        assert(waker_previous_sleeping == 0);
        assert(waker_desired == 0);
        assert(waker_delta == 0);
        assert(waker_converted == 0);
        assert(waker_remaining_reductions == 0);
        waker_previous_sleeping = sleeping_count;
        sleeping_count = 0
    };
    waker_pending = false
    atomic { // exchange reductions
        waker_remaining_reductions = reductions & REDUCTION_COUNT_MASK;
        reductions = 0;
        assert(previous_reductions >= waker_remaining_reductions);
        assert(taken_tokens_to_sleep + taken_tokens_to_wakeup +
            previous_reductions - waker_remaining_reductions <= N);
        taken_tokens_to_sleep = taken_tokens_to_sleep +
            previous_reductions - waker_remaining_reductions;
        previous_reductions = 0
    };
    reconcile_thread_count();
    atomic {
        waker_previous_activations = activation_credits;
        waker_budget = waker_previous_activations;
    }
    reconcile_workers(id, resume_state);
    atomic { // cas reductions
        assert((reductions & REDUCTION_COUNT_MASK) == 0);
        reductions = waker_remaining_reductions; // reset REDUCTION_WAKER_BIT (because we have waker_pending)
        previous_reductions = waker_remaining_reductions;
        waker_remaining_reductions = 0
    };
    wake_sleeping_workers(id, resume_state);
    atomic {
        sleeping_count = waker_previous_sleeping;
        waker_previous_sleeping = 0;
    }
    atomic {
        if
        :: activation_credits > waker_previous_activations ->
            waker_pending = true
        :: else -> skip
        fi
        waker_previous_activations = 0;
        waker_delta = 0;
        waker_converted = 0;
    }
}

/* The C++ RunWaker loop: elect/forward, run a pass, then publish resume. */
inline run_waker(id, resume_state) {
    run_waker_again_label:
    do
    :: atomic { waker_worker_id == INVALID_WAKER ->
        waker_worker_id = id;
    }
        run_waker_acquire_save_state_label:
        atomic {
            if
            :: IS_SLEEP(worker_state[id]) -> resume_state = SLEEP
            :: IS_BLOCKING(worker_state[id]) -> resume_state = BLOCKING
            :: worker_state[id] == NONE || worker_state[id] == NEED -> resume_state = NONE
            :: IS_SPIN(worker_state[id]) -> resume_state = SPIN
            :: else -> skip
            fi;
            worker_state[id] = WAKER
        }
        run_waker_pass_label:
        run_waker_pass(id, resume_state);
        atomic {
            if
            :: waker_pending -> goto run_waker_pass_label
            :: else -> skip
            fi
        }
        worker_state[id] = resume_state;
        if
        :: waker_pending -> goto run_waker_acquire_save_state_label
        :: else -> skip
        fi
        waker_worker_id = INVALID_WAKER
        atomic {
            if
            :: waker_pending -> goto run_waker_again_label
            :: else -> break
            fi
        }
    :: atomic { waker_worker_id == id ->
        goto run_waker_acquire_save_state_label
    }
    :: waker_worker_id != INVALID_WAKER && waker_worker_id != id ->
        atomic { 
            if
            :: !IS_NEED(worker_state[id]) ->
                break
            :: worker_state[id] == NEED_FROM_SLEEP ->
                worker_state[id] = SLEEP;
            :: worker_state[id] == NEED_FROM_BLOCKING ->
                worker_state[id] = BLOCKING
            :: worker_state[id] == NEED_FROM_SPIN ->
                worker_state[id] = SPIN
            :: worker_state[id] == NEED ->
                worker_state[id] = NONE
            fi
        }
    od;
}

proctype Worker(byte id) {
    byte resume_state = NONE;

worker_iteration:
    /* Returning an activation to the executor eventually re-enters
     * GetReadyActivationWaker and clears WORK. */
    if
    :: worker_state[id] == WORK ->
        atomic {
            worker_state[id] == WORK ->
            worker_state[id] = NONE;
        }
    :: else -> skip
    fi;

check_blocking:
    /* Match the C++ order: consume a pending persistent request or a
     * reduction before attempting to pop an activation. */
    if
    :: atomic {
        reductions & REDUCTION_WAKER_BIT ->
        reductions = reductions & REDUCTION_COUNT_MASK
    };
        worker_state[id] = NEED
        goto settle_waker_state
    :: atomic {
        reductions > 0 && reductions < REDUCTION_WAKER_BIT ->
        reductions--
    };
        worker_state[id] = BLOCKING
        if
        :: atomic { !waker_pending ->
            waker_pending = true;
        }
            atomic {
                if
                :: worker_state[id] == BLOCKING -> worker_state[id] = NEED_FROM_BLOCKING
                :: worker_state[id] == SLEEP -> worker_state[id] = NEED_FROM_SLEEP
                :: worker_state[id] == NONE -> worker_state[id] = NEED
                :: else -> skip
                fi
            }
        :: else -> skip;
        fi
        goto settle_waker_state
    :: reductions == 0 -> skip
    fi;

pop_activation:
    if
    :: atomic { queued_activations > 0 ->
        queued_activations--;
    }
        atomic {
            assert(activation_credits > 0);
            activation_credits--;
        }
        atomic {
            worker_state[id] = WORK;
            goto worker_iteration
        }
    :: queued_activations == 0 -> skip
    fi;

    /* A producer publishes the credit before the queue item. */
    
    if
    :: activation_credits > 0 ->
        if
        :: queued_activations != 0 || reductions != 0 -> skip // endless loop with activation_credits > 0 && queued_activations == 0 && reductions == 0
        :: activation_credits == 0 -> skip
        fi
        goto worker_iteration
    :: activation_credits == 0 ->
        worker_state[id] = SPIN;
        if
        :: atomic { !waker_pending ->
            waker_pending = true;
        }
            atomic {
                if
                :: worker_state[id] == SPIN ->
                    worker_state[id] = NEED_FROM_SPIN
                :: worker_state[id] == SLEEP ->
                    worker_state[id] = NEED_FROM_SLEEP
                :: worker_state[id] == NONE ->
                    worker_state[id] = NEED
                :: IS_NEED(worker_state[id]) -> skip
                fi
            }
        :: else -> skip;
        fi
    fi

settle_waker_state:
    /* This is settleWakerState() followed by RunWaker() in C++. */
    if
    :: IS_NEED(worker_state[id]) ->
        run_waker(id, resume_state);
        goto settle_waker_state
    :: worker_state[id] == SPIN ->
        if
        :: activation_credits > 0 ->
            atomic {
                if
                :: worker_state[id] == SPIN ->
                    worker_state[id] = NONE
                    goto settle_waker_state
                :: IS_NEED(worker_state[id]) -> goto settle_waker_state
                :: worker_state[id] == SLEEP -> goto sleep_state_label
                :: worker_state[id] == NONE -> goto worker_iteration
                fi
            }
        :: activation_credits == 0 ->
            worker_state[id] != SPIN || activation_credits > 0 ||
                (reductions & REDUCTION_WAKER_BIT);
            if
            :: reductions & REDUCTION_WAKER_BIT ->
                atomic {
                    if
                    :: worker_state[id] == SPIN ->
                        worker_state[id] = NONE
                    :: worker_state[id] == SLEEP ->
                        goto sleep_state_label
                    :: IS_NEED(worker_state[id]) ->
                        goto settle_waker_state
                    :: else -> skip
                    fi
                };
                goto check_blocking
            :: else -> goto settle_waker_state
            fi
        fi
    :: worker_state[id] == SLEEP || worker_state[id] == BLOCKING ->
        /* Park until the waker changes the public state. */
        sleep_state_label:
        worker_parked[id] = true;
        end_worker_parked:
        worker_state[id] != SLEEP && worker_state[id] != BLOCKING;
        worker_parked[id] = false;
        goto settle_waker_state
    :: worker_state[id] == NONE -> skip
    fi;

    goto worker_iteration
}

proctype Producer() {
    byte i;
    bool done;
    produser_iteration:
    do
    :: atomic { true -> producer_done = true }; break
    :: true ->
        atomic { activation_credits < MAX_QUEUE ->
            activation_credits++
            producer_epoch = !producer_epoch
        };
        atomic {
            queued_activations++;
        };
        if
        :: atomic { sleeping_count == 0 -> goto produser_iteration }
        :: else ->
            atomic {
                if
                :: !waker_pending ->
                    waker_pending = true;
                :: else -> goto produser_iteration
                fi
            }
        fi
        atomic {
            if
            :: waker_worker_id != INVALID_WAKER -> goto produser_iteration
            :: else -> skip
            fi
        }

        producer_wake_up_loop:
        if
        :: sleeping_count == 0 ->
            atomic {
                if
                :: reductions & REDUCTION_WAKER_BIT -> goto produser_iteration
                :: else -> reductions = reductions | REDUCTION_WAKER_BIT
                fi
            }
            atomic {
                if
                :: sleeping_count == 0 -> goto produser_iteration
                :: else -> goto producer_wake_up_iteration
                fi
            }
        :: else ->
            producer_wake_up_iteration:
            try_request_waker(i, done, true);
            atomic {
                if
                :: done -> goto produser_iteration
                :: else -> goto producer_wake_up_loop
                fi
            }
        fi
    od
}

proctype Controller() {
    byte i;
    bool notified;
    bool bit_setted = false;
    controller_iteration:
    do
    :: atomic { true -> controller_done = true }; break
    :: suggested_thread_count == thread_count || !waker_pending ->
        atomic {
            if
            :: suggested_thread_count == N ->
                suggested_thread_count = 1;
                controller_epoch = !controller_epoch
            :: suggested_thread_count == 1 ->
                suggested_thread_count = N;
                controller_epoch = !controller_epoch
            fi;
        };
        atomic {
            if
            :: !waker_pending ->
                waker_pending = true;
            :: else -> goto controller_iteration
            fi
        };

        atomic {
            if
            :: suggested_thread_count == N -> goto controller_wake_up
            :: else ->
                if
                :: sleeping_count == 0 -> goto controller_reduciton_bit
                :: else -> goto controller_wake_up
                fi
            fi
        }

        controller_reduciton_bit:
        atomic {
            if
            :: reductions & REDUCTION_WAKER_BIT -> goto controller_iteration
            :: else ->
                reductions = reductions | REDUCTION_WAKER_BIT
                bit_setted = true
            fi
        }
        atomic {
            if
            :: sleeping_count == 0 ->
                bit_setted = false
                goto controller_iteration
            :: else -> goto controller_wake_up
            fi
        }

        controller_wake_up:
        try_request_waker(i, notified, false);

        if
        :: atomic { !notified && !bit_setted ->
            i = 0;
            notified = false;
            bit_setted = false;
            if
            :: reductions & REDUCTION_WAKER_BIT ->
                goto controller_iteration
            :: else -> reductions = reductions | REDUCTION_WAKER_BIT
            fi
        }
        :: atomic { else ->
            i = 0;
            notified = false;
            bit_setted = false;
        }
        fi
    od
}

/* Once external changes stop, a globally parked pool is a valid end state
 * only when neither a queued activation nor its published credit remains. */
proctype FinalStateChecker() {
    producer_done && controller_done && ALL_WORKERS_PARKED;
    atomic {
        assert(queued_activations == 0);
        assert(activation_credits == 0)
    }
}

#define RESIZE_RECONCILES_WITH_EPOCHS(p, c) \
    ([] ((suggested_thread_count != thread_count && \
            producer_epoch == p && controller_epoch == c) -> \
        <> (suggested_thread_count == thread_count || \
            producer_epoch != p || controller_epoch != c)))

#define QUEUE_DECREASES_WITH_EPOCHS(q, p, c) \
    ([] ((queued_activations == q && \
            producer_epoch == p && controller_epoch == c) -> \
        <> (queued_activations < q || \
            producer_epoch != p || controller_epoch != c)))

#define WAKER_PENDING_CLEARS_WITH_PRODUCER_EPOCH(p) \
    ([] ((waker_pending && producer_epoch == p) -> \
        <> (!waker_pending || producer_epoch != p)))

#define ZERO_ACTIVATIONS_SETTLES_WITH_EPOCHS(p, c) \
    ([] ((activation_credits == 0 && producer_epoch == p && \
            controller_epoch == c) -> \
        <> (ALL_WORKERS_SLEEPING || activation_credits != 0 || \
            producer_epoch != p || controller_epoch != c)))

ltl live_resize_reconciles {
    RESIZE_RECONCILES_WITH_EPOCHS(false, false) &&
    RESIZE_RECONCILES_WITH_EPOCHS(false, true) &&
    RESIZE_RECONCILES_WITH_EPOCHS(true, false) &&
    RESIZE_RECONCILES_WITH_EPOCHS(true, true)
}

ltl live_queue_changes {
    QUEUE_DECREASES_WITH_EPOCHS(1, false, false) &&
    QUEUE_DECREASES_WITH_EPOCHS(1, false, true) &&
    QUEUE_DECREASES_WITH_EPOCHS(1, true, false) &&
    QUEUE_DECREASES_WITH_EPOCHS(1, true, true) &&
    QUEUE_DECREASES_WITH_EPOCHS(2, false, false) &&
    QUEUE_DECREASES_WITH_EPOCHS(2, false, true) &&
    QUEUE_DECREASES_WITH_EPOCHS(2, true, false) &&
    QUEUE_DECREASES_WITH_EPOCHS(2, true, true)
}

ltl live_waker_pending_clears {
    WAKER_PENDING_CLEARS_WITH_PRODUCER_EPOCH(false) &&
    WAKER_PENDING_CLEARS_WITH_PRODUCER_EPOCH(true)
}

ltl live_zero_activations_sleep {
    ZERO_ACTIVATIONS_SETTLES_WITH_EPOCHS(false, false) &&
    ZERO_ACTIVATIONS_SETTLES_WITH_EPOCHS(false, true) &&
    ZERO_ACTIVATIONS_SETTLES_WITH_EPOCHS(true, false) &&
    ZERO_ACTIVATIONS_SETTLES_WITH_EPOCHS(true, true)
}

init {
    atomic {
        byte i = 0;
        do
        :: i < N ->
            worker_state[i] = WORK;
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
        run FinalStateChecker()
    }
}
