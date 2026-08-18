/*
 * Finite-state model of a BasicExecutorPool where an executor worker
 * temporarily assumes the waker role. There is no dedicated waker thread.
 *
 * Deliberately modeled:
 *  - producer publication order: credit, queue item, wake request;
 *  - coalescing through waker_pending;
 *  - election through waker_worker_id;
 *  - forwarding a request with WAKER -> NEED_TO_BE_WAKER;
 *  - the completion race after the owner releases waker_worker_id;
 *  - workers entering SPIN and BLOCKING asking for reconciliation;
 *  - selecting and unparking a SPIN, BLOCKING, or SLEEP worker;
 *  - a local activation budget which does not reserve queue items;
 *  - preservation of the elected worker's intended post-waker state;
 *  - dynamic thread-count changes, including claimed and unclaimed
 *    reduction tokens overtaken by a later resize;
 *  - a waker-request bit packed with the reduction count, which delegates a
 *    missed resize request to the next worker entering the idle path.
 *
 * Abstracted away:
 *  - SleepingStack order (sleeping membership is sufficient here);
 *  - the futex itself (NEED_TO_BE_WAKER enables a parked worker);
 *  - memory-order syntax; atomic blocks mark the C++ CAS/exchange edges.
 */

#define N 2
#define PRODUCERS 1
#define PRODUCER_ROUNDS 2
#define CONTROLLER_ROUNDS 2
#define MAX_QUEUE N

#define NONE 0
#define SPIN 1
#define SLEEP 2
#define WORK 3
#define BLOCKING 4
#define NEED_TO_BE_WAKER 5
#define WAKER 6

#define INVALID_WAKER N
#define REDUCTION_WAKER_BIT 4
#define REDUCTION_COUNT_MASK (REDUCTION_WAKER_BIT - 1)

byte state[N];

/* State to publish when a worker stops participating in waker election. It
 * also preserves whether a candidate selected from SLEEP remains logically
 * counted as sleeping while its public state is NEED_TO_BE_WAKER/WAKER. */
byte resume_state[N];

byte activation_credits = 0;
byte queued_activations = 0;
byte sleeping_count = 0;
byte awake_workers = N;
byte sleep_workers = 0;

byte suggested_thread_count = N;
byte thread_count = N;
byte reductions = 0;
byte claimed_reductions = 0;
byte cancelled_claimed_reductions = 0;

byte waker_worker_id = INVALID_WAKER;
bool waker_pending = false;

/* Instrumentation state: unlike EThreadState::WAKER, this covers only the
 * interval which is allowed to mutate the persistent waker data. */
byte waker_passes = 0;

/* A worker owns these scratch variables only while waker_passes == 1.
 * Keeping one global set models the single logical waker instance even when
 * its worker owner changes, and avoids multiplying private waker state by N. */
byte waker_i = 0;
byte waker_budget = 0;
byte waker_final = NONE;
byte waker_desired = 0;
byte waker_delta = 0;
byte waker_converted = 0;
bool waker_found = false;

inline check_local_safety() {
    atomic {
        assert(waker_worker_id <= INVALID_WAKER);
        assert(waker_passes <= 1);
        assert(sleeping_count <= N);
        assert(awake_workers <= N);
        assert(sleep_workers <= N);
        assert(awake_workers + sleep_workers == N);
        assert(thread_count >= 1 && thread_count <= N);
        assert(suggested_thread_count >= 1 &&
            suggested_thread_count <= N);
        assert((reductions & REDUCTION_COUNT_MASK) <= N);
        assert(reductions < 2 * REDUCTION_WAKER_BIT);
        assert(claimed_reductions <= N);
        assert(cancelled_claimed_reductions <= claimed_reductions);
        assert(activation_credits <= MAX_QUEUE);
        assert(queued_activations <= activation_credits);
        if
        :: waker_passes == 0 ->
            assert(waker_i == 0);
            assert(waker_budget == 0);
            assert(waker_final == NONE);
            assert(waker_desired == 0);
            assert(waker_delta == 0);
            assert(waker_converted == 0);
            assert(!waker_found)
        :: else -> skip
        fi;
        if
        :: waker_worker_id < N ->
            assert(state[waker_worker_id] == WAKER ||
                state[waker_worker_id] == NEED_TO_BE_WAKER)
        :: else -> skip
        fi
    }
}

/* Publish the final state of the worker which has already released the waker
 * index. A producer may concurrently change WAKER to NEED_TO_BE_WAKER; in
 * that case the caller must return to election instead of publishing final. */
inline publish_waker_final(id, final) {
    atomic {
        if
        :: state[id] == WAKER &&
                (reductions & REDUCTION_WAKER_BIT) ->
            reductions = reductions & REDUCTION_COUNT_MASK;
            resume_state[id] = final;
            state[id] = NEED_TO_BE_WAKER
        :: state[id] == WAKER &&
                !(reductions & REDUCTION_WAKER_BIT) ->
            resume_state[id] = final;
            state[id] = final
        :: state[id] == NEED_TO_BE_WAKER -> skip
        fi
    }
}

/* Make one non-blocking attempt to request a waker pass. The owner load is
 * deliberately outside atomic: the model must preserve the completion race
 * between observing waker_worker_id and forwarding the request. */
inline try_request_waker(owner, candidate, notified, require_sleepers) {
    notified = false;
    owner = waker_worker_id;
    if
    :: owner < N ->
        atomic {
            if
            :: state[owner] == WAKER ->
                state[owner] = NEED_TO_BE_WAKER;
                notified = true
            :: state[owner] == NEED_TO_BE_WAKER ->
                notified = true
            :: else -> skip
            fi
        }

    :: owner == INVALID_WAKER ->
        if
        :: !require_sleepers || sleeping_count > 0 ->
            candidate = 0;
            do
            :: candidate < N && !notified ->
                atomic {
                    if
                    :: waker_worker_id == INVALID_WAKER &&
                            (state[candidate] == SPIN ||
                                state[candidate] == BLOCKING ||
                                state[candidate] == SLEEP) ->
                        state[candidate] = NEED_TO_BE_WAKER;
                        notified = true
                    :: else -> skip
                    fi
                };
                candidate++
            :: else -> break
            od
        :: require_sleepers && sleeping_count == 0 -> skip
        fi
    fi
}

proctype Worker(byte id) {
    byte owner;
    bool done;

    do
    :: state[id] == WORK ->
        state[id] = NONE

    :: state[id] == NONE ->
        if
        /* The controller could not find an existing waker or a parked
         * candidate. The first idle worker clears only the request bit and
         * enters election, preserving any numeric reduction count. */
        :: atomic {
            reductions & REDUCTION_WAKER_BIT ->
            reductions = reductions & REDUCTION_COUNT_MASK
        };
            resume_state[id] = NONE;
            state[id] = NEED_TO_BE_WAKER;
            atomic { waker_pending = true }

        /* A reduction claim and its ownership accounting are one abstract
         * operation. Publishing BLOCKING remains a separate transition. */
        :: atomic {
            reductions > 0 && reductions < REDUCTION_WAKER_BIT ->
            reductions--;
            claimed_reductions++
        };
            resume_state[id] = BLOCKING;
            state[id] = NEED_TO_BE_WAKER;
            atomic { waker_pending = true }

        :: reductions == 0 && suggested_thread_count == thread_count &&
                queued_activations > 0 ->
            atomic {
                queued_activations > 0 ->
                queued_activations--;
                assert(activation_credits > 0);
                activation_credits--;
                resume_state[id] = WORK;
                state[id] = WORK
            }

        :: reductions == 0 && suggested_thread_count == thread_count &&
                activation_credits == 0 ->
            resume_state[id] = SPIN;
            state[id] = NEED_TO_BE_WAKER;
            atomic { waker_pending = true }
        fi

    :: state[id] == SPIN && activation_credits > 0 ->
        atomic {
            if
            :: state[id] == SPIN && activation_credits > 0 ->
                resume_state[id] = NONE;
                state[id] = NONE
            :: else -> skip
            fi
        }

    :: state[id] == NEED_TO_BE_WAKER ->
        done = false;
        do
        :: !done ->
            owner = waker_worker_id;
            if
            :: owner == INVALID_WAKER ->
                atomic {
                    if
                    :: waker_worker_id == INVALID_WAKER &&
                            state[id] == NEED_TO_BE_WAKER ->
                        waker_worker_id = id;
                        state[id] = WAKER;
                        done = true
                    :: else -> skip
                    fi
                }

            :: owner < N && owner != id ->
                atomic {
                    if
                    :: state[owner] == WAKER ->
                        state[owner] = NEED_TO_BE_WAKER;
                        state[id] = resume_state[id];
                        done = true
                    :: state[owner] == NEED_TO_BE_WAKER ->
                        state[id] = resume_state[id];
                        done = true
                    :: else -> skip
                    fi
                }

            :: owner == id ->
                atomic {
                    if
                    :: state[id] == NEED_TO_BE_WAKER ->
                        state[id] = WAKER;
                        done = true
                    :: state[id] == WAKER ->
                        done = true
                    fi
                }
            fi
        :: else -> break
        od;
        atomic {
            assert(done);
            owner = 0;
            done = false
        }

    :: state[id] == WAKER && waker_worker_id == id ->
        atomic {
            assert(waker_passes == 0);
            waker_passes = 1;
            waker_pending = false;
            if
            :: reductions & REDUCTION_WAKER_BIT ->
                reductions = reductions & REDUCTION_COUNT_MASK
            :: else -> skip
            fi
        };

        /* Reconcile the latest target. Sleeping identities are symmetric:
         * sleeping_count is derived from thread_count and awake_workers, and
         * any SLEEP worker may represent an eligible sleeping slot. */
        waker_desired = suggested_thread_count;
        atomic {
            if
            :: waker_desired < thread_count ->
                waker_delta = thread_count - waker_desired;
                if
                :: sleeping_count < waker_delta ->
                    waker_converted = sleeping_count
                :: else -> waker_converted = waker_delta
                fi;
                waker_delta = waker_delta - waker_converted;
                assert(reductions + waker_delta <= N);
                reductions = reductions + waker_delta;
                thread_count = waker_desired

            :: waker_desired > thread_count ->
                waker_delta = waker_desired - thread_count;
                if
                :: reductions < waker_delta -> waker_converted = reductions
                :: else -> waker_converted = waker_delta
                fi;
                reductions = reductions - waker_converted;
                waker_delta = waker_delta - waker_converted;
                assert(claimed_reductions >= cancelled_claimed_reductions);
                if
                :: claimed_reductions - cancelled_claimed_reductions <
                        waker_delta ->
                    waker_converted = claimed_reductions -
                        cancelled_claimed_reductions
                :: else -> waker_converted = waker_delta
                fi;
                assert(cancelled_claimed_reductions + waker_converted <= N);
                cancelled_claimed_reductions =
                    cancelled_claimed_reductions + waker_converted;
                waker_delta = waker_delta - waker_converted;
                thread_count = waker_desired

            :: else -> skip
            fi;
            sleeping_count = 0;
            waker_delta = 0;
            waker_converted = 0
        };

        waker_budget = activation_credits;

        /* Observe every worker once. NONE only consumes the local budget at
         * the point where it is seen; it does not reserve a queue item and
         * may concurrently move to WORK or back into the idle path. */
        waker_i = 0;
        do
        :: waker_i < N ->
            if
            :: waker_i != id && state[waker_i] == NONE &&
                    waker_budget > 0 -> waker_budget--

            :: waker_i != id && state[waker_i] == SPIN ->
                if
                :: waker_budget > 0 ->
                    atomic {
                        if
                        :: state[waker_i] == SPIN ->
                            resume_state[waker_i] = NONE;
                            state[waker_i] = NONE;
                            waker_budget--
                        :: else -> skip
                        fi
                    }
                :: waker_budget == 0 ->
                    atomic {
                        if
                        :: state[waker_i] == SPIN ->
                            resume_state[waker_i] = SLEEP;
                            state[waker_i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                fi

            :: waker_i != id && state[waker_i] == BLOCKING ->
                if
                :: cancelled_claimed_reductions > 0 && waker_budget > 0 ->
                    atomic {
                        if
                        :: state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            resume_state[waker_i] = NONE;
                            state[waker_i] = NONE;
                            waker_budget--
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions > 0 && waker_budget == 0 ->
                    atomic {
                        if
                        :: state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            resume_state[waker_i] = SLEEP;
                            state[waker_i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions == 0 ->
                    atomic {
                        if
                        :: state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            resume_state[waker_i] = SLEEP;
                            state[waker_i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                fi
            :: else -> skip
            fi;
            waker_i++
        :: else -> break
        od;

        /* Wake eligible sleepers until the activation budget is covered. A
         * worker temporarily in NEED_TO_BE_WAKER remains counted and will be
         * resolved by either this owner or the next owner. */
        do
        :: waker_budget > 0 && thread_count > awake_workers ->
            waker_found = false;
            waker_i = 0;
            atomic {
                do
                :: waker_i < N && !waker_found ->
                    if
                    :: waker_i != id && state[waker_i] == SLEEP ->
                        state[waker_i] = NONE;
                        resume_state[waker_i] = NONE;
                        assert(sleep_workers > 0);
                        sleep_workers--;
                        awake_workers++;
                        waker_budget--;
                        waker_found = true
                    :: else -> skip
                    fi;
                    waker_i++
                :: else -> break
                od
            };
            if
            :: waker_found -> skip
            /* The eligible sleeper is the current owner or a worker which is
             * temporarily participating in election. */
            :: !waker_found -> break
            fi
        :: else -> break
        od;

        /* Treat the owner as its saved logical state while its public state
         * is WAKER/NEED_TO_BE_WAKER. */
        if
        :: resume_state[id] == BLOCKING ->
            assert(claimed_reductions > 0);
            if
            :: cancelled_claimed_reductions > 0 && waker_budget > 0 ->
                atomic {
                    claimed_reductions--;
                    cancelled_claimed_reductions--
                };
                waker_final = NONE;
                waker_budget--
            :: cancelled_claimed_reductions > 0 && waker_budget == 0 ->
                atomic {
                    claimed_reductions--;
                    cancelled_claimed_reductions--
                };
                waker_final = SLEEP
            :: cancelled_claimed_reductions == 0 ->
                claimed_reductions--;
                waker_final = SLEEP
            fi
        :: resume_state[id] != BLOCKING && waker_budget > 0 ->
            waker_final = NONE;
            waker_budget--
        :: resume_state[id] != BLOCKING && waker_budget == 0 ->
            if
            :: resume_state[id] == NONE || resume_state[id] == WORK ->
                waker_final = NONE
            :: else -> waker_final = SLEEP
            fi
        fi;

        atomic {
            if
            :: resume_state[id] == SLEEP && waker_final != SLEEP ->
                assert(sleep_workers > 0);
                sleep_workers--;
                awake_workers++
            :: resume_state[id] != SLEEP && waker_final == SLEEP ->
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            :: else -> skip
            fi;
            resume_state[id] = waker_final;
            if
            :: thread_count > awake_workers ->
                sleeping_count = thread_count - awake_workers
            :: else -> sleeping_count = 0
            fi
        };

        atomic {
            assert(waker_passes == 1);
            waker_i = 0;
            waker_budget = 0;
            waker_final = NONE;
            waker_desired = 0;
            waker_delta = 0;
            waker_converted = 0;
            waker_found = false;
            waker_passes = 0
        };

        if
        :: state[id] == NEED_TO_BE_WAKER ->
            atomic {
                state[id] == NEED_TO_BE_WAKER -> state[id] = WAKER
            }

        :: state[id] == WAKER ->
            atomic {
                assert(waker_worker_id == id);
                waker_worker_id = INVALID_WAKER
            };
            /* waker_final is shared scratch and may already belong to the
             * next owner. The retiring owner published its own final value
             * to resume_state[id] before releasing waker_worker_id. */
            publish_waker_final(id, resume_state[id])
        fi;

        check_local_safety()

    /* SLEEP and BLOCKING are parked. Changing either state to
     * NEED_TO_BE_WAKER models the matching Unpark(). */
    od
}

proctype Producer() {
    byte owner;
    byte i;
    byte round = 0;
    bool notify;
    bool done;

    do
    :: round < PRODUCER_ROUNDS ->
        atomic {
        activation_credits < MAX_QUEUE -> activation_credits++
        };
        atomic { queued_activations++ };

        notify = false;
        if
        :: sleeping_count > 0 ->
            atomic {
                if
                :: !waker_pending ->
                    waker_pending = true;
                    notify = true
                :: else -> skip
                fi
            }
        :: else -> skip
        fi;

        if
        :: notify ->
            done = false;
            do
            :: !done ->
                try_request_waker(owner, i, done, true);
                if
                :: done -> skip
                :: !done && owner < N -> skip
                :: !done && owner == INVALID_WAKER ->
                    if
                    :: sleeping_count > 0 ->
                        /* An eligible sleeper may already be between its
                         * candidate transition and index publication. */
                        i = 0;
                        do
                        :: i < N && !done ->
                            if
                            :: resume_state[i] == SLEEP &&
                                    (state[i] == NEED_TO_BE_WAKER ||
                                        state[i] == WAKER) ->
                                done = true
                            :: else -> skip
                            fi;
                            i++
                        :: else -> break
                        od;
                        if
                        :: done -> skip
                        :: else ->
                            atomic {
                                if
                                :: waker_worker_id == INVALID_WAKER ->
                                    waker_pending = false;
                                    done = true
                                :: else -> skip
                                fi
                            }
                        fi

                    :: sleeping_count == 0 ->
                        atomic {
                            if
                            :: waker_worker_id == INVALID_WAKER &&
                                    sleeping_count == 0 ->
                                waker_pending = false;
                                done = true
                            :: else -> skip
                            fi
                        }
                    fi
                fi
            :: else -> break
            od
        :: else -> skip
        fi;

        check_local_safety();
        atomic {
            owner = 0;
            i = 0;
            notify = false;
            done = false;
            round++
        }
    :: else -> break
    od
}

proctype Controller() {
    byte round = 0;
    byte owner;
    byte i;
    bool notified;

    do
    :: round < CONTROLLER_ROUNDS ->
        atomic {
            if
            :: suggested_thread_count == N -> suggested_thread_count = 1
            :: suggested_thread_count == 1 -> suggested_thread_count = N
            fi;
            waker_pending = true
        };

        try_request_waker(owner, i, notified, false);
        if
        /* No owner or parked candidate was stable during the scan. Preserve
         * the reduction count and publish a bit which the next idle worker
         * will consume before trying to block. */
        :: !notified ->
            atomic {
                if
                :: waker_worker_id == INVALID_WAKER ->
                    reductions = reductions | REDUCTION_WAKER_BIT
                :: else -> skip
                fi
            };
            /* A worker may have become parked between the first scan and
             * publishing the request bit. Scan again for both grow and
             * shrink; the bit keeps the request durable if this scan also
             * races with waker completion. */
            try_request_waker(owner, i, notified, false)
        :: else -> skip
        fi;
        /* Do not publish another target until the waker has observed this
         * one. This makes a lost resize request observable independently of
         * later controller updates. */
        thread_count == suggested_thread_count;
        atomic {
            owner = 0;
            i = 0;
            notified = false;
            round++
        }
    :: else -> break
    od
}

ltl live_resize_reconciles {
    [] ((suggested_thread_count != thread_count) ->
        <> (suggested_thread_count == thread_count))
}

init {
    atomic {
        byte i = 0;
        do
        :: i < N ->
            state[i] = WORK;
            resume_state[i] = WORK;
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
        run Controller()
    }
}
