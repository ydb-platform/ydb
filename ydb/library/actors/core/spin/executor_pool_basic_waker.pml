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
 *    reduction tokens overtaken by a later resize.
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
        assert(reductions <= N);
        assert(claimed_reductions <= N);
        assert(cancelled_claimed_reductions <= claimed_reductions);
        assert(activation_credits <= MAX_QUEUE);
        assert(queued_activations <= activation_credits);
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
inline publish_waker_final(id, final, published) {
    atomic {
        if
        :: state[id] == WAKER ->
            resume_state[id] = final;
            state[id] = final;
            published = true
        :: state[id] == NEED_TO_BE_WAKER ->
            published = false
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
    byte i;
    byte budget;
    byte final;
    byte desired;
    byte delta;
    byte converted;
    bool done;
    bool found;
    bool published;

    do
    :: state[id] == WORK ->
        state[id] = NONE

    :: state[id] == NONE ->
        if
        /* A reduction claim and its ownership accounting are one abstract
         * operation. Publishing BLOCKING remains a separate transition. */
        :: atomic {
            reductions > 0 ->
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
        assert(done)

    :: state[id] == WAKER && waker_worker_id == id ->
        atomic {
            assert(waker_passes == 0);
            waker_passes = 1;
            waker_pending = false
        };

        /* Reconcile the latest target. Sleeping identities are symmetric:
         * sleeping_count is derived from thread_count and awake_workers, and
         * any SLEEP worker may represent an eligible sleeping slot. */
        desired = suggested_thread_count;
        atomic {
            if
            :: desired < thread_count ->
                delta = thread_count - desired;
                if
                :: sleeping_count < delta -> converted = sleeping_count
                :: else -> converted = delta
                fi;
                delta = delta - converted;
                assert(reductions + delta <= N);
                reductions = reductions + delta;
                thread_count = desired

            :: desired > thread_count ->
                delta = desired - thread_count;
                if
                :: reductions < delta -> converted = reductions
                :: else -> converted = delta
                fi;
                reductions = reductions - converted;
                delta = delta - converted;
                assert(claimed_reductions >= cancelled_claimed_reductions);
                if
                :: claimed_reductions - cancelled_claimed_reductions < delta ->
                    converted = claimed_reductions -
                        cancelled_claimed_reductions
                :: else -> converted = delta
                fi;
                assert(cancelled_claimed_reductions + converted <= N);
                cancelled_claimed_reductions =
                    cancelled_claimed_reductions + converted;
                delta = delta - converted;
                thread_count = desired

            :: else -> skip
            fi;
            sleeping_count = 0;
            delta = 0;
            converted = 0
        };

        budget = activation_credits;

        /* Account for workers which are already searching before changing
         * SPIN/BLOCKING states. The budget is local and never reserves the
         * corresponding queue item. */
        i = 0;
        do
        :: i < N ->
            if
            :: i != id && state[i] == NONE && budget > 0 -> budget--
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        i = 0;
        do
        :: i < N ->
            if
            :: i != id && state[i] == SPIN ->
                if
                :: budget > 0 ->
                    atomic {
                        if
                        :: state[i] == SPIN ->
                            resume_state[i] = NONE;
                            state[i] = NONE;
                            budget--
                        :: else -> skip
                        fi
                    }
                :: budget == 0 ->
                    atomic {
                        if
                        :: state[i] == SPIN ->
                            resume_state[i] = SLEEP;
                            state[i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                fi

            :: i != id && state[i] == BLOCKING ->
                if
                :: cancelled_claimed_reductions > 0 && budget > 0 ->
                    atomic {
                        if
                        :: state[i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            resume_state[i] = NONE;
                            state[i] = NONE;
                            budget--
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions > 0 && budget == 0 ->
                    atomic {
                        if
                        :: state[i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            resume_state[i] = SLEEP;
                            state[i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions == 0 ->
                    atomic {
                        if
                        :: state[i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            resume_state[i] = SLEEP;
                            state[i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                fi
            :: else -> skip
            fi;
            i++
        :: else -> break
        od;

        /* Wake eligible sleepers until the activation budget is covered. A
         * worker temporarily in NEED_TO_BE_WAKER remains counted and will be
         * resolved by either this owner or the next owner. */
        do
        :: budget > 0 && thread_count > awake_workers ->
            found = false;
            i = 0;
            atomic {
                do
                :: i < N && !found ->
                    if
                    :: i != id && state[i] == SLEEP ->
                        state[i] = NONE;
                        resume_state[i] = NONE;
                        assert(sleep_workers > 0);
                        sleep_workers--;
                        awake_workers++;
                        budget--;
                        found = true
                    :: else -> skip
                    fi;
                    i++
                :: else -> break
                od
            };
            if
            :: found -> skip
            /* The eligible sleeper is the current owner or a worker which is
             * temporarily participating in election. */
            :: !found -> break
            fi
        :: else -> break
        od;

        /* Treat the owner as its saved logical state while its public state
         * is WAKER/NEED_TO_BE_WAKER. */
        if
        :: resume_state[id] == BLOCKING ->
            assert(claimed_reductions > 0);
            if
            :: cancelled_claimed_reductions > 0 && budget > 0 ->
                atomic {
                    claimed_reductions--;
                    cancelled_claimed_reductions--
                };
                final = NONE;
                budget--
            :: cancelled_claimed_reductions > 0 && budget == 0 ->
                atomic {
                    claimed_reductions--;
                    cancelled_claimed_reductions--
                };
                final = SLEEP
            :: cancelled_claimed_reductions == 0 ->
                claimed_reductions--;
                final = SLEEP
            fi
        :: resume_state[id] != BLOCKING && budget > 0 ->
            final = NONE;
            budget--
        :: resume_state[id] != BLOCKING && budget == 0 ->
            if
            :: resume_state[id] == NONE || resume_state[id] == WORK ->
                final = NONE
            :: else -> final = SLEEP
            fi
        fi;

        atomic {
            if
            :: resume_state[id] == SLEEP && final != SLEEP ->
                assert(sleep_workers > 0);
                sleep_workers--;
                awake_workers++
            :: resume_state[id] != SLEEP && final == SLEEP ->
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            :: else -> skip
            fi;
            resume_state[id] = final;
            if
            :: thread_count > awake_workers ->
                sleeping_count = thread_count - awake_workers
            :: else -> sleeping_count = 0
            fi
        };

        atomic {
            assert(waker_passes == 1);
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
            published = false;
            publish_waker_final(id, final, published)
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
        round++
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
        /* Do not publish another target until the waker has observed this
         * one. This makes a lost resize request observable independently of
         * later controller updates. */
        thread_count == suggested_thread_count;
        round++
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
