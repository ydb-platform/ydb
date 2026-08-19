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
 *  - an orthogonal worker state and waker role, so a worker selected from
 *    SLEEP remains logically sleeping throughout election;
 *  - dynamic thread-count changes, including claimed and unclaimed
 *    reduction tokens overtaken by a later resize;
 *  - a waker-request bit packed with the reduction count, which delegates a
 *    missed resize request to the next worker entering the idle path.
 *
 * Abstracted away:
 *  - exact SleepingStack order (worker ids provide a deterministic order);
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
#define ORDINARY 0
#define NEED_TO_BE_WAKER 1
#define WAKER 2

#define INVALID_WAKER N
#define REDUCTION_WAKER_BIT 4
#define REDUCTION_COUNT_MASK (REDUCTION_WAKER_BIT - 1)

byte worker_state[N];
byte waker_role[N];

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
byte waker_previous_activations = 0;
byte waker_final = NONE;
byte waker_desired = 0;
byte waker_delta = 0;
byte waker_converted = 0;
bool waker_found = false;
bool waker_woke = false;

inline check_local_safety() {
    atomic {
        assert(waker_worker_id <= INVALID_WAKER);
        assert(waker_passes <= 1);
        assert(waker_role[0] <= WAKER);
        assert(waker_role[1] <= WAKER);
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
            assert(waker_previous_activations == 0);
            assert(waker_final == NONE);
            assert(waker_desired == 0);
            assert(waker_delta == 0);
            assert(waker_converted == 0);
            assert(!waker_found);
            assert(!waker_woke)
        :: else -> skip
        fi;
        if
        :: waker_worker_id < N ->
            assert(waker_role[waker_worker_id] == WAKER ||
                waker_role[waker_worker_id] == NEED_TO_BE_WAKER)
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
        :: waker_role[id] == WAKER &&
                (reductions & REDUCTION_WAKER_BIT) ->
            reductions = reductions & REDUCTION_COUNT_MASK;
            worker_state[id] = final;
            waker_role[id] = NEED_TO_BE_WAKER
        :: waker_role[id] == WAKER &&
                !(reductions & REDUCTION_WAKER_BIT) ->
            worker_state[id] = final;
            waker_role[id] = ORDINARY
        :: waker_role[id] == NEED_TO_BE_WAKER ->
            worker_state[id] = final
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
            :: waker_role[owner] == WAKER ->
                waker_role[owner] = NEED_TO_BE_WAKER;
                notified = true
            :: waker_role[owner] == NEED_TO_BE_WAKER ->
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
                    :: require_sleepers &&
                            waker_role[candidate] == WAKER ->
                        waker_role[candidate] = NEED_TO_BE_WAKER;
                        notified = true
                    :: waker_role[candidate] == ORDINARY &&
                            (worker_state[candidate] == SPIN ||
                                worker_state[candidate] == BLOCKING ||
                                worker_state[candidate] == SLEEP) ->
                        waker_role[candidate] = NEED_TO_BE_WAKER;
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
    :: waker_role[id] == ORDINARY && worker_state[id] == WORK ->
        worker_state[id] = NONE

    :: waker_role[id] == ORDINARY && worker_state[id] == NONE ->
        if
        /* The controller could not find an existing waker or a parked
         * candidate. The first idle worker clears only the request bit and
         * enters election, preserving any numeric reduction count. */
        :: atomic {
            reductions & REDUCTION_WAKER_BIT ->
            reductions = reductions & REDUCTION_COUNT_MASK
        };
            atomic {
                assert(waker_role[id] == ORDINARY &&
                    worker_state[id] == NONE);
                waker_role[id] = NEED_TO_BE_WAKER;
                waker_pending = true
            }

        /* A reduction claim and its ownership accounting are one abstract
         * operation. Publishing BLOCKING remains a separate transition. */
        :: atomic {
            reductions > 0 && reductions < REDUCTION_WAKER_BIT ->
            reductions--;
            claimed_reductions++
        };
            atomic {
                assert(waker_role[id] == ORDINARY &&
                    worker_state[id] == NONE);
                worker_state[id] = BLOCKING;
                waker_role[id] = NEED_TO_BE_WAKER;
                waker_pending = true
            }

        :: reductions == 0 && suggested_thread_count == thread_count &&
                queued_activations > 0 ->
            atomic {
                queued_activations > 0 ->
                queued_activations--;
                assert(activation_credits > 0);
                activation_credits--;
                worker_state[id] = WORK
            }

        :: reductions == 0 && suggested_thread_count == thread_count &&
                activation_credits == 0 ->
            atomic {
                assert(waker_role[id] == ORDINARY &&
                    worker_state[id] == NONE);
                worker_state[id] = SPIN;
                waker_role[id] = NEED_TO_BE_WAKER;
                waker_pending = true
            }
        fi

    :: waker_role[id] == ORDINARY && worker_state[id] == SPIN &&
            activation_credits > 0 ->
        atomic {
            if
            :: waker_role[id] == ORDINARY &&
                    worker_state[id] == SPIN && activation_credits > 0 ->
                worker_state[id] = NONE
            :: else -> skip
            fi
        }

    :: waker_role[id] == NEED_TO_BE_WAKER ->
        done = false;
        do
        :: !done ->
            owner = waker_worker_id;
            if
            :: owner == INVALID_WAKER ->
                atomic {
                    if
                    :: waker_worker_id == INVALID_WAKER &&
                            waker_role[id] == NEED_TO_BE_WAKER ->
                        waker_worker_id = id;
                        done = true
                    :: else -> skip
                    fi
                };
                if
                :: done ->
                    atomic {
                        assert(waker_worker_id == id);
                        assert(waker_role[id] == NEED_TO_BE_WAKER);
                        waker_role[id] = WAKER
                    }
                :: else -> skip
                fi

            :: owner < N && owner != id ->
                atomic {
                    if
                    :: waker_role[owner] == WAKER ->
                        waker_role[owner] = NEED_TO_BE_WAKER;
                        done = true
                    :: waker_role[owner] == NEED_TO_BE_WAKER ->
                        done = true
                    :: else -> skip
                    fi
                };
                if
                :: done ->
                    atomic {
                        assert(waker_role[id] == NEED_TO_BE_WAKER);
                        waker_role[id] = ORDINARY
                    }
                :: else -> skip
                fi

            :: owner == id ->
                atomic {
                    if
                    :: waker_role[id] == NEED_TO_BE_WAKER ->
                        waker_role[id] = WAKER;
                        done = true
                    :: waker_role[id] == WAKER ->
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

    :: waker_role[id] == WAKER && waker_worker_id == id ->
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

        waker_previous_activations = activation_credits;
        waker_budget = waker_previous_activations;

        /* Observe every worker once. NONE only consumes the local budget at
         * the point where it is seen; it does not reserve a queue item and
         * may concurrently move to WORK or back into the idle path. */
        waker_i = 0;
        do
        :: waker_i < N ->
            if
            :: waker_i != id && waker_role[waker_i] == ORDINARY &&
                    worker_state[waker_i] == NONE &&
                    waker_budget > 0 -> waker_budget--

            :: waker_i != id && waker_role[waker_i] == ORDINARY &&
                    worker_state[waker_i] == SPIN ->
                if
                :: waker_budget > 0 ->
                    atomic {
                        if
                        :: waker_role[waker_i] == ORDINARY &&
                                worker_state[waker_i] == SPIN ->
                            worker_state[waker_i] = NONE;
                            waker_budget--
                        :: else -> skip
                        fi
                    }
                :: waker_budget == 0 ->
                    atomic {
                        if
                        :: waker_role[waker_i] == ORDINARY &&
                                worker_state[waker_i] == SPIN ->
                            worker_state[waker_i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                fi

            :: waker_i != id && waker_role[waker_i] == ORDINARY &&
                    worker_state[waker_i] == BLOCKING ->
                if
                :: cancelled_claimed_reductions > 0 && waker_budget > 0 ->
                    atomic {
                        if
                        :: waker_role[waker_i] == ORDINARY &&
                                worker_state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            worker_state[waker_i] = NONE;
                            waker_budget--
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions > 0 && waker_budget == 0 ->
                    atomic {
                        if
                        :: waker_role[waker_i] == ORDINARY &&
                                worker_state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            cancelled_claimed_reductions--;
                            worker_state[waker_i] = SLEEP;
                            assert(awake_workers > 0);
                            awake_workers--;
                            sleep_workers++
                        :: else -> skip
                        fi
                    }
                :: cancelled_claimed_reductions == 0 ->
                    atomic {
                        if
                        :: waker_role[waker_i] == ORDINARY &&
                                worker_state[waker_i] == BLOCKING ->
                            assert(claimed_reductions > 0);
                            claimed_reductions--;
                            worker_state[waker_i] = SLEEP;
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
            waker_woke = false;
            waker_i = 0;
            do
            :: waker_i < N && !waker_found ->
                if
                :: waker_i != id && worker_state[waker_i] == SLEEP ->
                    waker_found = true
                :: else -> waker_i++
                fi
            :: else -> break
            od;
            if
            :: waker_found ->
                atomic {
                    if
                    :: waker_role[waker_i] == ORDINARY &&
                            worker_state[waker_i] == SLEEP ->
                        worker_state[waker_i] = NONE;
                        assert(sleep_workers > 0);
                        sleep_workers--;
                        awake_workers++;
                        waker_budget--;
                        waker_woke = true
                    :: else -> skip
                    fi
                };
                if
                :: waker_woke -> skip
                /* Match the current C++ loop: a failed SLEEP -> NONE CAS
                 * stops this pass instead of scanning another stack entry. */
                :: !waker_woke -> break
                fi
            /* The only eligible sleeper may be the current owner. */
            :: !waker_found -> break
            fi
        :: else -> break
        od;

        /* The waker role is orthogonal to the worker's logical state. */
        if
        :: worker_state[id] == BLOCKING ->
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
        :: worker_state[id] != BLOCKING && waker_budget > 0 ->
            waker_final = NONE;
            waker_budget--
        :: worker_state[id] != BLOCKING && waker_budget == 0 ->
            if
            :: worker_state[id] == NONE || worker_state[id] == WORK ->
                waker_final = NONE
            :: else -> waker_final = SLEEP
            fi
        fi;

        atomic {
            if
            :: worker_state[id] == SLEEP && waker_final != SLEEP ->
                assert(sleep_workers > 0);
                sleep_workers--;
                awake_workers++
            :: worker_state[id] != SLEEP && waker_final == SLEEP ->
                assert(awake_workers > 0);
                awake_workers--;
                sleep_workers++
            :: else -> skip
            fi;
            worker_state[id] = waker_final;
            if
            :: thread_count > awake_workers ->
                sleeping_count = thread_count - awake_workers
            :: else -> sleeping_count = 0
            fi
        };

        /* Close the publication race where a producer increments credits
         * before this pass publishes the corresponding sleeping worker. */
        if
        :: activation_credits > waker_previous_activations ->
            atomic {
                waker_pending = true;
                if
                :: waker_role[id] == WAKER ->
                    waker_role[id] = NEED_TO_BE_WAKER
                :: waker_role[id] == NEED_TO_BE_WAKER -> skip
                fi
            }
        :: else -> skip
        fi;

        atomic {
            assert(waker_passes == 1);
            waker_i = 0;
            waker_budget = 0;
            waker_previous_activations = 0;
            waker_final = NONE;
            waker_desired = 0;
            waker_delta = 0;
            waker_converted = 0;
            waker_found = false;
            waker_woke = false;
            waker_passes = 0
        };

        if
        :: waker_role[id] == NEED_TO_BE_WAKER ->
            atomic {
                waker_role[id] == NEED_TO_BE_WAKER ->
                waker_role[id] = WAKER
            }

        :: waker_role[id] == WAKER ->
            atomic {
                assert(waker_worker_id == id);
                waker_worker_id = INVALID_WAKER
            };
            publish_waker_final(id, worker_state[id])
        fi;

        check_local_safety()

    /* SLEEP and BLOCKING are parked. Changing waker_role to
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
    :: true -> break
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
                        atomic {
                            if
                            :: waker_worker_id == INVALID_WAKER ->
                                waker_pending = false;
                                done = true
                            :: else -> skip
                            fi
                        }

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
    :: true -> break
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

/* With MAX_QUEUE == N == 2, express queue progress using only its size. A
 * single queued activation must eventually be either consumed or joined by
 * another activation; a full queue must eventually lose one activation. */
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
            worker_state[i] = WORK;
            waker_role[i] = ORDINARY;
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
