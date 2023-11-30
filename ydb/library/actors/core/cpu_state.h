#pragma once

#include "defs.h"

#include <ydb/library/actors/util/futex.h>

namespace NActors {

    class alignas(64) TCpuState {
        // Atomic cachelign-aligned 64-bit state, see description below
        TAtomic State = 0;
        char Padding[64 - sizeof(TAtomic)];

        // Bits 0-31: Currently executing pool
        //  - value less than MaxPools means cpu is executing corresponding pool (fast-worker is executing or waiting for slow-workers)
        //  - one of Cpu* values in case of idle cpu
        //  - used as futex by blocked fast-worker
        static constexpr ui64 CurrentBits = 32;
        static constexpr ui64 CurrentMask = ui64((1ull << CurrentBits) - 1);

        // Bits 32-63: Assigned pool
        //  - value is set by balancer
        //  - NOT used as futex
        //  - Not balanced
        static constexpr ui64 AssignedOffs = 32;
        static constexpr ui64 AssignedMask = ~CurrentMask;

    public:
        TCpuState() {
            Y_UNUSED(Padding);
        }

        void Load(TPoolId& assigned, TPoolId& current) const {
            TAtomicBase state = AtomicLoad(&State);
            assigned = (state & AssignedMask) >> AssignedOffs;
            current = state & CurrentMask;
        }

        TPoolId CurrentPool() const {
            return TPoolId(AtomicLoad(&State) & CurrentMask);
        }

        void SwitchPool(TPoolId pool) {
            while (true) {
                TAtomicBase state = AtomicLoad(&State);
                if (AtomicCas(&State, (state & ~CurrentMask) | pool, state)) {
                    return;
                }
            }
        }

        TPoolId AssignedPool() const {
            return TPoolId((AtomicLoad(&State) & AssignedMask) >> AssignedOffs);
        }

        // Assigns new pool to cpu and wakes it up if cpu is idle
        void AssignPool(TPoolId pool) {
            while (true) {
                TAtomicBase state = AtomicLoad(&State);
                TPoolId current(state & CurrentMask);
                if (Y_UNLIKELY(current == CpuStopped)) {
                    return; // it would be better to shutdown instead of balancing
                }
                // Idle cpu must be woken up after balancing to handle pending tokens (if any) in assigned/schedulable pool(s)
                if (current == CpuSpinning) {
                    if (AtomicCas(&State, (ui64(pool) << AssignedOffs) | pool, state)) {
                        return; // successfully woken up
                    }
                } else if (current == CpuBlocked) {
                    if (AtomicCas(&State, (ui64(pool) << AssignedOffs) | pool, state)) {
                        FutexWake();
                        return; // successfully woken up
                    }
                } else {
                    if (AtomicCas(&State, (ui64(pool) << AssignedOffs) | (state & ~AssignedMask), state)) {
                        return; // wakeup is not required
                    }
                }
            }
        }

        void Stop() {
            while (true) {
                TAtomicBase state = AtomicLoad(&State);
                if (AtomicCas(&State, (state & ~CurrentMask) | CpuStopped, state)) {
                    FutexWake();
                    return; // successfully stopped
                }
            }
        }

        // Start waiting, returns false in case of actorsystem shutdown
        bool StartSpinning() {
            while (true) {
                TAtomicBase state = AtomicLoad(&State);
                TPoolId current(state & CurrentMask);
                if (Y_UNLIKELY(current == CpuStopped)) {
                    return false;
                }
                Y_DEBUG_ABORT_UNLESS(current < MaxPools, "unexpected already waiting state of cpu (%d)", (int)current);
                if (AtomicCas(&State, (state & ~CurrentMask) | CpuSpinning, state)) { // successfully marked as spinning
                    return true;
                }
            }
        }

        bool StartBlocking() {
            while (true) {
                TAtomicBase state = AtomicLoad(&State);
                TPoolId current(state & CurrentMask);
                if (current == CpuSpinning) {
                    if (AtomicCas(&State, (state & ~CurrentMask) | CpuBlocked, state)) {
                        return false; // successful switch
                    }
                } else {
                    return true; // wakeup
                }
            }
        }

        bool Block(ui64 timeoutNs, TPoolId& result) {
#ifdef _linux_
            timespec timeout;
            timeout.tv_sec = timeoutNs / 1'000'000'000;
            timeout.tv_nsec = timeoutNs % 1'000'000'000;
            SysFutex(Futex(), FUTEX_WAIT_PRIVATE, CpuBlocked, &timeout, nullptr, 0);
#else
            NanoSleep(timeoutNs); // non-linux wake is not supported, cpu will go idle on wake after blocked state
#endif
            TAtomicBase state = AtomicLoad(&State);
            TPoolId current(state & CurrentMask);
            if (current == CpuBlocked) {
                return false; // timeout
            } else {
                result = current;
                return true; // wakeup
            }
        }

        enum EWakeResult {
            Woken, // successfully woken up
            NotIdle, // cpu is already not idle
            Forbidden, // cpu is assigned to another pool
            Stopped, // cpu is shutdown
        };

        EWakeResult WakeWithoutToken(TPoolId pool) {
            while (true) {
                TAtomicBase state = RelaxedLoad(&State);
                TPoolId current(state & CurrentMask);
                TPoolId assigned((state & AssignedMask) >> AssignedOffs);
                if (assigned == CpuShared || assigned == pool) {
                    if (current == CpuSpinning) {
                        if (AtomicCas(&State, (state & ~CurrentMask) | pool, state)) {
                            return Woken;
                        }
                    } else if (current == CpuBlocked) {
                        if (AtomicCas(&State, (state & ~CurrentMask) | pool, state)) {
                            FutexWake();
                            return Woken;
                        }
                    } else if (current == CpuStopped) {
                        return Stopped;
                    } else {
                        return NotIdle;
                    }
                } else {
                    return Forbidden;
                }
            }
        }

        EWakeResult WakeWithTokenAcquired(TPoolId token) {
            while (true) {
                TAtomicBase state = RelaxedLoad(&State);
                TPoolId current(state & CurrentMask);
                // NOTE: We ignore assigned value because we already have token, so
                // NOTE: not assigned pool may be run here. This will be fixed
                // NOTE: after we finish with current activation
                if (current == CpuSpinning) {
                    if (AtomicCas(&State, (state & ~CurrentMask) | token, state)) {
                        return Woken;
                    }
                } else if (current == CpuBlocked) {
                    if (AtomicCas(&State, (state & ~CurrentMask) | token, state)) {
                        FutexWake();
                        return Woken;
                    }
                } else if (current == CpuStopped) {
                    return Stopped;
                } else {
                    return NotIdle;
                }
            }
        }

        bool IsPoolReassigned(TPoolId current) const {
            TAtomicBase state = AtomicLoad(&State);
            TPoolId assigned((state & AssignedMask) >> AssignedOffs);
            return assigned != current;
        }

    private:
        void* Futex() {
            return (void*)&State; // little endian assumed
        }

        void FutexWake() {
#ifdef _linux_
            SysFutex(Futex(), FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
#endif
        }
    };

}
