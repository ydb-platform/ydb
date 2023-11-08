#pragma once

#include "defs.h"

#include <library/cpp/actors/util/futex.h>

namespace NActors {

    class alignas(64) TCpuState {
        // Atomic cachelign-aligned 64-bit state, see description below
        std::atomic<i64> State = 0;
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
            i64 state = State.load(std::memory_order_acquire);
            assigned = (state & AssignedMask) >> AssignedOffs;
            current = state & CurrentMask;
        }

        TPoolId CurrentPool() const {
            return TPoolId(State.load(std::memory_order_acquire) & CurrentMask);
        }

        void SwitchPool(TPoolId pool) {
            i64 state = State.load(std::memory_order_acquire);

            while (true) {
                if (State.compare_exchange_strong(state, (state & ~CurrentMask) | pool)) {
                    return;
                }
            }
        }

        TPoolId AssignedPool() const {
            return TPoolId((State.load(std::memory_order_acquire) & AssignedMask) >> AssignedOffs);
        }

        // Assigns new pool to cpu and wakes it up if cpu is idle
        void AssignPool(TPoolId pool) {
            i64 state = State.load(std::memory_order_acquire);

            while (true) {
                TPoolId current(state & CurrentMask);
                if (Y_UNLIKELY(current == CpuStopped)) {
                    return; // it would be better to shutdown instead of balancing
                }
                // Idle cpu must be woken up after balancing to handle pending tokens (if any) in assigned/schedulable pool(s)
                if (current == CpuSpinning) {
                    if (State.compare_exchange_strong(state, (ui64(pool) << AssignedOffs) | pool)) {
                        return; // successfully woken up
                    }
                } else if (current == CpuBlocked) {
                    if (State.compare_exchange_strong(state, (ui64(pool) << AssignedOffs) | pool)) {
                        FutexWake();
                        return; // successfully woken up
                    }
                } else {
                    if (State.compare_exchange_strong(state, (ui64(pool) << AssignedOffs) | (state & ~AssignedMask))) {
                        return; // wakeup is not required
                    }
                }
            }
        }

        void Stop() {
            i64 state = State.load(std::memory_order_acquire);

            while (true) {
                if (State.compare_exchange_strong(state, (state & ~CurrentMask) | CpuStopped)) {
                    FutexWake();
                    return; // successfully stopped
                }
            }
        }

        // Start waiting, returns false in case of actorsystem shutdown
        bool StartSpinning() {
            i64 state = State.load(std::memory_order_acquire);

            while (true) {
                TPoolId current(state & CurrentMask);
                if (Y_UNLIKELY(current == CpuStopped)) {
                    return false;
                }
                Y_DEBUG_ABORT_UNLESS(current < MaxPools, "unexpected already waiting state of cpu (%d)", (int)current);
                if (State.compare_exchange_strong(state, (state & ~CurrentMask) | CpuSpinning)) { // successfully marked as spinning
                    return true;
                }
            }
        }

        bool StartBlocking() {
            i64 state = State.load(std::memory_order_acquire);

            while (true) {
                TPoolId current(state & CurrentMask);
                if (current == CpuSpinning) {
                    if (State.compare_exchange_strong(state, (state & ~CurrentMask) | CpuBlocked)) {
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
            i64 state = State.load(std::memory_order_acquire);
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
            i64 state = State.load(std::memory_order_relaxed);

            while (true) {
                TPoolId current(state & CurrentMask);
                TPoolId assigned((state & AssignedMask) >> AssignedOffs);
                if (assigned == CpuShared || assigned == pool) {
                    if (current == CpuSpinning) {
                        if (State.compare_exchange_strong(state, (state & ~CurrentMask) | pool)) {
                            return Woken;
                        }
                    } else if (current == CpuBlocked) {
                        if (State.compare_exchange_strong(state, (state & ~CurrentMask) | pool)) {
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
            i64 state = State.load(std::memory_order_relaxed);

            while (true) {
                TPoolId current(state & CurrentMask);
                // NOTE: We ignore assigned value because we already have token, so
                // NOTE: not assigned pool may be run here. This will be fixed
                // NOTE: after we finish with current activation
                if (current == CpuSpinning) {
                    if (State.compare_exchange_strong(state, (state & ~CurrentMask) | token)) {
                        return Woken;
                    }
                } else if (current == CpuBlocked) {
                    if (State.compare_exchange_strong(state, (state & ~CurrentMask) | token)) {
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
            i64 state = State.load(std::memory_order_acquire);
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
