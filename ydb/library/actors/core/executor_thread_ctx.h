#pragma once

#include "defs.h"
#include "thread_context.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/threadparkpad.h>


namespace NActors {
    class TExecutorThread;
    class TBasicExecutorPool;

    struct TExecutorThreadCtx {
        enum EWaitState : ui64 {
            WS_NONE,
            WS_ACTIVE,
            WS_BLOCKED,
            WS_RUNNING
        };

        struct TWaitState {
            EWaitState Flag = WS_NONE;
            ui32 NextPool = Max<ui32>();

            TWaitState() = default;

            explicit TWaitState(ui64 state)
                : Flag(static_cast<EWaitState>(state & 0x7))
                , NextPool(state >> 3)
            {}

            explicit TWaitState(EWaitState flag, ui32 nextPool = Max<ui32>())
                : Flag(flag)
                , NextPool(nextPool)
            {}

            explicit operator ui64() {
                return Flag | ui64(NextPool << 3);
            }
        };

        TAutoPtr<TExecutorThread> Thread;
        TThreadParkPad WaitingPad;

    private:
        std::atomic<ui64> WaitingFlag = WS_NONE;

    public:
        TBasicExecutorPool *OwnerExecutorPool = nullptr;
        std::atomic<TBasicExecutorPool*> OtherExecutorPool = nullptr;
        // std::atomic<ui64> FullCycleCheckCount = 0;
        ui64 StartWakingTs = 0;
        ui32 NextPool = 0;
        bool IsShared;

        // different threads must spin/block on different cache-lines.
        // we add some padding bytes to enforce this rule;

        TWaitState GetState() {
            return TWaitState(WaitingFlag.load());
        }

        TWaitState ExchangeState(EWaitState flag, ui32 nextPool = Max<ui32>()) {
            return TWaitState(WaitingFlag.exchange(static_cast<ui64>(TWaitState(flag, nextPool))));
        }

        bool ReplaceState(TWaitState &expected, EWaitState flag, ui32 nextPool = Max<ui32>()) {
            ui64 expectedInt = static_cast<ui64>(expected);
            bool result = WaitingFlag.compare_exchange_strong(expectedInt, static_cast<ui64>(TWaitState(flag, nextPool)));
            expected = TWaitState(expectedInt);
            return result;
        }

        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
            ui64 start = GetCycleCountFast();
            bool doSpin = true;
            while (true) {
                for (ui32 j = 0; doSpin && j < 12; ++j) {
                    if (GetCycleCountFast() >= (start + spinThresholdCycles)) {
                        doSpin = false;
                        break;
                    }
                    for (ui32 i = 0; i < 12; ++i) {
                        TWaitState state = GetState();
                        if (state.Flag == WS_ACTIVE) {
                            SpinLockPause();
                        } else {
                            NextPool = state.NextPool;
                            doSpin = false;
                            break;
                        }
                    }
                }
                if (!doSpin) {
                    break;
                }
                if (stopFlag->load(std::memory_order_relaxed)) {
                    break;
                }
            }
        }

        bool Sleep(std::atomic<bool> *stopFlag) {
            Y_DEBUG_ABORT_UNLESS(TlsThreadContext);

            TWaitState state;
            do {
                TlsThreadContext->Timers.HPNow = GetCycleCountFast();
                TlsThreadContext->Timers.Elapsed += TlsThreadContext->Timers.HPNow - TlsThreadContext->Timers.HPStart;
                if (WaitingPad.Park()) // interrupted
                    return true;
                TlsThreadContext->Timers.HPStart = GetCycleCountFast();
                TlsThreadContext->Timers.Parked += TlsThreadContext->Timers.HPStart - TlsThreadContext->Timers.HPNow;
                state = GetState();
            } while (state.Flag == WS_BLOCKED && !stopFlag->load(std::memory_order_relaxed));
            NextPool = state.NextPool;
            return false;
        }

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag); // in executor_pool_basic.cpp

        bool Block(std::atomic<bool> *stopFlag) {
            TWaitState state{WS_ACTIVE};
            if (ReplaceState(state, WS_BLOCKED)) {
                Y_ABORT_UNLESS(state.Flag == WS_ACTIVE, "WaitingFlag# %d", int(state.Flag));
                return Sleep(stopFlag);
            } else {
                return false;
            }
        }

        TExecutorThreadCtx() = default;
    };

}
