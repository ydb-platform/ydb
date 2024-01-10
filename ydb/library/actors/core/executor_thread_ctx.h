#pragma once

#include "defs.h"
#include "thread_context.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/threadparkpad.h>


namespace NActors {
    class TExecutorThread;
    class TBasicExecutorPool;

    enum class EThreadState : ui64 {
        None,
        Spin,
        Sleep,
        Work
    };

    struct TGenericExecutorThreadCtx {
        TAutoPtr<TExecutorThread> Thread;
        TThreadParkPad WaitingPad;

    private:
        std::atomic<ui64> WaitingFlag = static_cast<ui64>(EThreadState::None);

    public:
        ui64 StartWakingTs = 0;

        template <typename TWaitState>
        TWaitState GetState() {
            return TWaitState(WaitingFlag.load());
        }

        template <typename TWaitState>
        TWaitState ExchangeState(TWaitState state) {
            return TWaitState(WaitingFlag.exchange(static_cast<ui64>(state)));
        }

        template <typename TWaitState>
        bool ReplaceState(TWaitState &expected, TWaitState state) {
            ui64 expectedInt = static_cast<ui64>(expected);
            bool result = WaitingFlag.compare_exchange_strong(expectedInt, static_cast<ui64>(state));
            expected = TWaitState(expectedInt);
            return result;
        }

    protected:
        template <typename TDerived, typename TWaitState>
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
                        TWaitState state = GetState<TWaitState>();
                        if (static_cast<EThreadState>(state) == EThreadState::Spin) {
                            SpinLockPause();
                        } else {
                            static_cast<TDerived*>(this)->AfterWakeUp(state);
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

        template <typename TDerived, typename TWaitState>
        bool Sleep(std::atomic<bool> *stopFlag) {
            Y_DEBUG_ABORT_UNLESS(TlsThreadContext);

            TWaitState state = TWaitState{EThreadState::Spin};
            if (!ReplaceState<TWaitState>(state, TWaitState{EThreadState::Sleep})) {
                static_cast<TDerived*>(this)->AfterWakeUp(state);
                return false;
            }

            do {
                TlsThreadContext->Timers.HPNow = GetCycleCountFast();
                TlsThreadContext->Timers.Elapsed += TlsThreadContext->Timers.HPNow - TlsThreadContext->Timers.HPStart;
                if (WaitingPad.Park()) // interrupted
                    return true;
                TlsThreadContext->Timers.HPStart = GetCycleCountFast();
                TlsThreadContext->Timers.Parked += TlsThreadContext->Timers.HPStart - TlsThreadContext->Timers.HPNow;
                state = GetState<TWaitState>();
            } while (static_cast<EThreadState>(state) == EThreadState::Sleep && !stopFlag->load(std::memory_order_relaxed));

            static_cast<TDerived*>(this)->AfterWakeUp(state);
            return false;
        }
    };
    
    struct TExecutorThreadCtx : public TGenericExecutorThreadCtx {
        using TBase = TGenericExecutorThreadCtx;

        TBasicExecutorPool *OwnerExecutorPool = nullptr;

        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
            this->TBase::Spin<TExecutorThreadCtx, EThreadState>(spinThresholdCycles, stopFlag);
        }

        bool Sleep(std::atomic<bool> *stopFlag) {
            return this->TBase::Sleep<TExecutorThreadCtx, EThreadState>(stopFlag);
        }

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag); // in executor_pool_basic.cpp

        void AfterWakeUp(EThreadState /*state*/) {
        }

        TExecutorThreadCtx() = default;
    };


    constexpr ui32 MaxPoolsForSharedThreads = 4;

    struct TSharedExecutorThreadCtx : public TGenericExecutorThreadCtx {
        using TBase = TGenericExecutorThreadCtx;

        struct TWaitState {
            EThreadState Flag = EThreadState::None;
            ui32 NextPool = Max<ui32>();

            TWaitState() = default;

            TWaitState(ui64 state)
                : Flag(static_cast<EThreadState>(state & 0x7))
                , NextPool(state >> 3)
            {}

            TWaitState(EThreadState flag, ui32 nextPool = Max<ui32>())
                : Flag(flag)
                , NextPool(nextPool)
            {}

            explicit operator ui64() {
                return static_cast<ui64>(Flag) | ui64(NextPool << 3);
            }

            explicit operator EThreadState() {
                return Flag;
            }
        };

        std::atomic<TBasicExecutorPool*> ExecutorPools[MaxPoolsForSharedThreads];
        ui32 NextPool = 0;

        void AfterWakeUp(TWaitState state) {
            NextPool = state.NextPool;
        }

        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
            this->TBase::Spin<TSharedExecutorThreadCtx, TWaitState>(spinThresholdCycles, stopFlag);
        }

        bool Sleep(std::atomic<bool> *stopFlag) {
            return this->TBase::Sleep<TSharedExecutorThreadCtx, TWaitState>(stopFlag);
        }

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag); // in executor_pool_basic.cpp

        TSharedExecutorThreadCtx() = default;
    };

}
