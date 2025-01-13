#pragma once

#include "defs.h"
#include "activity_guard.h"
#include "executor_thread.h"
#include "thread_context.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/threadparkpad.h>


namespace NActors {
    class TGenericExecutorThread;
    class IExecutorPool;

    enum class EThreadState : ui64 {
        None,
        Spin,
        Sleep,
        Work
    };

    struct TGenericExecutorThreadCtx {
        std::unique_ptr<TGenericExecutorThread> Thread;

    protected:
        friend class TIOExecutorPool;
        TThreadParkPad WaitingPad;

    private:
        std::atomic<ui64> WaitingFlag = static_cast<ui64>(EThreadState::None);

    public:
        ~TGenericExecutorThreadCtx(); // in executor_thread.cpp

        ui64 StartWakingTs = 0;

        ui64 GetStateInt() {
            return WaitingFlag.load();
        }

    protected:
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

        template <typename TDerived, typename TWaitState>
        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
            bool doSpin = true;
            NHPTimer::STime start = GetCycleCountFast();
            TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_SPIN> activityGuard(start);
            while (true) {
                for (ui32 j = 0; doSpin && j < 12; ++j) {
                    NHPTimer::STime hpnow = GetCycleCountFast();
                    if (hpnow >= i64(start + spinThresholdCycles)) {
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

            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ui32 prevActivity = TlsThreadContext->ElapsingActorActivity.exchange(Max<ui64>(), std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(prevActivity, hpnow - hpprev);
            do {
                if (WaitingPad.Park()) // interrupted
                    return true;
                hpnow = GetCycleCountFast();
                hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
                TlsThreadContext->WorkerCtx->AddParkedCycles(hpnow - hpprev);
                state = GetState<TWaitState>();
            } while (static_cast<EThreadState>(state) == EThreadState::Sleep && !stopFlag->load(std::memory_order_relaxed));
            TlsThreadContext->ActivationStartTS.store(hpnow, std::memory_order_release);
            TlsThreadContext->ElapsingActorActivity.store(TlsThreadContext->ActorSystemIndex, std::memory_order_release);
            static_cast<TDerived*>(this)->AfterWakeUp(state);
            return false;
        }
    };
    
    struct TExecutorThreadCtx : public TGenericExecutorThreadCtx {
        using TBase = TGenericExecutorThreadCtx;

        IExecutorPool *OwnerExecutorPool = nullptr;

        void SetWork() {
            ExchangeState(EThreadState::Work);
        }

        void UnsetWork() {
            ExchangeState(EThreadState::None);
        }

        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
            this->TBase::Spin<TExecutorThreadCtx, EThreadState>(spinThresholdCycles, stopFlag);
        }

        bool Sleep(std::atomic<bool> *stopFlag) {
            return this->TBase::Sleep<TExecutorThreadCtx, EThreadState>(stopFlag);
        }

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag); // in executor_pool_basic.cpp

        bool WakeUp();

        void Interrupt() {
            WaitingPad.Interrupt();
        }

        void AfterWakeUp(EThreadState /*state*/) {
        }

        TExecutorThreadCtx() = default;
    };

    struct TSharedExecutorThreadCtx : public TExecutorThreadCtx {
        using TBase = TExecutorThreadCtx;

        i16 PoolLeaseIndex = -1;
        i16 OwnerPoolId = -1;
        i16 CurrentPoolId = -1;
        NHPTimer::STime SoftDeadlineForPool = 0;
        NHPTimer::STime SoftProcessingDurationTs = 0;

        bool Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp
    };

}
