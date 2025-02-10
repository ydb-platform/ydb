#include "executor_thread_ctx.h"

#include "debug.h"
#include "thread_context.h"
#include "executor_pool_basic.h"

#define POOL_ID() \
    (!TlsThreadContext ? "OUTSIDE" : \
    (TlsThreadContext->IsShared() ? "Shared[" + ToString(TlsThreadContext->OwnerPoolId()) + "]_" + ToString(TlsThreadContext->PoolId()) : \
    ("Pool_" + ToString(TlsThreadContext->PoolId()))))

#define WORKER_ID() ("Worker_" + ToString(TlsThreadContext ? TlsThreadContext->WorkerId() : Max<TWorkerId>()))

#define EXECUTOR_THREAD_CTX_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, POOL_ID(), " ", WORKER_ID(), " T*ExecutorThreadCtx::", __func__, ": ", __VA_ARGS__)


namespace NActors {

    template <typename TDerived, typename TWaitState>
    void TGenericExecutorThreadCtx::Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
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
    bool TGenericExecutorThreadCtx::Sleep(std::atomic<bool> *stopFlag) {
        Y_DEBUG_ABORT_UNLESS(TlsThreadContext);

        TWaitState state = TWaitState{EThreadState::Spin};
        if (!ReplaceState<TWaitState>(state, TWaitState{EThreadState::Sleep})) {
            static_cast<TDerived*>(this)->AfterWakeUp(state);
            return false;
        }

        NHPTimer::STime hpnow = GetCycleCountFast();
        NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
        ui32 prevActivity = TlsThreadContext->ActivityContext.ElapsingActorActivity.exchange(SleepActivity, std::memory_order_acq_rel);
        TlsThreadContext->ExecutionStats->AddElapsedCycles(prevActivity, hpnow - hpprev);
        do {
            if (WaitingPad.Park()) // interrupted
                return true;
            hpnow = GetCycleCountFast();
            hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            TlsThreadContext->ExecutionStats->AddParkedCycles(hpnow - hpprev);
            state = GetState<TWaitState>();
        } while (static_cast<EThreadState>(state) == EThreadState::Sleep && !stopFlag->load(std::memory_order_relaxed));
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(TlsThreadContext->ActivityContext.ActorSystemIndex, std::memory_order_release);
        static_cast<TDerived*>(this)->AfterWakeUp(state);
        return false;
    }

    bool TExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            Spin(spinThresholdCycles, stopFlag);
        }
        return Sleep(stopFlag);
    }

    bool TExecutorThreadCtx::WakeUp() {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_WAKE_UP, false> activityGuard;
        for (ui32 i = 0; i < 2; ++i) {
            EThreadState state = GetState<EThreadState>();
            switch (state) {
                case EThreadState::None:
                case EThreadState::Work:
                    return false;
                case EThreadState::Spin:
                case EThreadState::Sleep:
                    if (ReplaceState<EThreadState>(state, EThreadState::None)) {
                        if (state == EThreadState::Sleep) {
                            ui64 beforeUnpark = GetCycleCountFast();
                            StartWakingTs = beforeUnpark;
                            WaitingPad.Unpark();
                            if (TlsThreadContext && TlsThreadContext->WaitingStats) {
                                TlsThreadContext->WaitingStats->AddWakingUp(GetCycleCountFast() - beforeUnpark);
                            }
                        }
                        return true;
                    }
                    break;
                default:
                    Y_ABORT();
            }
        }
        return false;
    }

    bool TSharedExecutorThreadCtx::Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        NHPTimer::STime start = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_SPIN> activityGuard(start);
        while (true) {
            for (ui32 j = 0;j < 12; ++j) {
                NHPTimer::STime hpnow = GetCycleCountFast();
                if (hpnow >= i64(start + spinThresholdCycles)) {
                    return false;
                }
                for (ui32 i = 0; i < 12; ++i) {
                    EThreadState state = GetState<EThreadState>();
                    if (state == EThreadState::Spin) {
                        TSharedExecutorPool::TThreadsState threadsStateValue = TSharedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
                        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
                        if (threadsStateValue.Notifications == 0 && localNotificationsValue == 0) {
                            SpinLockPause();
                        } else {
                            EXECUTOR_THREAD_CTX_DEBUG(EDebugLevel::Activation, "wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
                            ExchangeState(EThreadState::None);
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }
            if (stopFlag->load(std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    bool TSharedExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            if (Spin(spinThresholdCycles, stopFlag, localNotifications, threadsState)) {
                return false;
            }
        }
        TSharedExecutorPool::TThreadsState threadsStateValue = TSharedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
        if (threadsStateValue.Notifications != 0 || localNotificationsValue != 0)
        {
            EXECUTOR_THREAD_CTX_DEBUG(EDebugLevel::Activation, "wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
            ExchangeState(EThreadState::None);
            return false;
        } else {
            EXECUTOR_THREAD_CTX_DEBUG(EDebugLevel::Activation, "going to sleep after checking notifications; ownerPoolId == ", OwnerPoolId);
        }
        return Sleep(stopFlag);
    }

}
