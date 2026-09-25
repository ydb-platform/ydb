#pragma once

#include "defs.h"
#include "activity_guard.h"
#include "executor_thread.h"
#include "thread_context.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/threadparkpad.h>


namespace NActors {
    class TExecutorThread;
    class TBasicExecutorPoolBase;
    class IExecutorPool;

    enum class EThreadState : ui64 {
        None,
        Spin,
        Sleep,
        Work,
        Blocking,
        NeedToBeWaker,
        NeedToBeWakerFromSpin,
        NeedToBeWakerFromSleep,
        NeedToBeWakerFromBlocking,
        Waker
    };

    constexpr bool IsNeedToBeWaker(EThreadState state) {
        return state == EThreadState::NeedToBeWaker ||
            state == EThreadState::NeedToBeWakerFromSpin ||
            state == EThreadState::NeedToBeWakerFromSleep ||
            state == EThreadState::NeedToBeWakerFromBlocking;
    }

    struct TGenericExecutorThreadCtx {
        std::unique_ptr<TExecutorThread> Thread;

    protected:
        friend class TBasicExecutorPoolBase;
        template<class TQueue>
        friend class TBasicExecutorPoolImpl;
        friend class TSharedExecutorPool;
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
        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag);

        template <typename TDerived, typename TWaitState>
        bool Sleep(std::atomic<bool> *stopFlag);
    };

    struct TExecutorThreadCtx : public TGenericExecutorThreadCtx {
        using TBase = TGenericExecutorThreadCtx;

    private:
        static bool TryGetNeedToBeWakerState(EThreadState state, EThreadState* wakerState) {
            Y_ABORT_UNLESS(wakerState);
            switch (state) {
                case EThreadState::None:
                    *wakerState = EThreadState::NeedToBeWaker;
                    return true;
                case EThreadState::Spin:
                    *wakerState = EThreadState::NeedToBeWakerFromSpin;
                    return true;
                case EThreadState::Sleep:
                    *wakerState = EThreadState::NeedToBeWakerFromSleep;
                    return true;
                case EThreadState::Blocking:
                    *wakerState = EThreadState::NeedToBeWakerFromBlocking;
                    return true;
                default:
                    return false;
            }
        }

        static bool TryGetWakerResumeState(EThreadState state, EThreadState* resumeState) {
            Y_ABORT_UNLESS(resumeState);
            switch (state) {
                case EThreadState::None:
                case EThreadState::NeedToBeWaker:
                    *resumeState = EThreadState::None;
                    return true;
                case EThreadState::Spin:
                case EThreadState::NeedToBeWakerFromSpin:
                    *resumeState = EThreadState::Spin;
                    return true;
                case EThreadState::Sleep:
                case EThreadState::NeedToBeWakerFromSleep:
                    *resumeState = EThreadState::Sleep;
                    return true;
                case EThreadState::Blocking:
                case EThreadState::NeedToBeWakerFromBlocking:
                    *resumeState = EThreadState::Blocking;
                    return true;
                default:
                    return false;
            }
        }

    public:
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

        bool TrySetNeedToBeWaker(EThreadState* expected) {
            Y_ABORT_UNLESS(expected);
            EThreadState wakerState;
            if (!TryGetNeedToBeWakerState(*expected, &wakerState)) {
                return false;
            }
            return ReplaceState(*expected, wakerState);
        }

        bool TrySetNeedToBeWaker() {
            EThreadState state = GetState<EThreadState>();
            while (true) {
                if (IsNeedToBeWaker(state) || state == EThreadState::Waker) {
                    return true;
                }
                if (state != EThreadState::None && state != EThreadState::Spin &&
                        state != EThreadState::Sleep && state != EThreadState::Blocking) {
                    return false;
                }
                if (TrySetNeedToBeWaker(&state)) {
                    return true;
                }
            }
        }

        bool TryBecomeWaker(EThreadState* resumeState) {
            EThreadState state = GetState<EThreadState>();
            while (true) {
                EThreadState currentResumeState;
                if (!TryGetWakerResumeState(state, &currentResumeState)) {
                    return false;
                }
                if (ReplaceState(state, EThreadState::Waker)) {
                    *resumeState = currentResumeState;
                    return true;
                }
            }
        }

        bool CancelWakerRequest() {
            EThreadState state = GetState<EThreadState>();
            while (true) {
                if (!IsNeedToBeWaker(state)) {
                    return false;
                }
                EThreadState resumeState;
                Y_ABORT_UNLESS(TryGetWakerResumeState(state, &resumeState));
                if (ReplaceState(state, resumeState)) {
                    return true;
                }
            }
        }

        bool WaitForWaker(
            const std::atomic<bool>& stopFlag,
            const std::atomic<i64>& activationCredits,
            const std::atomic<ui64>& reductions,
            ui64 wakerRequestBit);

        bool ParkForWaker(const std::atomic<bool>& stopFlag);

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
        i16 AdjacentPoolId = -1;
        NHPTimer::STime SoftDeadlineForPool = 0;
        NHPTimer::STime SoftProcessingDurationTs = 0;

        void SetWorkForWaker() {
            EThreadState state = GetState<EThreadState>();
            while (state == EThreadState::None || state == EThreadState::Spin) {
                if (ReplaceState(state, EThreadState::Work)) {
                    return;
                }
            }
            // A dequeue racing with a sleep or waker request owns its mailbox.
            // Preserve the request until that activation has finished.
            Y_DEBUG_ABORT_UNLESS(state == EThreadState::Blocking || IsNeedToBeWaker(state));
        }

        void UnsetWorkForWaker() {
            EThreadState state = EThreadState::Work;
            ReplaceState(state, EThreadState::None);
        }

        bool Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp
    };

}
