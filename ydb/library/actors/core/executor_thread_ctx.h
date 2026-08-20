#pragma once

#include "defs.h"
#include "activity_guard.h"
#include "executor_thread.h"
#include "thread_context.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/threadparkpad.h>


namespace NActors {
    class TExecutorThread;
    class TBasicExecutorPool;
    class IExecutorPool;

    enum class EThreadState : ui64 {
        None,
        Spin,
        Sleep,
        Work,
        Blocking,
        NeedToBeWaker,
        NeedToBeWakerFromSleep,
        NeedToBeWakerFromBlocking,
        Waker
    };

    constexpr bool IsNeedToBeWaker(EThreadState state) {
        return state == EThreadState::NeedToBeWaker ||
            state == EThreadState::NeedToBeWakerFromSleep ||
            state == EThreadState::NeedToBeWakerFromBlocking;
    }

    struct TGenericExecutorThreadCtx {
        std::unique_ptr<TExecutorThread> Thread;

    protected:
        friend class TBasicExecutorPool;
        friend class TIOExecutorPool;
        TThreadParkPad WaitingPad;

    private:
        static constexpr ui64 EncodeState(EThreadState state) {
            return static_cast<ui64>(state);
        }

        static constexpr EThreadState DecodeState(ui64 state) {
            return static_cast<EThreadState>(state);
        }

        std::atomic<ui64> WaitingFlag = EncodeState(EThreadState::None);

    public:
        ~TGenericExecutorThreadCtx(); // in executor_thread.cpp

        ui64 StartWakingTs = 0;

        ui64 GetStateInt() {
            return static_cast<ui64>(DecodeState(WaitingFlag.load()));
        }

    protected:
        template <typename TWaitState>
        TWaitState GetState() {
            return TWaitState(DecodeState(WaitingFlag.load()));
        }

        template <typename TWaitState>
        TWaitState ExchangeState(TWaitState state) {
            return TWaitState(DecodeState(WaitingFlag.exchange(EncodeState(static_cast<EThreadState>(state)))));
        }

        template <typename TWaitState>
        bool ReplaceState(TWaitState &expected, TWaitState state) {
            ui64 expectedInt = WaitingFlag.load();
            while (true) {
                const EThreadState current = DecodeState(expectedInt);
                if (current != static_cast<EThreadState>(expected)) {
                    expected = TWaitState(current);
                    return false;
                }
                if (WaitingFlag.compare_exchange_weak(expectedInt, EncodeState(static_cast<EThreadState>(state)))) {
                    return true;
                }
            }
        }

        bool BecomeWakerState(EThreadState* resumeState) {
            Y_ABORT_UNLESS(resumeState);
            ui64 state = WaitingFlag.load();
            while (true) {
                EThreadState currentResumeState;
                switch (DecodeState(state)) {
                    case EThreadState::NeedToBeWaker:
                        currentResumeState = EThreadState::None;
                        break;
                    case EThreadState::NeedToBeWakerFromSleep:
                        currentResumeState = EThreadState::Sleep;
                        break;
                    case EThreadState::NeedToBeWakerFromBlocking:
                        currentResumeState = EThreadState::Blocking;
                        break;
                    default:
                        return false;
                }
                if (WaitingFlag.compare_exchange_weak(state, EncodeState(EThreadState::Waker))) {
                    *resumeState = currentResumeState;
                    return true;
                }
            }
        }

        bool RestoreNeedToBeWakerState() {
            ui64 state = WaitingFlag.load();
            while (true) {
                EThreadState restoredState;
                switch (DecodeState(state)) {
                    case EThreadState::NeedToBeWaker:
                        restoredState = EThreadState::None;
                        break;
                    case EThreadState::NeedToBeWakerFromSleep:
                        restoredState = EThreadState::Sleep;
                        break;
                    case EThreadState::NeedToBeWakerFromBlocking:
                        restoredState = EThreadState::Blocking;
                        break;
                    default:
                        return false;
                }
                if (WaitingFlag.compare_exchange_weak(state, EncodeState(restoredState))) {
                    return true;
                }
            }
        }

        template <typename TDerived, typename TWaitState>
        void Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag);

        template <typename TDerived, typename TWaitState>
        bool Sleep(std::atomic<bool> *stopFlag);
    };

    struct TExecutorThreadCtx : public TGenericExecutorThreadCtx {
        using TBase = TGenericExecutorThreadCtx;

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

        bool TrySetNeedToBeWaker(EThreadState expected) {
            EThreadState wakerState;
            switch (expected) {
                case EThreadState::None:
                case EThreadState::Spin:
                    wakerState = EThreadState::NeedToBeWaker;
                    break;
                case EThreadState::Sleep:
                    wakerState = EThreadState::NeedToBeWakerFromSleep;
                    break;
                case EThreadState::Blocking:
                    wakerState = EThreadState::NeedToBeWakerFromBlocking;
                    break;
                default:
                    return false;
            }
            return ReplaceState(expected, wakerState);
        }

        bool BecomeWaker(EThreadState* resumeState) {
            return BecomeWakerState(resumeState);
        }

        bool RestoreWakerResumeState() {
            return RestoreNeedToBeWakerState();
        }

        bool WaitForWaker(std::atomic<bool>* stopFlag, std::atomic<i64>* activationCredits);

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

        bool Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp

        bool Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState); // in executor_pool_united.cpp
    };

}
