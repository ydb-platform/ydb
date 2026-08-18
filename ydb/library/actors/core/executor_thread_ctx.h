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
        Waker
    };

    struct TGenericExecutorThreadCtx {
        std::unique_ptr<TExecutorThread> Thread;

    protected:
        friend class TBasicExecutorPool;
        friend class TIOExecutorPool;
        TThreadParkPad WaitingPad;

    private:
        static constexpr ui64 StateMask = 0xff;
        static constexpr ui64 ResumeStateShift = 8;
        static constexpr ui64 InvalidResumeState = 0xff;

        static constexpr ui64 EncodeState(EThreadState state, ui64 resumeState = InvalidResumeState) {
            return static_cast<ui64>(state) | (resumeState << ResumeStateShift);
        }

        static constexpr EThreadState DecodeState(ui64 state) {
            return static_cast<EThreadState>(state & StateMask);
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

        bool ReplaceStateWithResume(EThreadState& expected, EThreadState state, EThreadState resumeState) {
            ui64 expectedInt = WaitingFlag.load();
            while (true) {
                const EThreadState current = DecodeState(expectedInt);
                if (current != expected) {
                    expected = current;
                    return false;
                }
                const ui64 desired = EncodeState(state, static_cast<ui64>(resumeState));
                if (WaitingFlag.compare_exchange_weak(expectedInt, desired)) {
                    return true;
                }
            }
        }

        bool ReplaceStatePreservingResume(EThreadState& expected, EThreadState state) {
            ui64 expectedInt = WaitingFlag.load();
            while (true) {
                const EThreadState current = DecodeState(expectedInt);
                if (current != expected) {
                    expected = current;
                    return false;
                }
                const ui64 desired = static_cast<ui64>(state) | (expectedInt & ~StateMask);
                if (WaitingFlag.compare_exchange_weak(expectedInt, desired)) {
                    return true;
                }
            }
        }

        EThreadState GetResumeState() {
            const ui64 state = WaitingFlag.load();
            const ui64 resumeState = (state >> ResumeStateShift) & StateMask;
            Y_ABORT_UNLESS(resumeState != InvalidResumeState);
            return static_cast<EThreadState>(resumeState);
        }

        void SetResumeState(EThreadState resumeState) {
            ui64 state = WaitingFlag.load();
            while (true) {
                const EThreadState current = DecodeState(state);
                Y_ABORT_UNLESS(current == EThreadState::Waker || current == EThreadState::NeedToBeWaker);
                const ui64 desired = EncodeState(current, static_cast<ui64>(resumeState));
                if (WaitingFlag.compare_exchange_weak(state, desired)) {
                    return;
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

        bool StartWaker(EThreadState expected, EThreadState resumeState) {
            return ReplaceStateWithResume(expected, EThreadState::NeedToBeWaker, resumeState);
        }

        bool BecomeWaker() {
            EThreadState expected = EThreadState::NeedToBeWaker;
            return ReplaceStatePreservingResume(expected, EThreadState::Waker);
        }

        bool RequestAnotherWakerPass() {
            EThreadState expected = EThreadState::Waker;
            if (ReplaceStatePreservingResume(expected, EThreadState::NeedToBeWaker)) {
                return true;
            }
            return expected == EThreadState::NeedToBeWaker;
        }

        EThreadState GetWakerResumeState() {
            return GetResumeState();
        }

        void SetWakerResumeState(EThreadState state) {
            SetResumeState(state);
        }

        bool RestoreWakerResumeState() {
            EThreadState expected = EThreadState::NeedToBeWaker;
            return ReplaceState(expected, GetResumeState());
        }

        bool WakeFromWaker() {
            EThreadState expected = EThreadState::Sleep;
            if (!ReplaceState(expected, EThreadState::None)) {
                return false;
            }
            WaitingPad.Unpark();
            return true;
        }

        bool WaitForWaker(ui64 spinThresholdCycles, std::atomic<bool>* stopFlag, std::atomic<i64>* activationCredits);

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
