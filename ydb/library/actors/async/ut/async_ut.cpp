#include "common.h"

namespace NAsyncTest {

    using namespace NActors;

    namespace NActorAwareTest {

        struct TActorAwareNone {};
        struct TActorAwareType { using IsActorAwareAwaiter = void; };
        struct TActorAwareTrue { static constexpr bool IsActorAwareAwaiter = true; };
        struct TActorAwareFalse { static constexpr bool IsActorAwareAwaiter = false; };
        struct TActorAwareNonConstexpr { static inline bool IsActorAwareAwaiter = true; };
        struct TActorAwareNonStatic { bool IsActorAwareAwaiter = true; };

        static_assert(!IsActorAwareAwaiter<TActorAwareNone>);
        static_assert(IsActorAwareAwaiter<TActorAwareType>);
        static_assert(IsActorAwareAwaiter<TActorAwareTrue>);
        static_assert(!IsActorAwareAwaiter<TActorAwareFalse>);
        static_assert(!IsActorAwareAwaiter<TActorAwareNonConstexpr>);
        static_assert(!IsActorAwareAwaiter<TActorAwareNonStatic>);

    } // NActorAwareTest

    struct TReturnIntAwaiter {
        static constexpr bool IsActorAwareAwaiter = true;
        const int Value;

        bool await_ready() noexcept { return true; }
        void await_suspend(std::coroutine_handle<>) noexcept {
            Y_ABORT("unexpected call to await_suspend");
        }
        int await_resume() noexcept {
            return Value;
        }
    };

    struct TReturnIntStdAwaiter {
        const int Value;

        bool await_ready() noexcept { return true; }
        void await_suspend(std::coroutine_handle<>) noexcept {
            Y_ABORT("unexpected call to await_suspend");
        }
        int await_resume() noexcept {
            return Value;
        }
    };

    struct TThrowAwaiter {
        static constexpr bool IsActorAwareAwaiter = true;

        enum EWhen {
            Ready,
            Suspend,
            Resume,
        };
        const EWhen When;

        bool await_ready() {
            if (When == Ready) {
                throw TTestException() << "await_ready";
            }
            return false;
        }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
            if (When == Suspend) {
                throw TTestException() << "await_suspend";
            }
            return h;
        }

        void await_resume() {
            if (When == Resume) {
                throw TTestException() << "await_resume";
            }
        }
    };

    struct TThrowStdAwaiter {
        enum EWhen {
            Ready,
            Suspend,
            Resume,
        };
        const EWhen When;

        bool await_ready() {
            if (When == Ready) {
                throw TTestException() << "await_ready";
            }
            return false;
        }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
            if (When == Suspend) {
                throw TTestException() << "await_suspend";
            }
            return h;
        }

        void await_resume() {
            if (When == Resume) {
                throw TTestException() << "await_resume";
            }
        }
    };

    class TAsyncBootstrapTestActor : public TActorBootstrapped<TAsyncBootstrapTestActor> {
    public:
        struct TEvPrivate {
            enum EEv {
                EvTestEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            struct TEvTestEvent : public TEventLocal<TEvTestEvent, EvTestEvent> {
                int Value;

                TEvTestEvent(int value) : Value(value) {}
            };
        };

        struct TState {
            bool Destroyed = false;
        };

        TState& State;

        TAsyncBootstrapTestActor(TState& state)
            : State(state)
        {}

        ~TAsyncBootstrapTestActor() {
            State.Destroyed = true;
        }

        void Bootstrap() {
            Become(&TThis::StateWork);

            // This tests Bootstrap can be a coroutine
            co_await SimpleReturnVoid();

            int value1 = co_await SimpleReturnInt(42);
            UNIT_ASSERT_VALUES_EQUAL(value1, 42);

            int value2 = co_await SimpleReturnIntByRefArg(43);
            UNIT_ASSERT_VALUES_EQUAL(value2, 43);

            // This will test async handler taking event ptr by value
            Send(SelfId(), new TEvPrivate::TEvTestEvent{ 51 });
        }

        async<void> SimpleReturnVoid() {
            co_return;
        }

        async<int> SimpleReturnInt(int value) {
            co_return value;
        }

        async<int> SimpleReturnIntByRefArg(const int& value) {
            co_return value;
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvTestEvent, Handle);
                default:
                    Y_ABORT("Unexpected event");
            }
        }

        void Handle(TEvPrivate::TEvTestEvent::TPtr ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Value, 51);

            int value1 = co_await TReturnIntAwaiter{ 11 };
            UNIT_ASSERT_VALUES_EQUAL(value1, 11);
            int value2 = co_await TReturnIntStdAwaiter{ 22 };
            UNIT_ASSERT_VALUES_EQUAL(value2, 22);

            PassAway();
        }
    };

    Y_UNIT_TEST_SUITE(Async) {

        Y_UNIT_TEST(AsyncBootstrap) {
            TAsyncBootstrapTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            runtime.Register(new TAsyncBootstrapTestActor(state));
            runtime.DispatchEvents();

            UNIT_ASSERT_VALUES_EQUAL(state.Destroyed, true);
        }

        Y_UNIT_TEST(AwaiterThrowWithoutSuspend) {
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](TAsyncTestActor*) -> async<void> {
                // Note: we must not suspend in all these cases
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowAwaiter{ TThrowAwaiter::Ready }, TTestException, "await_ready");
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowAwaiter{ TThrowAwaiter::Suspend }, TTestException, "await_suspend");
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowAwaiter{ TThrowAwaiter::Resume }, TTestException, "await_resume");
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowStdAwaiter{ TThrowStdAwaiter::Ready }, TTestException, "await_ready");
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowStdAwaiter{ TThrowStdAwaiter::Suspend }, TTestException, "await_suspend");
                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await TThrowStdAwaiter{ TThrowStdAwaiter::Resume }, TTestException, "await_resume");
                UNIT_ASSERT(!state.CallbackReturned);
            });
            UNIT_ASSERT(state.CallbackReturned);

            actor.Poison();
            UNIT_ASSERT(state.Destroyed);
        }

        enum class EPassAway {
            None,
            BeforeSuspend,
            AfterSuspend,
            AfterResume,
        };

        enum class EAction {
            None,
            ResumeWithEvent,
            ResumeOutside,
            ResumeOutsideAndDispatch,
            CancelWithEvent,
            CancelOutside,
            CancelOutsideAndDispatch,
        };

        enum class ELastAction {
            None,
            Shutdown,
            ShutdownThenResume,
            ShutdownThenCancel,
        };

        template<bool WithCancelSupport>
        void DoTestAwaiter(
                EPassAway passAway,
                EAction action = EAction::None,
                ELastAction lastAction = ELastAction::None)
        {
            // Note: all state must be above the runtime
            TAsyncTestActor::TState state;
            bool killing = false;
            bool finished = false;
            bool finishedNormally = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](TAsyncTestActor* self) -> async<void> {
                Y_DEFER { finished = true; };

                if (passAway == EPassAway::BeforeSuspend) {
                    killing = true;
                    self->PassAway();
                }

                if constexpr (WithCancelSupport) {
                    co_await TSuspendAwaiter{ &resume, &cancel };
                } else {
                    co_await TSuspendAwaiterWithoutCancel{ &resume };
                }

                finishedNormally = true;
            });

            if (passAway == EPassAway::BeforeSuspend && WithCancelSupport) {
                UNIT_ASSERT(!resume);
                UNIT_ASSERT(!cancel);
                UNIT_ASSERT(finished);
                UNIT_ASSERT(!finishedNormally);
                UNIT_ASSERT(state.Destroyed);
                // Note: actor is dead and there's nothing else to test
                return;
            }

            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!state.Destroyed);

            if (passAway == EPassAway::AfterSuspend) {
                killing = true;
                actor.Poison();
                if (WithCancelSupport) {
                    UNIT_ASSERT(cancel);
                }
                UNIT_ASSERT(!finished);
                UNIT_ASSERT(!state.Destroyed);
            }

            switch (action) {
                case EAction::None:
                    break;
                case EAction::ResumeWithEvent:
                    Y_ABORT_UNLESS(resume, "Cannot resume without resume coroutine");
                    actor.ResumeCoroutine(resume);
                    UNIT_ASSERT(finished);
                    UNIT_ASSERT(finishedNormally);
                    if (killing) {
                        UNIT_ASSERT(state.Destroyed);
                    } else {
                        UNIT_ASSERT(!state.Destroyed);
                    }
                    break;
                case EAction::CancelWithEvent:
                    Y_ABORT_UNLESS(cancel, "Cannot cancel without cancel coroutine");
                    actor.ResumeCoroutine(cancel);
                    UNIT_ASSERT(finished);
                    UNIT_ASSERT(!finishedNormally);
                    if (killing) {
                        UNIT_ASSERT(state.Destroyed);
                    } else {
                        UNIT_ASSERT(!state.Destroyed);
                    }
                    break;
                default:
                    Y_ABORT("Not supported by single-threaded coroutines");
            }

            if (passAway == EPassAway::AfterResume) {
                Y_ABORT_UNLESS(finished, "EPassAway::AfterResume without actually resuming");
                killing = true;
                actor.Poison();
                UNIT_ASSERT(state.Destroyed);
            }

            if (lastAction != ELastAction::None) {
                bool finishedAlready = finishedNormally;
                // Run cleanup (which destroys all actors and events) as if during shutdown
                runtime.CleanupNode(0);
                // Actor must be destroyed now
                UNIT_ASSERT(state.Destroyed);
                // Destructors in coroutines must run
                UNIT_ASSERT(finished);
                // Coroutines cannot finish normally during shutdown
                UNIT_ASSERT(finishedAlready || !finishedNormally);
            }

            switch (lastAction) {
                case ELastAction::None:
                case ELastAction::Shutdown:
                    break;
                case ELastAction::ShutdownThenResume:
                case ELastAction::ShutdownThenCancel:
                    Y_ABORT("Not support by single-threaded coroutines");
            }
        }

        template<bool WithCancelSupport>
        void DoTestStdAwaiter(
                EPassAway passAway,
                EAction action = EAction::None,
                ELastAction lastAction = ELastAction::None)
        {
            // Note: all state must be above the runtime
            TAsyncTestActor::TState state;
            bool killing = false;
            bool finished = false;
            bool finishedNormally = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](TAsyncTestActor* self) -> async<void> {
                Y_DEFER { finished = true; };

                if (passAway == EPassAway::BeforeSuspend) {
                    killing = true;
                    self->PassAway();
                }

                if constexpr (WithCancelSupport) {
                    co_await TSuspendStdAwaiter{ &resume, &cancel };
                } else {
                    co_await TSuspendStdAwaiterWithoutCancel{ &resume };
                }

                finishedNormally = true;
            });

            if (passAway == EPassAway::BeforeSuspend && WithCancelSupport) {
                UNIT_ASSERT(!resume);
                UNIT_ASSERT(finished);
                UNIT_ASSERT(!finishedNormally);
                UNIT_ASSERT(state.Destroyed);
                // Note: actor is dead and there's nothing else to test
                return;
            }

            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!state.Destroyed);

            if (passAway == EPassAway::AfterSuspend) {
                killing = true;
                actor.Poison();
                if (WithCancelSupport) {
                    UNIT_ASSERT(cancel);
                }
                UNIT_ASSERT(!finished);
                UNIT_ASSERT(!state.Destroyed);
            }

            switch (action) {
                case EAction::None:
                    break;
                case EAction::ResumeWithEvent:
                    Y_ABORT_UNLESS(resume, "Cannot resume without resume coroutine");
                    actor.ResumeCoroutine(resume);
                    break;
                case EAction::ResumeOutside:
                case EAction::ResumeOutsideAndDispatch:
                    Y_ABORT_UNLESS(resume, "Cannot resume without resume coroutine");
                    // Standard C++ awaiters must be able to resume or cancel from any thread
                    resume();
                    // The resume is enqueued on the mailbox and must not happen immediately
                    UNIT_ASSERT(!finished);
                    break;
                case EAction::CancelWithEvent:
                    Y_ABORT_UNLESS(cancel, "Cannot cancel without cancel coroutine");
                    actor.ResumeCoroutine(cancel);
                    break;
                case EAction::CancelOutside:
                case EAction::CancelOutsideAndDispatch:
                    Y_ABORT_UNLESS(cancel, "Cannot cancel without cancel coroutine");
                    // Standard C++ awaiters must be able to resume or cancel from any thread
                    cancel();
                    // The cancel is enqueued on the mailbox and must not happen immediately
                    UNIT_ASSERT(!finished);
                    break;
            }

            switch (action) {
                case EAction::ResumeOutsideAndDispatch:
                case EAction::CancelOutsideAndDispatch:
                    runtime.DispatchEvents();
                    break;
                default:
                    break;
            }

            switch (action) {
                case EAction::ResumeOutsideAndDispatch:
                case EAction::ResumeWithEvent:
                    UNIT_ASSERT(finished);
                    UNIT_ASSERT(finishedNormally);
                    if (killing) {
                        UNIT_ASSERT(state.Destroyed);
                    } else {
                        UNIT_ASSERT(!state.Destroyed);
                    }
                    break;
                case EAction::CancelOutsideAndDispatch:
                case EAction::CancelWithEvent:
                    UNIT_ASSERT(finished);
                    UNIT_ASSERT(!finishedNormally);
                    UNIT_ASSERT(state.Destroyed);
                    break;
                default:
                    break;
            }

            if (passAway == EPassAway::AfterResume) {
                killing = true;
                actor.Poison();
                if (finished) {
                    UNIT_ASSERT(state.Destroyed);
                } else {
                    UNIT_ASSERT(!state.Destroyed);
                }
            }

            if (lastAction != ELastAction::None) {
                bool finishedAlready = finishedNormally;
                // Run cleanup (which destroys all actors and events) as if during shutdown
                runtime.CleanupNode(0);
                // Actor must be destroyed now
                UNIT_ASSERT(state.Destroyed);
                // Destructors in coroutines must run
                UNIT_ASSERT(finished);
                // Coroutines cannot finish normally during shutdown
                UNIT_ASSERT(finishedAlready || !finishedNormally);
            }

            switch (lastAction) {
                case ELastAction::None:
                case ELastAction::Shutdown:
                    break;
                case ELastAction::ShutdownThenResume:
                    // It must be possible to resume or cancel as long as actor system is not destroyed
                    // Note: awaiter must resume or cancel, otherwise it will be a memory leak
                    Y_ABORT_UNLESS(resume, "Cannot resume without resume coroutine");
                    resume();
                    break;
                case ELastAction::ShutdownThenCancel:
                    // It must be possible to resume or cancel as long as actor system is not destroyed
                    // Note: awaiter must resume or cancel, otherwise it will be a memory leak
                    Y_ABORT_UNLESS(cancel, "Cannot cancel without cancel coroutine");
                    resume();
                    break;
            }
        }

        Y_UNIT_TEST(AwaiterResume) {
            DoTestAwaiter<true>(EPassAway::AfterResume, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterResume) {
            DoTestStdAwaiter<true>(EPassAway::AfterResume, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterResumeOutsideActorSystem) {
            DoTestStdAwaiter<true>(EPassAway::AfterResume, EAction::ResumeOutsideAndDispatch);
        }

        Y_UNIT_TEST(AwaiterPassAwayThenResume) {
            DoTestAwaiter<true>(EPassAway::AfterSuspend, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenResume) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenResumeOutsideActorSystem) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::ResumeOutsideAndDispatch);
        }

        Y_UNIT_TEST(AwaiterPassAwayThenCancel) {
            DoTestAwaiter<true>(EPassAway::AfterSuspend, EAction::CancelWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenCancel) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::CancelWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenCancelOutsideActorSystem) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::CancelOutsideAndDispatch);
        }

        Y_UNIT_TEST(AwaiterNotSuspendingAfterPassAway) {
            DoTestAwaiter<true>(EPassAway::BeforeSuspend);
        }

        Y_UNIT_TEST(StdAwaiterNotSuspendingAfterPassAway) {
            DoTestStdAwaiter<true>(EPassAway::BeforeSuspend);
        }

        Y_UNIT_TEST(AwaiterWithoutCancelSupportSuspendAfterPassAwayThenResume) {
            DoTestAwaiter<false>(EPassAway::BeforeSuspend, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterWithoutCancelSupportSuspendAfterPassAwayThenResume) {
            DoTestStdAwaiter<false>(EPassAway::BeforeSuspend, EAction::ResumeWithEvent);
        }

        Y_UNIT_TEST(StdAwaiterWithoutCancelSupportSuspendAfterPassAwayThenResumeOutsideActorSystem) {
            DoTestStdAwaiter<false>(EPassAway::BeforeSuspend, EAction::ResumeOutsideAndDispatch);
        }

        // Shutdown tests for standard C++ awaiters

        Y_UNIT_TEST(StdAwaiterResumeOutsideActorSystemBeforeStop) {
            DoTestStdAwaiter<true>(EPassAway::None, EAction::ResumeOutside, ELastAction::Shutdown);
        }

        Y_UNIT_TEST(StdAwaiterResumeOutsideActorSystemAfterStop) {
            DoTestStdAwaiter<true>(EPassAway::None, EAction::None, ELastAction::ShutdownThenResume);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenResumeOutsideActorSystemBeforeStop) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::ResumeOutside, ELastAction::Shutdown);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenResumeOutsideActorSystemAfterStop) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::None, ELastAction::ShutdownThenResume);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenConfirmOutsideActorSystemBeforeStop) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::CancelOutside, ELastAction::Shutdown);
        }

        Y_UNIT_TEST(StdAwaiterPassAwayThenConfirmOutsideActorSystemAfterStop) {
            DoTestStdAwaiter<true>(EPassAway::AfterSuspend, EAction::None, ELastAction::ShutdownThenCancel);
        }

        Y_UNIT_TEST(StdAwaiterWithoutCancelSupportSuspendAfterPassAwayThenResumeOutsideActorSystemBeforeStop) {
            DoTestStdAwaiter<false>(EPassAway::BeforeSuspend, EAction::ResumeOutside, ELastAction::Shutdown);
        }

        Y_UNIT_TEST(StdAwaiterWithoutCancelSupportSuspendAfterPassAwayThenResumeOutsideActorSystemAfterStop) {
            DoTestStdAwaiter<false>(EPassAway::BeforeSuspend, EAction::None, ELastAction::ShutdownThenResume);
        }

        Y_UNIT_TEST(StdAwaiterNoResumeOnShutdown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume1, resume2;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                Y_DEFER {
                    if (resume2) {
                        sequence.push_back("calling resume");
                        resume2.resume();
                    }
                };

                co_await TSuspendStdAwaiterWithoutCancel{ &resume1 };

                sequence.push_back("resumed");
            });

            actor.RunAsync([&]() -> async<void> {
                sequence.push_back("second started");
                Y_DEFER { sequence.push_back("second finished"); };

                co_await TSuspendStdAwaiterWithoutCancel{ &resume2 };

                sequence.push_back("second resumed");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "second started");

            // Call to resume required to avoid a memory leak
            Y_DEFER { resume1.resume(); };

            runtime.CleanupNode();

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling resume", "finished", "second finished");
        }

    } // Y_UNIT_TEST_SUITE(Async)

}
