#include "actor.h"
#include "actor_bootstrapped.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/platform.h>
#include <util/system/backtrace.h>

#include <exception>

#if defined(_unix_)
#include <cerrno>
#include <sys/wait.h>
#include <unistd.h>
#endif

Y_UNIT_TEST_SUITE(ActorException) {

    using namespace NActors;
    using namespace NActors::NTests;
    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;

    class TActorBootstrapExceptionActor
        : public NActors::TActorWithExceptionHandler<TActorBootstrapped<TActorBootstrapExceptionActor>>
    {
    public:
        TActorBootstrapExceptionActor(TManualEvent& done)
            : Done(done)
        {}

        void Bootstrap() {
            throw std::runtime_error("test");
        }

    private:
        bool OnUnhandledException(const std::exception& e) override {
            Y_ABORT_UNLESS(TypeName(e) == "std::runtime_error");
            Done.Signal();
            PassAway();
            return true;
        }

    private:
        TManualEvent& Done;
    };

    class TActorHandlerExceptionActor
        : public NActors::TActorWithExceptionHandler<TActor<TActorHandlerExceptionActor>>
    {
    public:
        TActorHandlerExceptionActor(TManualEvent& done)
            : TActorWithExceptionHandler(&TThis::StateWork)
            , Done(done)
        {}

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvWakeup, Handle)
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr&) {
            if (ExceptionsHandled < 2) {
                throw std::runtime_error("test");
            }
            Done.Signal();
            PassAway();
        }

    private:
        bool OnUnhandledException(const std::exception& e) override {
            Y_ABORT_UNLESS(TypeName(e) == "std::runtime_error");
            ++ExceptionsHandled;
            Send(SelfId(), new TEvents::TEvWakeup);
            return true;
        }

    private:
        TManualEvent& Done;
        ui32 ExceptionsHandled = 0;
    };

    Y_UNIT_TEST(ActorBootstrapExceptionCaught) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TManualEvent doneEvent;
        actorSystem.Register(new TActorBootstrapExceptionActor(doneEvent));
        doneEvent.WaitI();

        actorSystem.Stop();
    }

    class TInheritedPrivateExceptionHandlerActor final
        : public TActorBootstrapExceptionActor
    {
    public:
        using TActorBootstrapExceptionActor::TActorBootstrapExceptionActor;
    };

    Y_UNIT_TEST(ActorInheritedPrivateExceptionHandlerCaught) {
        THolder<TActorSystemSetup> setup = TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TManualEvent doneEvent;
        IActor* actor = new TInheritedPrivateExceptionHandlerActor(doneEvent);
        UNIT_ASSERT(actor->HasExceptionHandler());
        actorSystem.Register(actor);
        doneEvent.WaitI();
        actorSystem.Stop();
    }

    Y_UNIT_TEST(ActorHandlerExceptionCaught) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TManualEvent doneEvent;
        const TActorId actorId = actorSystem.Register(new TActorHandlerExceptionActor(doneEvent));
        actorSystem.Send(actorId, new TEvents::TEvWakeup);
        doneEvent.WaitI();

        actorSystem.Stop();
    }

    class TCatchAllExceptionActor final
        : public TActorWithExceptionHandler<TActorBootstrapped<TCatchAllExceptionActor>>
    {
        TManualEvent& Done;

    public:
        explicit TCatchAllExceptionActor(TManualEvent& done)
            : Done(done)
        {}

        void Bootstrap() {
            throw 42;
        }

    private:
        bool OnUnhandledException(const std::exception_ptr& exception) override {
            try {
                std::rethrow_exception(exception);
            } catch (int value) {
                Y_ABORT_UNLESS(value == 42);
                Done.Signal();
                PassAway();
                return true;
            }
        }

        bool OnUnhandledException(const std::exception&) override {
            Y_ABORT("Non-std exception reached the std::exception overload");
        }
    };

    Y_UNIT_TEST(ActorExceptionPtrHandlerCatchesNonStdException) {
        THolder<TActorSystemSetup> setup = TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TManualEvent doneEvent;
        actorSystem.Register(new TCatchAllExceptionActor(doneEvent));
        doneEvent.WaitI();
        actorSystem.Stop();
    }

#if defined(_unix_)
    namespace {
        thread_local bool ThrowStarted = false;
        thread_local bool HandlerUnwound = false;
        thread_local bool ExceptionHandlerCalled = false;

        struct TUnwindProbe {
            ~TUnwindProbe() {
                HandlerUnwound = true;
            }
        };

        class TUnhandledExceptionActor : public TActor<TUnhandledExceptionActor> {
            ui32 WarmupEvents;
            bool NonStdException;

        public:
            explicit TUnhandledExceptionActor(ui32 warmupEvents, bool nonStdException = false)
                : TActor(&TThis::StateWork)
                , WarmupEvents(warmupEvents)
                , NonStdException(nonStdException)
            {}

            STFUNC(StateWork) {
                Y_UNUSED(ev);
                if (WarmupEvents) {
                    --WarmupEvents;
                    Send(SelfId(), new TEvents::TEvWakeup);
                    return;
                }
                TUnwindProbe probe;
                ThrowStarted = true;
                if (NonStdException) {
                    throw 42;
                }
                throw std::runtime_error("unhandled actor exception");
            }
        };

        class TDecliningExceptionActor final
            : public NActors::TActorWithExceptionHandler<TUnhandledExceptionActor>
        {
        public:
            explicit TDecliningExceptionActor(ui32 warmupEvents)
                : TActorWithExceptionHandler(warmupEvents)
            {}

        private:
            bool OnUnhandledException(const std::exception&) override {
                ExceptionHandlerCalled = true;
                return false;
            }
        };

        class TOverrideOnlyActor final : public TUnhandledExceptionActor {
        public:
            using TUnhandledExceptionActor::TUnhandledExceptionActor;

        private:
            bool OnUnhandledException(const std::exception&) override {
                ExceptionHandlerCalled = true;
                return true;
            }
        };

        class TDecliningNonStdExceptionActor final
            : public TActorWithExceptionHandler<TUnhandledExceptionActor>
        {
        public:
            explicit TDecliningNonStdExceptionActor(ui32 warmupEvents)
                : TActorWithExceptionHandler(warmupEvents, true)
            {}

        private:
            bool OnUnhandledException(const std::exception&) override {
                ExceptionHandlerCalled = true;
                return true;
            }
        };

        class TThrowingExceptionHandlerActor final
            : public TActorWithExceptionHandler<TUnhandledExceptionActor>
        {
        public:
            explicit TThrowingExceptionHandlerActor(ui32 warmupEvents)
                : TActorWithExceptionHandler(warmupEvents)
            {}

        private:
            bool OnUnhandledException(const std::exception&) override {
                ExceptionHandlerCalled = true;
                throw std::runtime_error("exception handler failed");
            }
        };

    }

    template <bool ThrowInHandler>
    class TTypedDecliningActor final
        : public TActorWithExceptionHandler<TActor<TTypedDecliningActor<ThrowInHandler>>>
    {
        using TBase = TActorWithExceptionHandler<TActor<TTypedDecliningActor<ThrowInHandler>>>;

    public:
        static constexpr bool CheckTypedRethrow = true;

        explicit TTypedDecliningActor(ui32)
            : TBase(&TTypedDecliningActor::StateWork)
        {}

        STFUNC(StateWork) {
            Y_UNUSED(ev);
            TUnwindProbe probe;
            ThrowStarted = true;
            throw std::runtime_error("original typed actor exception");
        }

    private:
        bool OnUnhandledException(const std::exception&) override {
            ExceptionHandlerCalled = true;
            if constexpr (ThrowInHandler) {
                throw std::logic_error("exception handler failed");
            }
            return false;
        }
    };

    namespace {
        template <typename TActorType>
        void CheckFatalException(int expectedMask, ui32 warmupEvents = 0) {
            // Start the actor system after fork: the worker must have no test
            // framework catch above Receive. A custom terminate handler lets us
            // inspect unwinding without relying on symbol names or core files.
            const pid_t child = fork();
            UNIT_ASSERT_C(child >= 0, "fork failed: " << errno);
            if (child == 0) {
                alarm(30);
                std::set_terminate([] {
                    // Report all three observations even on failure, so a
                    // regression distinguishes unwinding from an earlier crash.
                    int mask = (ThrowStarted ? 4 : 0) | (HandlerUnwound ? 2 : 0)
                        | (ExceptionHandlerCalled ? 1 : 0);
                    if constexpr (requires { TActorType::CheckTypedRethrow; }) {
                        void* frames[64];
                        const size_t count = BackTrace(frames, Y_ARRAY_SIZE(frames));
                        const TString expected = TypeName<TActor<TActorType>>() + "::RethrowUnhandledException()";
                        for (size_t i = 0; i < count; ++i) {
                            char name[2048];
                            // A noreturn call can leave its return PC just past
                            // the function; resolve the calling instruction.
                            const TResolvedSymbol symbol = ResolveSymbol(
                                reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(frames[i]) - 1),
                                name, sizeof(name));
                            if (TStringBuf(symbol.Name) == expected) {
                                mask |= 8;
                            }
                        }
                        try {
                            std::rethrow_exception(std::current_exception());
                        } catch (const std::runtime_error& exception) {
                            if (TStringBuf(exception.what()) == "original typed actor exception") {
                                mask |= 16;
                            }
                        } catch (...) {
                        }
                    }
                    _exit(mask);
                });

                THolder<TActorSystemSetup> setup = TActorBenchmark::GetActorSystemSetup();
                TActorBenchmark::AddBasicPool(setup, 1, 1, false);
                TActorSystem actorSystem(setup);
                actorSystem.Start();
                IActor* actor = new TActorType(warmupEvents);
                const TActorId actorId = actorSystem.Register(actor);
                actorSystem.Send(actorId, new TEvents::TEvWakeup);

                TManualEvent neverSignaled;
                neverSignaled.WaitI();
                _exit(2);
            }

            int status = 0;
            pid_t result;
            do {
                result = waitpid(child, &status, 0);
            } while (result == -1 && errno == EINTR);
            UNIT_ASSERT_VALUES_EQUAL(result, child);
            UNIT_ASSERT_C(WIFEXITED(status), "child did not exit normally, status: " << status);
            UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), expectedMask);
        }
    }

    Y_UNIT_TEST(DeclinedExceptionStackIdentifiesActor) {
        CheckFatalException<TTypedDecliningActor<false>>(31);
    }

    Y_UNIT_TEST(ThrowingHandlerPreservesOriginalExceptionAndActorFrame) {
        CheckFatalException<TTypedDecliningActor<true>>(31);
    }

    Y_UNIT_TEST(UnhandledExceptionKeepsActorFrame) {
        CheckFatalException<TUnhandledExceptionActor>(4);
    }

    Y_UNIT_TEST(DeclinedExceptionStillTerminates) {
        CheckFatalException<TDecliningExceptionActor>(7);
    }

    Y_UNIT_TEST(UnhandledExceptionKeepsActorFrameAfterEarlierEvents) {
        CheckFatalException<TUnhandledExceptionActor>(4, 2);
    }

    Y_UNIT_TEST(DeclinedExceptionStillTerminatesAfterEarlierEvents) {
        CheckFatalException<TDecliningExceptionActor>(7, 2);
    }

    Y_UNIT_TEST(OverridingHandlerDoesNotOptIntoCatching) {
        CheckFatalException<TOverrideOnlyActor>(4);
    }

    Y_UNIT_TEST(DefaultHandlerDeclinesNonStdException) {
        CheckFatalException<TDecliningNonStdExceptionActor>(6);
    }

    Y_UNIT_TEST(ThrowingExceptionHandlerStillTerminates) {
        CheckFatalException<TThrowingExceptionHandlerActor>(7);
    }
#endif

} // Y_UNIT_TEST_SUITE(ActorException)
