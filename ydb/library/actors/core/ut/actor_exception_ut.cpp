#include "actor.h"
#include "actor_bootstrapped.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"
#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/platform.h>

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
        : public TActorBootstrapped<TActorBootstrapExceptionActor>
        , public IActorExceptionHandler
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
        : public TActor<TActorHandlerExceptionActor>
        , public IActorExceptionHandler
    {
    public:
        TActorHandlerExceptionActor(TManualEvent& done)
            : TActor(&TThis::StateWork)
            , Done(done)
        {}

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvWakeup, Handle)
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr&) {
            throw std::runtime_error("test");
        }

    private:
        bool OnUnhandledException(const std::exception& e) {
            Y_ABORT_UNLESS(TypeName(e) == "std::runtime_error");
            Done.Signal();
            PassAway();
            return true;
        }

    private:
        TManualEvent& Done;
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
        public:
            TUnhandledExceptionActor()
                : TActor(&TThis::StateWork)
            {}

            STFUNC(StateWork) {
                Y_UNUSED(ev);
                TUnwindProbe probe;
                ThrowStarted = true;
                throw std::runtime_error("unhandled actor exception");
            }
        };

        class TDecliningExceptionActor final
            : public TUnhandledExceptionActor
            , public IActorExceptionHandler
        {
            bool OnUnhandledException(const std::exception&) override {
                ExceptionHandlerCalled = true;
                return false;
            }
        };

        void CheckFatalException(bool withHandler) {
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
                    _exit((ThrowStarted ? 4 : 0) | (HandlerUnwound ? 2 : 0)
                        | (ExceptionHandlerCalled ? 1 : 0));
                });

                THolder<TActorSystemSetup> setup = TActorBenchmark::GetActorSystemSetup();
                TActorBenchmark::AddBasicPool(setup, 1, 1, false);
                TActorSystem actorSystem(setup);
                actorSystem.Start();
                IActor* actor = withHandler
                    ? static_cast<IActor*>(new TDecliningExceptionActor)
                    : static_cast<IActor*>(new TUnhandledExceptionActor);
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
            UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), withHandler ? 7 : 4);
        }
    }

    Y_UNIT_TEST(UnhandledExceptionKeepsActorFrame) {
        CheckFatalException(false);
    }

    Y_UNIT_TEST(DeclinedExceptionStillTerminates) {
        CheckFatalException(true);
    }
#endif

} // Y_UNIT_TEST_SUITE(ActorException)
