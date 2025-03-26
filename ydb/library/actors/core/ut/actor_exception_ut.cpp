#include "actor.h"
#include "actor_bootstrapped.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"
#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>

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
        auto actorId = actorSystem.Register(new TActorHandlerExceptionActor(doneEvent));
        actorSystem.Send(actorId, new TEvents::TEvWakeup);
        doneEvent.WaitI();

        actorSystem.Stop();
    }

} // Y_UNIT_TEST_SUITE(ActorException)
