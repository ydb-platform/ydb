#include "actor.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"
#include "actor_benchmark_helper.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <util/system/hp_timer.h>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(SharedThreads) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;
    using TSettings = TActorBenchmark::TSettings;
    using TSendReceiveActorParams = TActorBenchmark::TSendReceiveActorParams;

    class TAliveCounterDecorator : public TDecorator {
    public:
        TAliveCounterDecorator(IActor* actor, TThreadParkPad* pad, std::atomic<ui64> &actorsAlive)
            : TDecorator(THolder(actor))
            , Pad(pad)
            , ActorsAlive(actorsAlive)
        {
            auto x = ActorsAlive.fetch_add(1) + 1;
            ACTORLIB_DEBUG(EDebugLevel::Test, "TAliveCounterDecorator::TAliveCounterDecorator: alive ", x);
        }

        virtual ~TAliveCounterDecorator() {
            auto alive = ActorsAlive.fetch_sub(1) - 1;
            ACTORLIB_DEBUG(EDebugLevel::Test, "TAliveCounterDecorator::~TAliveCounterDecorator: alive ", alive);
            if (alive == 0) {
                Pad->Unpark();
            }
        }

    private:
        TThreadParkPad* Pad;
        std::atomic<ui64> &ActorsAlive;
    };

    template <ESendingType SendingType>
    class TRegistratorActor : public TActorBootstrapped<TRegistratorActor<SendingType>> {
        using TBase = TActorBootstrapped<TRegistratorActor<SendingType>>;
    public:
        TRegistratorActor(std::function<std::vector<IActor*>(ui64 /*iteration*/, ui32 /*poolId*/)> actorFactory, ui32 poolId, ui64 endIteration, TThreadParkPad* pad, std::atomic<ui64> &actorsAlive, bool strictPool)
            : ActorFactory(actorFactory)
            , PoolId(poolId)
            , EndIteration(endIteration)
            , Pad(pad)
            , ActorsAlive(actorsAlive)
            , StrictPool(strictPool)
        {}

        virtual ~TRegistratorActor() {
            ACTORLIB_DEBUG(EDebugLevel::Test, "TRegistratorActor::~TRegistratorActor: ", this->SelfId());
        }

        void Bootstrap(const TActorContext &) {
            ACTORLIB_DEBUG(EDebugLevel::Test, "TSendReceiveActor::Bootstrap: ", this->SelfId());
            this->Become(&TRegistratorActor<SendingType>::StateFunc);
            this->Schedule(TDuration::MicroSeconds(1), new TEvents::TEvWakeup());
        }

        void PassAway() override {
            ACTORLIB_DEBUG(EDebugLevel::Test, "TSendReceiveActor::PassAway: ", this->SelfId());
            this->TBase::PassAway();
        }

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            auto actors = ActorFactory(Iteration++, PoolId);
            for (auto &actor : actors) {
                if (StrictPool) {
                    this->template Register<SendingType>(new TAliveCounterDecorator(actor, Pad, ActorsAlive), TMailboxType::HTSwap, PoolId);
                } else {
                    this->template Register<SendingType>(new TAliveCounterDecorator(actor, Pad, ActorsAlive), TMailboxType::HTSwap);
                }
            }
            if (Iteration >= EndIteration) {
                this->PassAway();
            } else {
                this->Schedule(TDuration::MicroSeconds(1), new TEvents::TEvWakeup());
            }
        }

    private:
        std::function<std::vector<IActor*>(ui64 /*iteration*/, ui32 /*poolId*/)> ActorFactory;
        ui32 PoolId;
        ui64 Iteration = 0;
        const ui64 EndIteration;
        TThreadParkPad* Pad;
        std::atomic<ui64> &ActorsAlive;
        bool StrictPool;
    };


    class TDelayedPassAwayActor : public TActorBootstrapped<TDelayedPassAwayActor> {
        using TBase = TActorBootstrapped<TDelayedPassAwayActor>;
    public:
        TDelayedPassAwayActor(ui32 delay)
            : Delay(delay)
        {}

        void Bootstrap() {
            Become(&TDelayedPassAwayActor::StateFunc);
            if (Delay--) {
                Schedule(TDuration::MicroSeconds(1), new TEvents::TEvWakeup());
            } else {
                PassAway();
            }
        }

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            if (Delay--) {
                Schedule(TDuration::MicroSeconds(1), new TEvents::TEvWakeup());
            } else {
                PassAway();
            }
        }

        void PassAway() override {
            ACTORLIB_DEBUG(EDebugLevel::Test, "TDelayedPassAwayActor::PassAway: ", this->SelfId());
            TBase::PassAway();
        }
    
    private:
        ui32 Delay;
    };


    template <ESendingType SendingType>
    void RunRegistrationAndPassingAwayActors(bool strictPool) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
         TActorBenchmark::AddBasicPool(setup, 1, 1, true);
         TActorBenchmark::AddBasicPool(setup, 1, 1, true);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        std::atomic<ui64> actorsAlive = 0;
        THPTimer Timer;

        auto actorFactory = [](ui64 iteration, ui32) -> std::vector<IActor*> {
            return {new TDelayedPassAwayActor(iteration)};
        };

        Timer.Reset();
        for (ui32 i = 0; i < 50; ++i) {
            ui32 poolId = 0;
            std::unique_ptr<IActor> actor = std::make_unique<TRegistratorActor<SendingType>>(actorFactory, poolId, 1000, &pad, actorsAlive, strictPool);

            THolder<IActor> decoratedActor{
                new TAliveCounterDecorator(
                    actor.release(),
                    &pad,
                    actorsAlive
                )
            };
            actorSystem.Register(decoratedActor.Release(), TMailboxType::HTSwap, poolId);
        }
        for (ui32 i = 0; i < 10; ++i) {
            ui32 poolId = 1;
            std::unique_ptr<IActor> actor = std::make_unique<TRegistratorActor<SendingType>>(actorFactory, poolId, 1000, &pad, actorsAlive, strictPool);
            THolder<IActor> decoratedActor{
                new TAliveCounterDecorator(
                    actor.release(),
                    &pad,
                    actorsAlive
                )
            };
            actorSystem.Register(decoratedActor.Release(), TMailboxType::HTSwap, poolId);
        }

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (4 * TSettings::TotalEventsAmountPerThread);
        actorSystem.Stop();

        TExecutorThreadStats aggregated;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats, sharedStats);
        // Sum all per-thread counters into the 0th element
        for (auto &stat : stats) {
            aggregated.Aggregate(stat);
        }
        for (auto &stat : sharedStats) {
            aggregated.Aggregate(stat);
        }

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsCommon) {
        RunRegistrationAndPassingAwayActors<ESendingType::Common>(false);
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsLazy) {
        RunRegistrationAndPassingAwayActors<ESendingType::Lazy>(false);
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsTail) {
        RunRegistrationAndPassingAwayActors<ESendingType::Tail>(false);
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsStrictPool) {
        RunRegistrationAndPassingAwayActors<ESendingType::Common>(true);
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsTailStrictPool) {
        RunRegistrationAndPassingAwayActors<ESendingType::Tail>(true);
    }

    Y_UNIT_TEST(RegistrationAndPassingAwayActorsLazyStrictPool) {
        RunRegistrationAndPassingAwayActors<ESendingType::Lazy>(true);
    }

} // Y_UNIT_TEST_SUITE(ActorBenchmark)
