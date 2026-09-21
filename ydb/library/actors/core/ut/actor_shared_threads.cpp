#include "actor.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"
#include "actor_benchmark_helper.h"
#include "subsystems/stats.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>
#include <util/system/rwlock.h>
#include <util/system/hp_timer.h>

#include <array>
#include <atomic>
#include <thread>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(SharedThreads) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;
    using TSettings = TActorBenchmark::TSettings;
    using TSendReceiveActorParams = TActorBenchmark::TSendReceiveActorParams;

    class TSignalActor : public TActorBootstrapped<TSignalActor> {
    public:
        explicit TSignalActor(TManualEvent* done)
            : Done(done)
        {}

        void Bootstrap() {
            Done->Signal();
            PassAway();
        }

    private:
        TManualEvent* const Done;
    };

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
        GetActorSystemStats(actorSystem).GetPoolStats(0, poolStats, stats, sharedStats);
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

    Y_UNIT_TEST(AllThreadsSharedRunWithoutLegacySharedFlag) {
        THolder<TActorSystemSetup> setup = TActorBenchmark::GetActorSystemSetup();
        setup->CpuManager.Shared.United = true;
        setup->CpuManager.Basic.emplace_back(TBasicExecutorPoolConfig{
            .PoolId = 0,
            .PoolName = "UnitedPool",
            .Threads = 2,
            .SpinThreshold = 0,
            .MaxThreadCount = 2,
            .DefaultThreadCount = 2,
            .AllThreadsAreShared = true,
        });

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TManualEvent done;
        actorSystem.Register(new TSignalActor(&done), TMailboxType::HTSwap, 0);
        const bool completed = done.WaitT(TDuration::Seconds(5));

        actorSystem.Stop();
        UNIT_ASSERT_C(completed,
            "AllThreadsAreShared pool did not execute an actor without the legacy HasSharedThread flag");
    }

    class TWakerCountingActor : public TActorBootstrapped<TWakerCountingActor> {
    public:
        TWakerCountingActor(std::atomic<ui64>* completed, std::atomic<ui64>* shared)
            : Completed(completed)
            , Shared(shared)
        {}

        void Bootstrap() {
            Become(&TWakerCountingActor::StateWork);
        }

        STFUNC(StateWork) {
            if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                if (TlsThreadContext->IsShared()) {
                    Shared->fetch_add(1, std::memory_order_relaxed);
                }
                Completed->fetch_add(1, std::memory_order_release);
            }
        }

    private:
        std::atomic<ui64>* const Completed;
        std::atomic<ui64>* const Shared;
    };

    void RunSharedWakerBursts(bool sharedOnly, bool mixed, bool foreignOnly, bool singleSharedWorker = false) {
        auto setup = TActorBenchmark::GetActorSystemSetup();
        constexpr ui32 poolCount = 3;
        constexpr ui32 actorsPerPool = 16;
        constexpr ui32 eventsPerPool = 256;
        for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
            TBasicExecutorPoolConfig config;
            config.PoolId = poolId;
            config.PoolName = "SharedWakerTest";
            config.Threads = sharedOnly ? 2 : 3;
            if (singleSharedWorker) {
                config.Threads = 1;
            }
            config.MinThreadCount = config.Threads;
            config.MaxThreadCount = config.Threads;
            config.DefaultThreadCount = config.Threads;
            config.SpinThreshold = 0;
            config.EventsPerMailbox = 1;
            config.HasSharedThread = !sharedOnly;
            config.AllThreadsAreShared = sharedOnly;
            config.EnableWaker = !mixed || poolId != 1;
            if (foreignOnly && (poolId == 2 || singleSharedWorker && poolId != 0)) {
                config.Threads = 0;
                config.MinThreadCount = 0;
                config.MaxThreadCount = 0;
                config.DefaultThreadCount = 0;
                config.HasSharedThread = false;
                config.AllThreadsAreShared = false;
                config.ForcedForeignSlotCount = 2;
            }
            setup->CpuManager.Basic.push_back(config);
        }

        // These outlive the actor system, including forced teardown on failure.
        std::array<std::atomic<ui64>, poolCount> completed{};
        std::array<std::atomic<ui64>, poolCount> shared{};
        TActorSystem actorSystem(setup);
        actorSystem.Start();
        std::array<std::array<TActorId, actorsPerPool>, poolCount> actors;
        for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
            for (auto& actor : actors[poolId]) {
                actor = actorSystem.Register(new TWakerCountingActor(&completed[poolId], &shared[poolId]),
                    TMailboxType::HTSwap, poolId);
            }
        }

        bool drained = true;
        TString failure;
        for (ui32 round = 0; round < 20 && drained; ++round) {
            // Exercise repeated idle/burst transitions as well as concurrent
            // publication. This delay is not used as evidence of a sleep state.
            Sleep(TDuration::MilliSeconds(5));
            std::array<std::thread, poolCount> producers;
            for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
                producers[poolId] = std::thread([&, poolId] {
                    for (ui32 event = 0; event < eventsPerPool; ++event) {
                        actorSystem.Send(actors[poolId][event % actorsPerPool], new TEvents::TEvWakeup());
                    }
                });
            }
            for (auto& producer : producers) {
                producer.join();
            }
            const ui64 expected = (round + 1) * eventsPerPool;
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
                while (completed[poolId].load(std::memory_order_acquire) < expected && TInstant::Now() < deadline) {
                    Sleep(TDuration::MilliSeconds(1));
                }
                const ui64 actual = completed[poolId].load(std::memory_order_acquire);
                if (actual != expected) {
                    drained = false;
                    failure = TStringBuilder() << "pool# " << poolId << " round# " << round
                        << " expected# " << expected << " actual# " << actual;
                    break;
                }
            }
        }
        actorSystem.Stop();
        UNIT_ASSERT_C(drained, failure);
        if (sharedOnly || foreignOnly) {
            const ui32 poolId = foreignOnly ? 2 : 0;
            UNIT_ASSERT_VALUES_EQUAL(shared[poolId].load(), completed[poolId].load());
        }
    }

    Y_UNIT_TEST(WakerMultiplePools) {
        RunSharedWakerBursts(false, false, false);
    }

    Y_UNIT_TEST(WakerSharedOnly) {
        RunSharedWakerBursts(true, false, false);
    }

    Y_UNIT_TEST(WakerMixedWithLegacyPool) {
        RunSharedWakerBursts(false, true, false);
    }

    Y_UNIT_TEST(WakerForeignOnlyPool) {
        RunSharedWakerBursts(true, false, true);
    }

    Y_UNIT_TEST(WakerSingleSharedWorker) {
        RunSharedWakerBursts(true, false, true, true);
    }

} // Y_UNIT_TEST_SUITE(ActorBenchmark)
