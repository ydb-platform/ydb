#include "actor_bootstrapped.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

using namespace NActors;

namespace {

    struct TEvCount : TEventLocal<TEvCount, 10348> {};

    class TCountingActor : public TActorBootstrapped<TCountingActor> {
    public:
        explicit TCountingActor(std::atomic<ui64>* completed)
            : Completed(completed)
        {}

        void Bootstrap() {
            Become(&TCountingActor::StateWork);
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvCount, Handle);
        )

        void Handle(TEvCount::TPtr&) {
            Completed->fetch_add(1, std::memory_order_relaxed);
        }

    private:
        std::atomic<ui64>* const Completed;
    };

    THolder<TActorSystemSetup> GetActorSystemSetup(TBasicExecutorPool* pool) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        setup->Executors[0] = pool;
        setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));
        return setup;
    }

}

Y_UNIT_TEST_SUITE(BasicExecutorPoolWaker) {

    Y_UNIT_TEST(StressManual) {
        if (const char* testMode = getenv("ACTORSYSTEM_TEST_MODE"); !testMode || TString(testMode) != "manual") {
            return;
        }

        constexpr ui32 threads = 4;
        constexpr ui32 actors = 64;
        constexpr ui32 producers = 8;
        constexpr ui32 burstSize = 5'000;
        constexpr ui32 rounds = 40;
        constexpr ui32 idleMs = 100;

        TBasicExecutorPoolConfig config;
        config.Threads = threads;
        config.MinThreadCount = 1;
        config.MaxThreadCount = threads;
        config.DefaultThreadCount = threads;
        config.SpinThreshold = 0;
        config.EnableWaker = true;

        auto* executorPool = new TBasicExecutorPool(config, nullptr, nullptr);
        auto setup = GetActorSystemSetup(executorPool);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        std::atomic<ui64> completed = 0;
        TVector<TActorId> actorIds;
        actorIds.reserve(actors);
        for (ui32 actor = 0; actor < actors; ++actor) {
            actorIds.push_back(actorSystem.Register(new TCountingActor(&completed), TMailboxType::HTSwap));
        }

        for (ui32 round = 0; round < rounds; ++round) {
            const i16 threadCount = round % threads + 1;
            executorPool->SetFullThreadCount(threadCount);
            Sleep(TDuration::MilliSeconds(idleMs));
            UNIT_ASSERT_VALUES_EQUAL_C(executorPool->GetFullThreadCount(), threadCount,
                "thread count did not converge in round# " << round);
            const ui64 expected = completed.load(std::memory_order_relaxed) + burstSize;

            TVector<std::thread> producerThreads;
            producerThreads.reserve(producers);
            for (ui32 producer = 0; producer < producers; ++producer) {
                producerThreads.emplace_back([&, producer] {
                    for (ui32 event = producer; event < burstSize; event += producers) {
                        actorSystem.Send(actorIds[event % actors], new TEvCount());
                    }
                });
            }
            for (auto& producer : producerThreads) {
                producer.join();
            }

            const TInstant deadline = TInstant::Now() + TDuration::Seconds(2);
            while (completed.load(std::memory_order_relaxed) < expected && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL_C(completed.load(std::memory_order_relaxed), expected,
                "round# " << round);
            Cerr << "round# " << round << " threads# " << threadCount << " completed# " << expected << Endl;
        }

        actorSystem.Stop();
    }

}
