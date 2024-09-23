#include "harmonizer.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/helpers/pool_stats_collector.h>

using namespace NActors;


/*
    Сценарии тестов без полупотоков:
    - IncreaseThreadsByNeedyState/DecreaseThreadsByHoggishState
    - DecreaseThreadsByStarvedState
    - IncreaseThreadsByExchange/DecreaseThreadsByExchange
*/

#define CHECK_CHANGING_THREADS(stats, inc_needy, inc_exchange, dec_hoggish, dec_starved, dec_exchange) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IncreasingThreadsByNeedyState, inc_needy, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IncreasingThreadsByExchange, inc_exchange, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByHoggishState, dec_hoggish, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByStarvedState, dec_starved, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByExchange, dec_exchange, (stats).ToString());
// end CHECK_CHANGING_THREADS

#define CHECK_STATE(stats, state) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsNeedy, TString(state) == "needy", (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsHoggish, TString(state) == "hoggish", (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsStarved, TString(state) == "starved", (stats).ToString());
// end CHECK_STATE


Y_UNIT_TEST_SUITE(HarmonizerTests) {

    struct TMockExecutorPoolParams {
        i16 DefaultFullThreadCount = 4;
        i16 MinFullThreadCount = 4;
        i16 MaxFullThreadCount = 8;
        float DefaultThreadCount = 4.0f;
        float MinThreadCount = 4.0f;
        float MaxThreadCount = 8.0f;
        i16 Priority = 0;
        TString Name = "MockPool";
        ui32 PoolId = 0;
    };

    struct TCpuConsumptionModel {
        TCpuConsumption value;
        TCpuConsumptionModel() : value() {}
        TCpuConsumptionModel(const TCpuConsumption& other) : value(other) {}
        operator TCpuConsumption() const {
            return value;
        }
        void Increase(const TCpuConsumption& other) {
            value.ElapsedUs += other.ElapsedUs;
            value.CpuUs += other.CpuUs;
            value.NotEnoughCpuExecutions += other.NotEnoughCpuExecutions;
        }
    };

    class TMockExecutorPool : public IExecutorPool {
    public:
        TMockExecutorPool(const TMockExecutorPoolParams& params = TMockExecutorPoolParams())
            : IExecutorPool(params.PoolId)
            , Params(params)
            , ThreadCount(params.DefaultFullThreadCount)
            , ThreadCpuConsumptions(params.MaxFullThreadCount, TCpuConsumption{0.0, 0.0})
        {}

        TMockExecutorPoolParams Params;
        i16 ThreadCount = 0;
        std::vector<TCpuConsumptionModel> ThreadCpuConsumptions;

        i16 GetDefaultFullThreadCount() const override { return Params.DefaultFullThreadCount; }
        i16 GetMinFullThreadCount() const override { return Params.MinFullThreadCount; }
        i16 GetMaxFullThreadCount() const override { return Params.MaxFullThreadCount; }
        void SetFullThreadCount(i16 count) override { ThreadCount = count; }
        i16 GetFullThreadCount() const override { return ThreadCount; }
        float GetDefaultThreadCount() const override { return Params.DefaultThreadCount; }
        float GetMinThreadCount() const override { return Params.MinThreadCount; }
        float GetMaxThreadCount() const override { return Params.MaxThreadCount; }
        i16 GetPriority() const override { return Params.Priority; }
        TString GetName() const override { return Params.Name; }

        // Дополнительные методы из IExecutorPool
        void Prepare(TActorSystem* /*actorSystem*/, NSchedulerQueue::TReader** /*scheduleReaders*/, ui32* /*scheduleSz*/) override {}
        void Start() override {}
        void PrepareStop() override {}
        void Shutdown() override {}
        bool Cleanup() override { return true; }

        ui32 GetReadyActivation(TWorkerContext& /*wctx*/, ui64 /*revolvingCounter*/) override { return 0; }
        void ReclaimMailbox(TMailboxType::EType /*mailboxType*/, ui32 /*hint*/, TWorkerId /*workerId*/, ui64 /*revolvingCounter*/) override {}
        TMailboxHeader* ResolveMailbox(ui32 /*hint*/) override { return nullptr; }

        void Schedule(TInstant /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TMonotonic /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TDuration /*delta*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}

        bool Send(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        bool SpecificSend(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        void ScheduleActivation(ui32 /*activation*/) override {}
        void SpecificScheduleActivation(ui32 /*activation*/) override {}
        void ScheduleActivationEx(ui32 /*activation*/, ui64 /*revolvingCounter*/) override {}
        TActorId Register(IActor* /*actor*/, TMailboxType::EType /*mailboxType*/, ui64 /*revolvingCounter*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId Register(IActor* /*actor*/, TMailboxHeader* /*mailbox*/, ui32 /*hint*/, const TActorId& /*parentId*/) override { return TActorId(); }

        TAffinity* Affinity() const override { return nullptr; }

        ui32 GetThreads() const override { return static_cast<ui32>(ThreadCount); }
        float GetThreadCount() const override { return static_cast<float>(ThreadCount); }

        void IncreaseThreadCpuConsumption(TCpuConsumption consumption, i16 start = 0, i16 count = -1) {
            if (count == -1) {
                count = Params.MaxFullThreadCount - start;
            }
            for (i16 i = start; i < start + count; ++i) {
                ThreadCpuConsumptions[i].Increase(consumption);
            }
        }

        void SetThreadCpuConsumption(TCpuConsumption consumption, i16 start = 0, i16 count = -1) {
            if (count == -1) {
                count = Params.MaxFullThreadCount - start;
            }
            for (i16 i = start; i < start + count; ++i) {
                ThreadCpuConsumptions[i] = consumption;
            }
        }

        TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) override {
            UNIT_ASSERT_GE(threadIdx, 0);
            UNIT_ASSERT_LE(static_cast<ui16>(threadIdx), ThreadCpuConsumptions.size());
            return ThreadCpuConsumptions[threadIdx];
        }
    };

    Y_UNIT_TEST(TestHarmonizerCreation) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        UNIT_ASSERT(harmonizer != nullptr);
    }

    Y_UNIT_TEST(TestAddPool) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        auto mockPool = std::make_unique<TMockExecutorPool>();
        harmonizer->AddPool(mockPool.get());

        auto stats = harmonizer->GetPoolStats(0);
        UNIT_ASSERT_VALUES_EQUAL(stats.PotentialMaxThreadCount, 8);
        UNIT_ASSERT_VALUES_EQUAL(stats.IncreasingThreadsByNeedyState, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DecreasingThreadsByStarvedState, 0);
    }

    Y_UNIT_TEST(TestHarmonize) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        auto mockPool = new TMockExecutorPool();
        harmonizer->AddPool(mockPool);

        harmonizer->Harmonize(currentTs + 1000000);  // 1 second later

        auto stats = harmonizer->GetPoolStats(0);
        Y_UNUSED(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPool->ThreadCount, 4);  // Should start with default

        delete harmonizer;
        delete mockPool;
    }

    Y_UNIT_TEST(TestToNeedyNextToHoggish) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        TMockExecutorPoolParams params;
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        mockPools.emplace_back(new TMockExecutorPool(params));
        params.PoolId = 1;
        mockPools.emplace_back(new TMockExecutorPool(params));
        for (auto& pool : mockPools) {
            harmonizer->AddPool(pool.get());
            pool->SetThreadCpuConsumption(TCpuConsumption{0.0, 0.0}, 0, params.MaxFullThreadCount);
        }

        TCpuConsumptionModel cpuConsumptionModel;

        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);
        mockPools[0]->SetThreadCpuConsumption({59'000'000.0, 59'000'000.0}, 0, params.DefaultFullThreadCount);

        currentTs += Us2Ts(59'000'000);
        harmonizer->Harmonize(currentTs);

        auto stats = harmonizer->GetPoolStats(0);
        
        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 0, 0);
        CHECK_STATE(stats, "needy");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->SetThreadCpuConsumption({0.0, 0.0}, 0, params.DefaultFullThreadCount);
        harmonizer->Harmonize(currentTs);

        stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 1, 0, 0);
        CHECK_STATE(stats, "hoggish");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);
    }

    Y_UNIT_TEST(TestToNeedyNextToStarved) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        TMockExecutorPoolParams params;
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        mockPools.emplace_back(new TMockExecutorPool(params));
        params.PoolId = 1;
        mockPools.emplace_back(new TMockExecutorPool(params));
        for (auto& pool : mockPools) {
            harmonizer->AddPool(pool.get());
            pool->SetThreadCpuConsumption(TCpuConsumption{0.0, 0.0}, 0, params.MaxFullThreadCount);
        }

        TCpuConsumptionModel cpuConsumptionModel;

        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);
        mockPools[0]->IncreaseThreadCpuConsumption({59'000'000.0, 59'000'000.0}, 0, params.DefaultFullThreadCount);

        currentTs += Us2Ts(59'000'000);
        harmonizer->Harmonize(currentTs);

        auto stats = harmonizer->GetPoolStats(0);
        
        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 0, 0);
        CHECK_STATE(stats, "needy");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 43'000'000.0}, 0, 5);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 4);
        harmonizer->Harmonize(currentTs);

        stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 1, 0);
        CHECK_STATE(stats, "starved");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);
    }

    Y_UNIT_TEST(TestExchangeThreads) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        TMockExecutorPoolParams params {
            .DefaultFullThreadCount = 1,
            .MinFullThreadCount = 1,
            .MaxFullThreadCount = 2,
            .DefaultThreadCount = 1.0f,
            .MinThreadCount = 1.0f,
            .MaxThreadCount = 2.0f,
        };
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        mockPools.emplace_back(new TMockExecutorPool(params));
        params.PoolId = 1;
        mockPools.emplace_back(new TMockExecutorPool(params));
        params.PoolId = 2;
        mockPools.emplace_back(new TMockExecutorPool(params));
        for (auto& pool : mockPools) {
            harmonizer->AddPool(pool.get());
            pool->SetThreadCpuConsumption(TCpuConsumption{0.0, 0.0}, 0, params.MaxFullThreadCount);
        }

        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 1);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 1);
        mockPools[2]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 1);
        harmonizer->Harmonize(currentTs);

        auto stats0 = harmonizer->GetPoolStats(0);
        auto stats1 = harmonizer->GetPoolStats(1);
        auto stats2 = harmonizer->GetPoolStats(2);

        CHECK_CHANGING_THREADS(stats0, 0, 0, 0, 0, 0);
        CHECK_CHANGING_THREADS(stats1, 1, 0, 0, 0, 0);
        CHECK_CHANGING_THREADS(stats2, 0, 0, 0, 0, 0);
        CHECK_STATE(stats0, "hoggish");
        CHECK_STATE(stats1, "needy");
        CHECK_STATE(stats2, "needy");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[2]->ThreadCount, 1);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 1);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 2);
        mockPools[2]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 1);
        harmonizer->Harmonize(currentTs);

        stats0 = harmonizer->GetPoolStats(0);
        stats1 = harmonizer->GetPoolStats(1);
        stats2 = harmonizer->GetPoolStats(2);

        CHECK_CHANGING_THREADS(stats0, 0, 1, 0, 0, 0);
        CHECK_CHANGING_THREADS(stats1, 1, 0, 0, 0, 1);
        CHECK_CHANGING_THREADS(stats2, 0, 0, 0, 0, 0);
        CHECK_STATE(stats0, "needy");
        CHECK_STATE(stats1, "needy");
        CHECK_STATE(stats2, "hoggish");
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[2]->ThreadCount, 1);
    }

    Y_UNIT_TEST(TestEnableDisable) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        auto mockPool = new TMockExecutorPool();
        harmonizer->AddPool(mockPool);

        harmonizer->Enable(false);  // Disable harmonizer
        harmonizer->Harmonize(currentTs + 1000000);

        auto stats = harmonizer->GetPoolStats(0);
        Y_UNUSED(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPool->ThreadCount, 4);  // Should not change when disabled

        harmonizer->Enable(true);  // Enable harmonizer
        harmonizer->Harmonize(currentTs + 2000000);

        stats = harmonizer->GetPoolStats(0);
        // Now it might change, but we can't predict exactly how without more complex mocking

        delete harmonizer;
        delete mockPool;
    }

    Y_UNIT_TEST(TestDeclareEmergency) {
        ui64 currentTs = 1000000;
        auto harmonizer = MakeHarmonizer(currentTs);
        auto mockPool = new TMockExecutorPool();
        harmonizer->AddPool(mockPool);

        ui64 emergencyTs = currentTs + 500000;
        harmonizer->DeclareEmergency(emergencyTs);
        harmonizer->Harmonize(emergencyTs);

        // We can't easily test the internal state, but we can verify that Harmonize was called
        // by checking if any stats have changed
        auto stats = harmonizer->GetPoolStats(0);
        Y_UNUSED(stats);
        // Add appropriate assertions based on expected behavior during emergency

        delete harmonizer;
        delete mockPool;
    }
}
