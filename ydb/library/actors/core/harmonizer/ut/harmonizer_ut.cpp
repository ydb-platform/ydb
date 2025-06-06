#include "harmonizer.h"
#include "debug.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/executor_pool_shared.h>
#include <ydb/library/actors/core/executor_thread_ctx.h>
#include <ydb/library/actors/helpers/pool_stats_collector.h>

using namespace NActors;


#define CHECK_CHANGING_THREADS(stats, inc_needy, inc_exchange, dec_hoggish, dec_starved, dec_exchange) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IncreasingThreadsByNeedyState, inc_needy, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IncreasingThreadsByExchange, inc_exchange, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByHoggishState, dec_hoggish, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByStarvedState, dec_starved, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).DecreasingThreadsByExchange, dec_exchange, (stats).ToString());
// end CHECK_CHANGING_THREADS

#define CHECK_CHANGING_HALF_THREADS(stats, received_needy, given_starved, given_hoggish, given_other_needy, returned_starved, returned_other_hoggish) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).ReceivedHalfThreadByNeedyState, received_needy, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).GivenHalfThreadByOtherStarvedState, given_starved, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).GivenHalfThreadByHoggishState, given_hoggish, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).GivenHalfThreadByOtherNeedyState, given_other_needy, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).ReturnedHalfThreadByStarvedState, returned_starved, (stats).ToString()); \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).ReturnedHalfThreadByOtherHoggishState, returned_other_hoggish, (stats).ToString());
// end CHECK_CHANGING_HALF_THREADS

#define CHECK_IS_NEEDY(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsNeedy, true, (stats).ToString()); \
// end CHECK_IS_NEEDY

#define CHECK_IS_NOT_NEEDY(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsNeedy, false, (stats).ToString()); \
// end CHECK_IS_NOT_NEEDY

#define CHECK_IS_HOGGISH(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsHoggish, true, (stats).ToString()); \
// end CHECK_IS_HOGGISH

#define CHECK_IS_NOT_HOGGISH(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsHoggish, false, (stats).ToString()); \
// end CHECK_IS_NOT_HOGGISH

#define CHECK_IS_STARVED(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsStarved, true, (stats).ToString()); \
// end CHECK_IS_STARVED

#define CHECK_IS_NOT_STARVED(stats) \
    UNIT_ASSERT_VALUES_EQUAL_C((stats).IsStarved, false, (stats).ToString()); \
// end CHECK_IS_NOT_STARVED


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

        TString ToString() const {
            return TStringBuilder() << "PoolId: " << PoolId << ", Name: " << Name << ", DefaultFullThreadCount: " << DefaultFullThreadCount << ", MinFullThreadCount: " << MinFullThreadCount << ", MaxFullThreadCount: " << MaxFullThreadCount << ", DefaultThreadCount: " << DefaultThreadCount << ", MinThreadCount: " << MinThreadCount << ", MaxThreadCount: " << MaxThreadCount << ", Priority: " << Priority;
        }
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
        {

        }

        TMockExecutorPoolParams Params;
        i16 ThreadCount = 0;
        std::vector<TCpuConsumptionModel> ThreadCpuConsumptions;
        std::vector<TSharedExecutorThreadCtx*> SharedThreads;

        i16 GetDefaultFullThreadCount() const override { return Params.DefaultFullThreadCount; }
        i16 GetMinFullThreadCount() const override { return Params.MinFullThreadCount; }
        i16 GetMaxFullThreadCount() const override { return Params.MaxFullThreadCount; }
        void SetFullThreadCount(i16 count) override {
            HARMONIZER_DEBUG_PRINT(Params.PoolId, Params.Name, "set full thread count", count);
            ThreadCount = Max(Params.MinFullThreadCount, Min(Params.MaxFullThreadCount, count));
        }
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

        TMailbox* GetReadyActivation(ui64 /*revolvingCounter*/) override { return nullptr; }
        TMailbox* ResolveMailbox(ui32 /*hint*/) override { return nullptr; }

        void Schedule(TInstant /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TMonotonic /*deadline*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}
        void Schedule(TDuration /*delta*/, TAutoPtr<IEventHandle> /*ev*/, ISchedulerCookie* /*cookie*/, TWorkerId /*workerId*/) override {}

        bool Send(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        bool SpecificSend(TAutoPtr<IEventHandle>& /*ev*/) override { return true; }
        void ScheduleActivation(TMailbox* /*activation*/) override {}
        void SpecificScheduleActivation(TMailbox* /*activation*/) override {}
        void ScheduleActivationEx(TMailbox* /*activation*/, ui64 /*revolvingCounter*/) override {}
        TActorId Register(IActor* /*actor*/, TMailboxType::EType /*mailboxType*/, ui64 /*revolvingCounter*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId Register(IActor* /*actor*/, TMailboxCache& /*cache*/, ui64 /*revolvingCounter*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId Register(IActor* /*actor*/, TMailbox* /*mailbox*/, const TActorId& /*parentId*/) override { return TActorId(); }
        TActorId RegisterAlias(TMailbox*, IActor*) override { return TActorId(); }
        void UnregisterAlias(TMailbox*, const TActorId&) override {}

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

        ui64 TimePerMailboxTs() const override {
            return 1000000;
        }

        ui32 EventsPerMailbox() const override {
            return 1;
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
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        std::unique_ptr<TMockExecutorPool> mockPool(new TMockExecutorPool);
        harmonizer->AddPool(mockPool.get());

        harmonizer->Harmonize(currentTs + 1000000);  // 1 second later

        auto stats = harmonizer->GetPoolStats(0);
        Y_UNUSED(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPool->ThreadCount, 4);  // Should start with default
    }

    Y_UNIT_TEST(TestToNeedyNextToHoggish) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
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
        CHECK_IS_NEEDY(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->SetThreadCpuConsumption({0.0, 0.0}, 0, params.DefaultFullThreadCount);
        harmonizer->Harmonize(currentTs);

        stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 1, 0, 0);
        CHECK_IS_HOGGISH(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);
    }

    Y_UNIT_TEST(TestToNeedyNextToStarved) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
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
        CHECK_IS_NEEDY(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({40'000'000.0, 60'000'000.0}, 0, 5);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 4);
        harmonizer->Harmonize(currentTs);

        stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 1, 0);
        CHECK_IS_STARVED(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);
    }

    Y_UNIT_TEST(TestExchangeThreads) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
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
        CHECK_IS_HOGGISH(stats0);
        CHECK_IS_NEEDY(stats1);
        CHECK_IS_NEEDY(stats2);
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
        CHECK_IS_NEEDY(stats0);
        CHECK_IS_NEEDY(stats1);
        CHECK_IS_HOGGISH(stats2);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[2]->ThreadCount, 1);
    }

    Y_UNIT_TEST(TestThreadCounts) {
        ui64 currentTs = 1000000;
        std::vector<TMockExecutorPoolParams> params {
            {
                .DefaultFullThreadCount = 5,
                .MinFullThreadCount = 5,
                .MaxFullThreadCount = 15,
                .DefaultThreadCount = 5.0f,
                .MinThreadCount = 5.0f,
                .MaxThreadCount = 15.0f,
                .PoolId = 0,
            },
            {
                .DefaultFullThreadCount = 5,
                .MinFullThreadCount = 5,
                .MaxFullThreadCount = 15,
                .DefaultThreadCount = 5.0f,
                .MinThreadCount = 5.0f,
                .MaxThreadCount = 15.0f,
                .PoolId = 1,
            },
            {
                .DefaultFullThreadCount = 5,
                .MinFullThreadCount = 5,
                .MaxFullThreadCount = 15,
                .DefaultThreadCount = 5.0f,
                .MinThreadCount = 5.0f,
                .MaxThreadCount = 15.0f,
                .PoolId = 2,
            },
        };
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        i16 budget = 0;
        for (auto& param : params) {
            mockPools.emplace_back(new TMockExecutorPool(param));
            HARMONIZER_DEBUG_PRINT("created pool", mockPools.back()->Params.ToString());
            budget += param.DefaultFullThreadCount;
        }
        for (ui32 poolIdx = 0; poolIdx < params.size(); ++poolIdx) {
            auto &pool = mockPools[poolIdx];
            harmonizer->AddPool(pool.get());
            pool->SetThreadCpuConsumption(TCpuConsumption{0.0, 0.0}, 0, params[poolIdx].MaxFullThreadCount);
        }
        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);

        for (i16 i = 0; i < params[0].MaxFullThreadCount; ++i) {
            for (i16 ii = 0; ii < params[1].MaxFullThreadCount; ++ii) {
                for (i16 iii = 0; iii < params[2].MaxFullThreadCount; ++iii) {
                    if (i + ii + iii > budget) {
                        continue;
                    }
                    ui32 localBudget = budget - (i + ii + iii);
                    HARMONIZER_DEBUG_PRINT("first pool", i, "second pool", ii, "third pool", iii, "budget", budget, "local budget", localBudget);
                    currentTs += Us2Ts(60'000'000);
                    mockPools[0]->SetFullThreadCount(i);
                    mockPools[1]->SetFullThreadCount(ii);
                    mockPools[2]->SetFullThreadCount(iii);
                    mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, std::min<i16>(i, mockPools[0]->ThreadCount));
                    mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, std::min<i16>(ii, mockPools[1]->ThreadCount));
                    mockPools[2]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, std::min(iii, mockPools[2]->ThreadCount));
                    harmonizer->Harmonize(currentTs);
                    std::vector<TPoolHarmonizerStats> stats;
                    for (auto& pool : params) {
                        stats.emplace_back(harmonizer->GetPoolStats(pool.PoolId));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(TestSharedHalfThreads) {
        return;
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        TMockExecutorPoolParams params {
            .DefaultFullThreadCount = 2,
            .MinFullThreadCount = 1,
            .MaxFullThreadCount = 3,
            .DefaultThreadCount = 2.0f,
            .MinThreadCount = 1.0f,
            .MaxThreadCount = 3.0f,
        };
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        std::vector<IExecutorPool*> pools;
        mockPools.emplace_back(new TMockExecutorPool(params));
        pools.push_back(mockPools.back().get());
        params.PoolId = 1;
        mockPools.emplace_back(new TMockExecutorPool(params));
        pools.push_back(mockPools.back().get());
        params.PoolId = 2;
        mockPools.emplace_back(new TMockExecutorPool(params));
        pools.push_back(mockPools.back().get());


        TSharedExecutorPoolConfig sharedConfig;
        sharedConfig.Threads = 3;
        //std::unique_ptr<ISharedExecutorPool> sharedPool(new TMockSharedExecutorPool(sharedConfig, 3, pools));

        for (auto& pool : mockPools) {
            harmonizer->AddPool(pool.get());
        }
        //harmonizer->SetSharedPool(sharedPool.get());

        //for (ui32 i = 0; i < mockPools.size(); ++i) {
            //mockPools[i]->AddSharedThread(sharedPool->GetSharedThread(i));
        //}

        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 2);
        mockPools[1]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 2);
        mockPools[2]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 2);

        harmonizer->Harmonize(currentTs);

        auto stats0 = harmonizer->GetPoolStats(0);
        auto stats1 = harmonizer->GetPoolStats(1);
        auto stats2 = harmonizer->GetPoolStats(2);
        //auto sharedState = sharedPool->GetState();

        CHECK_CHANGING_HALF_THREADS(stats0, 1, 0, 0, 0, 0, 0);
        CHECK_CHANGING_HALF_THREADS(stats1, 0, 0, 0, 1, 0, 0);
        CHECK_IS_NEEDY(stats0);
        CHECK_IS_HOGGISH(stats1);
        CHECK_IS_NEEDY(stats2);
        //UNIT_ASSERT_VALUES_EQUAL(sharedState.BorrowedThreadByPool[0], 1);
        //UNIT_ASSERT_VALUES_EQUAL(sharedState.PoolByBorrowedThread[1], 0);

        currentTs += Us2Ts(60'000'000);

        harmonizer->Harmonize(currentTs);

        stats0 = harmonizer->GetPoolStats(0);
        stats1 = harmonizer->GetPoolStats(1);
        stats2 = harmonizer->GetPoolStats(2);
        //sharedState = sharedPool->GetState();

        CHECK_CHANGING_HALF_THREADS(stats0, 1, 0, 1, 0, 0, 0);
        CHECK_CHANGING_HALF_THREADS(stats1, 0, 0, 0, 1, 0, 1);
        CHECK_IS_HOGGISH(stats0);
        CHECK_IS_HOGGISH(stats1);
        CHECK_IS_HOGGISH(stats2);
        //UNIT_ASSERT_VALUES_EQUAL(sharedState.BorrowedThreadByPool[0], -1);
        //UNIT_ASSERT_VALUES_EQUAL(sharedState.PoolByBorrowedThread[1], -1);
    }

    Y_UNIT_TEST(TestSharedHalfThreadsStarved) {
        return;
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        TMockExecutorPoolParams params {
            .DefaultFullThreadCount = 2,
            .MinFullThreadCount = 1,
            .MaxFullThreadCount = 3,
            .DefaultThreadCount = 2.0f,
            .MinThreadCount = 1.0f,
            .MaxThreadCount = 3.0f,
        };
        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        std::vector<IExecutorPool*> pools;
        mockPools.emplace_back(new TMockExecutorPool(params));
        pools.push_back(mockPools.back().get());
        params.PoolId = 1;
        mockPools.emplace_back(new TMockExecutorPool(params));
        pools.push_back(mockPools.back().get());

        TSharedExecutorPoolConfig sharedConfig;
        sharedConfig.Threads = 2;
        //std::unique_ptr<ISharedExecutorPool> sharedPool(new TMockSharedExecutorPool(sharedConfig, 2, pools));
        

        for (auto& pool : mockPools) {
            harmonizer->AddPool(pool.get());
        }
        //harmonizer->SetSharedPool(sharedPool.get());

        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 2);
        mockPools[1]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 2);
        harmonizer->Harmonize(currentTs);

        auto stats0 = harmonizer->GetPoolStats(0);
        auto stats1 = harmonizer->GetPoolStats(1);
        //auto sharedState = sharedPool->GetState();
        CHECK_CHANGING_HALF_THREADS(stats0, 1, 0, 0, 0, 0, 0);
        CHECK_CHANGING_HALF_THREADS(stats1, 0, 0, 0, 1, 0, 0);
        CHECK_IS_NEEDY(stats0);
        CHECK_IS_HOGGISH(stats1);
        //UNIT_ASSERT_VALUES_EQUAL_C(sharedState.BorrowedThreadByPool[0], 1, sharedState.ToString());
        //UNIT_ASSERT_VALUES_EQUAL_C(sharedState.PoolByBorrowedThread[1], 0, sharedState.ToString());

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({30'000'000.0, 60'000'000.0}, 0, 2);
        mockPools[1]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 2);
        harmonizer->Harmonize(currentTs);

        stats0 = harmonizer->GetPoolStats(0);
        stats1 = harmonizer->GetPoolStats(1);
        //sharedState = sharedPool->GetState();
        //UNIT_ASSERT_VALUES_EQUAL_C(sharedState.BorrowedThreadByPool[0], -1, sharedState.ToString());
        //UNIT_ASSERT_VALUES_EQUAL_C(sharedState.PoolByBorrowedThread[1], -1, sharedState.ToString());
        CHECK_CHANGING_HALF_THREADS(stats0, 1, 1, 0, 0, 0, 0);
        CHECK_CHANGING_HALF_THREADS(stats1, 0, 0, 0, 1, 1, 0);
        CHECK_IS_STARVED(stats0);
        CHECK_IS_HOGGISH(stats1);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 2);
        mockPools[1]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 2);

        harmonizer->Harmonize(currentTs);

        stats0 = harmonizer->GetPoolStats(0);
        stats1 = harmonizer->GetPoolStats(1);

        CHECK_CHANGING_HALF_THREADS(stats0, 2, 1, 0, 0, 0, 0);
        CHECK_CHANGING_HALF_THREADS(stats1, 0, 0, 0, 2, 1, 0);
        CHECK_IS_NEEDY(stats0);
        CHECK_IS_HOGGISH(stats1);
    }

}
