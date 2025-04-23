#include "harmonizer.h"
#include "executor_pool_mock.h"
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


#define HISTORY_CHECK_POOL(pool, max_thread_count, potential_max_thread_count, current_thread_count, state, operation) \
    do { \
        UNIT_ASSERT_VALUES_EQUAL_C(pool.Threads.size(), max_thread_count, "incorrect max thread count"); \
        UNIT_ASSERT_DOUBLES_EQUAL_C(pool.PotentialMaxThreadCount, potential_max_thread_count, 0.000001, "incorrect potential max thread count"); \
        UNIT_ASSERT_DOUBLES_EQUAL_C(pool.CurrentThreadCount, current_thread_count, 0.000001, "incorrect current thread count"); \
        UNIT_ASSERT_VALUES_EQUAL_C(pool.IsNeedy, TString(state) == "needy", "incorrect needy state"); \
        UNIT_ASSERT_VALUES_EQUAL_C(pool.IsStarved, TString(state) == "starved", "incorrect starved state"); \
        UNIT_ASSERT_VALUES_EQUAL_C(pool.IsHoggish, TString(state) == "hoggish", "incorrect hoggish state"); \
        UNIT_ASSERT_VALUES_EQUAL_C(pool.Operation.ToString(), operation, "incorrect operation"); \
    } while (false) \
// end HISTORY_CHECK_POOL


Y_UNIT_TEST_SUITE(HarmonizerTests) {

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
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
            auto& iteration = history[0];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 0);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 8.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            for (auto &pool : iteration.Pools) {
                HISTORY_CHECK_POOL(pool, 8, 8.0, 4, "hoggish", "NoOperation");
                for (auto &thread : pool.Threads) {
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.UsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ElapsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ParkedCpu.Cpu, 1.0, 0.000001);
                }
            }
        });

        mockPools[0]->IncreaseThreadCpuConsumption({59'000'000.0, 59'000'000.0}, 0, params.DefaultFullThreadCount);
        currentTs += Us2Ts(59'000'000);
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 2);
            auto& iteration = history[1];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 1);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 4.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            HISTORY_CHECK_POOL(iteration.Pools[0], 8, 8.0, 4, "needy", "IncreaseThreadByNeedyState");
            HISTORY_CHECK_POOL(iteration.Pools[1], 8, 4.0, 4, "hoggish", "NoOperation");

            for (ui32 idx = 0; idx < 4; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 0.0, 0.000001);
            }
            for (ui32 idx = 4; idx < 8; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 1.0, 0.000001);
            }

            for (auto &thread : iteration.Pools[1].Threads) {
                UNIT_ASSERT_DOUBLES_EQUAL(thread.UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(thread.ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(thread.ParkedCpu.Cpu, 1.0, 0.000001);
            }
        });

        auto stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 0, 0);
        CHECK_IS_NEEDY(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, params.DefaultFullThreadCount);
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 3);
            auto& iteration = history[2];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 2);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 8.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            HISTORY_CHECK_POOL(iteration.Pools[0], 8, 8.0, 5, "hoggish", "DecreaseThreadByHoggishState");
            HISTORY_CHECK_POOL(iteration.Pools[1], 8, 8.0, 4, "hoggish", "NoOperation");
        });

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
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
            auto& iteration = history[0];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 0);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 8.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            for (auto &pool : iteration.Pools) {
                HISTORY_CHECK_POOL(pool, 8, 8.0, 4, "hoggish", "NoOperation");
                for (auto &thread : pool.Threads) {
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.UsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ElapsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ParkedCpu.Cpu, 1.0, 0.000001);
                }
            }
        });

        mockPools[0]->IncreaseThreadCpuConsumption({59'000'000.0, 59'000'000.0}, 0, params.DefaultFullThreadCount);
        currentTs += Us2Ts(59'000'000);
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 2);
            auto& iteration = history[1];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 1);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 4.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            HISTORY_CHECK_POOL(iteration.Pools[0], 8, 8.0, 4, "needy", "IncreaseThreadByNeedyState");
            HISTORY_CHECK_POOL(iteration.Pools[1], 8, 4.0, 4, "hoggish", "NoOperation");

            for (ui32 idx = 0; idx < 4; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 0.0, 0.000001);
            }
            for (ui32 idx = 4; idx < 8; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 1.0, 0.000001);
            }

            for (auto &thread : iteration.Pools[1].Threads) {
                UNIT_ASSERT_DOUBLES_EQUAL(thread.UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(thread.ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(thread.ParkedCpu.Cpu, 1.0, 0.000001);
            }
        });


        auto stats = harmonizer->GetPoolStats(0);

        CHECK_CHANGING_THREADS(stats, 1, 0, 0, 0, 0);
        CHECK_IS_NEEDY(stats);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[0]->ThreadCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(mockPools[1]->ThreadCount, 4);

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({40'000'000.0, 60'000'000.0}, 0, 5);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 4);
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 3);
            auto& iteration = history[2];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 2);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, -1.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 9.0 - (1.0/0.9)*4.0/6.0*5 - 4, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 2);
            HISTORY_CHECK_POOL(iteration.Pools[0], 8, 4.0, 5, "starved", "DecreaseThreadByStarvedState");
            HISTORY_CHECK_POOL(iteration.Pools[1], 8, 4.0, 4, "needy", "NoOperation");

            for (ui32 idx = 0; idx < 5; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 2.0/3, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 0.0, 0.000001);
            }
            for (ui32 idx = 5; idx < 8; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[0].Threads[idx].ParkedCpu.Cpu, 1.0, 0.000001);
            }

            for (ui32 idx = 0; idx < 4; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].UsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].ElapsedCpu.Cpu, 1.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].ParkedCpu.Cpu, 0.0, 0.000001);
            }
            for (ui32 idx = 4; idx < 8; ++idx) {
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].UsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].ElapsedCpu.Cpu, 0.0, 0.000001);
                UNIT_ASSERT_DOUBLES_EQUAL(iteration.Pools[1].Threads[idx].ParkedCpu.Cpu, 1.0, 0.000001);
            }
        });


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
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
            auto& iteration = history[0];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 0);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 3, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 3);
            for (auto &pool : iteration.Pools) {
                HISTORY_CHECK_POOL(pool, 2, 2, 1, "hoggish", "NoOperation");
                for (auto &thread : pool.Threads) {
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.UsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ElapsedCpu.Cpu, 0.0, 0.000001);
                    UNIT_ASSERT_DOUBLES_EQUAL(thread.ParkedCpu.Cpu, 1.0, 0.000001);
                }
            }
        });

        currentTs += Us2Ts(60'000'000);
        mockPools[0]->IncreaseThreadCpuConsumption({0.0, 0.0}, 0, 1);
        mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 1);
        mockPools[2]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, 1);
        harmonizer->Harmonize(currentTs);
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 2);
            auto& iteration = history[1];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 1);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 1, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 3);
            HISTORY_CHECK_POOL(iteration.Pools[0], 2, 1, 1, "hoggish", "NoOperation");
            HISTORY_CHECK_POOL(iteration.Pools[1], 2, 2, 1, "needy", "IncreaseThreadByNeedyState");
            HISTORY_CHECK_POOL(iteration.Pools[2], 2, 2, 1, "needy", "NoOperation");
        });

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
        harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            UNIT_ASSERT_VALUES_EQUAL(history.size(), 3);
            auto& iteration = history[2];
            UNIT_ASSERT_VALUES_EQUAL(iteration.Iteration, 2);
            UNIT_ASSERT_VALUES_EQUAL(iteration.Ts, currentTs);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.Budget, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.LostCpu, 0.0, 0.000001);
            UNIT_ASSERT_DOUBLES_EQUAL(iteration.FreeSharedCpu, 0.0, 0.000001);

            UNIT_ASSERT_VALUES_EQUAL(iteration.Pools.size(), 3);
            HISTORY_CHECK_POOL(iteration.Pools[0], 2, 2, 1, "needy", "IncreaseThreadByExchange");
            HISTORY_CHECK_POOL(iteration.Pools[1], 2, 1, 2, "needy", "DecreaseThreadByExchange");
            HISTORY_CHECK_POOL(iteration.Pools[2], 2, 1, 1, "hoggish", "NoOperation");
        });

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
                    if (i + ii + iii > budget - 1) {
                        continue;
                    }
                    ui32 localBudget = budget - (i + ii + iii);
                    HARMONIZER_DEBUG_PRINT("first pool", i, "second pool", ii, "third pool", iii, "budget", budget, "local budget", localBudget);
                    currentTs += Us2Ts(60'000'000);
                    mockPools[0]->SetFullThreadCount(i);
                    mockPools[1]->SetFullThreadCount(ii);
                    mockPools[2]->SetFullThreadCount(iii);
                    i16 workingThreads[3] = {
                        std::min<i16>(i, mockPools[0]->ThreadCount),
                        std::min<i16>(ii, mockPools[1]->ThreadCount),
                        std::min<i16>(iii, mockPools[2]->ThreadCount),
                    };
                    mockPools[0]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, workingThreads[0]);
                    mockPools[1]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, workingThreads[1]);
                    mockPools[2]->IncreaseThreadCpuConsumption({60'000'000.0, 60'000'000.0}, 0, workingThreads[2]);
                    harmonizer->Harmonize(currentTs);
                    std::vector<TPoolHarmonizerStats> stats;
                    for (auto& pool : params) {
                        stats.emplace_back(harmonizer->GetPoolStats(pool.PoolId));
                    }
                    for (ui32 poolIdx = 0; poolIdx < params.size(); ++poolIdx) {
                        i16 selfParkedThreads = mockPools[poolIdx]->ThreadCount - workingThreads[poolIdx];
                        i16 localBudgetForPool = Max<i16>(0, localBudget - selfParkedThreads);

                        UNIT_ASSERT_VALUES_EQUAL_C(stats[poolIdx].PotentialMaxThreadCount, std::min<i16>(mockPools[poolIdx]->ThreadCount + localBudgetForPool, params[poolIdx].MaxFullThreadCount),
                            "i# " << i << " ii# " << ii << " iii# " << iii <<
                            " budget# " << budget << " local budget# " << localBudget << " local budget for pool# " << localBudgetForPool <<
                            " local working threads# " << workingThreads[poolIdx] <<
                            " thread count# " << mockPools[poolIdx]->ThreadCount <<
                            " poolIdx# " << poolIdx << " potential max thread count# " << stats[poolIdx].PotentialMaxThreadCount << " max thread count# " << params[poolIdx].MaxFullThreadCount);
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

    Y_UNIT_TEST(TestSharedCheckCorrectnessOfNeedyStateWithOnlySharedThreadConsumption) {
        ui64 currentTs = 1000000;
        std::unique_ptr<IHarmonizer> harmonizer(MakeHarmonizer(currentTs));
        currentTs += Us2Ts(1'000'000);
        harmonizer->Harmonize(currentTs);

        TMockExecutorPoolParams commonParams {
            .DefaultFullThreadCount = 0,
            .MinFullThreadCount = 0,
            .MaxFullThreadCount = 2,
            .DefaultThreadCount = 1.0f,
            .MinThreadCount = 1.0f,
            .MaxThreadCount = 3.0f,
        };
        TMockExecutorPoolParams poolParams {
            .DefaultFullThreadCount = 0,
            .MinFullThreadCount = 0,
            .MaxFullThreadCount = 2,
            .DefaultThreadCount = 1.0f,
            .MinThreadCount = 1.0f,
            .MaxThreadCount = 3.0f,
        };

        std::vector<std::unique_ptr<TMockExecutorPool>> mockPools;
        std::vector<IExecutorPool*> pools;
        mockPools.emplace_back(new TMockExecutorPool(commonParams));
        pools.push_back(mockPools.back().get());
        mockPools.emplace_back(new TMockExecutorPool(poolParams));
        pools.push_back(mockPools.back().get());

    }


}
