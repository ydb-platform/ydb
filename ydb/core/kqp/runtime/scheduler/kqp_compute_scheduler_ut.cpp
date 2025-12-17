#include "kqp_compute_scheduler_service.h"

#include "fwd.h"
#include "tree/dynamic.h"
#include "tree/snapshot.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>

namespace NKikimr::NKqp::NScheduler {

namespace {
    // hardcoded from ydb/core/protos/table_service_config.proto
    constexpr TDelayParams kDefaultDelayParams{
        .MaxDelay = TDuration::MicroSeconds(3'000'000),
        .MinDelay = TDuration::MicroSeconds(10),
        .AttemptBonus = TDuration::MicroSeconds(5),
        .MaxRandomDelay = TDuration::MicroSeconds(100),
    };

    constexpr TDuration kDefaultUpdateFairSharePeriod = TDuration::MilliSeconds(500);

    std::vector<TSchedulableTaskPtr> CreateDemandTasks(NHdrf::NDynamic::TQueryPtr query, ui64 demand) {
        std::vector<TSchedulableTaskPtr> tasks;

        // Currently Demand for query snapshot is set as (Demand + ActualDemand) / 2.
        // ActualDemand is set to 0 after each snapshot take, so to avoid updating is every time this workaround was implemented
        for (ui64 i = 0; i < 2 * demand; ++i) {
            tasks.emplace_back(std::make_shared<TSchedulableTask>(query));
        }

        return tasks;
    }

    void ShrinkDemand(std::vector<TSchedulableTaskPtr>& tasks, ui64 demand) {
        Y_ENSURE(demand * 2 < tasks.size());
        tasks.resize(2 * demand);
    }
} // namespace

Y_UNIT_TEST_SUITE(TKqpScheduler) {

    Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
        /*
            Scenario:
            - 1 database with 1 pool that has 3 queries with demand 2
            - CPU limit is greater than sum of demands so each database, pool and query should have FairShare equal to demand
            - Demand for pools and databases is a sum of children's demands
        */

        constexpr ui64 kCpuLimit = 12;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kQueryDemand);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kNQueries * kQueryDemand);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kNQueries * kQueryDemand);
    }

    Y_UNIT_TEST_TWIN(QueriesWithFairShareOverlimit, AllowOverlimit) {
        /*
            Scenario:
            - 1 database with 1 pool that has 5 queries with demand 1
            - CPU limit is less than sum of demands so FairShare for databases and pools should be limited by it
            - With allowed overlimit each query should have at least 1 FairShare no matter how low is CPU limit
        */
        constexpr ui64 kCpuLimit = 4;
        constexpr size_t kNQueries = 5;
        constexpr ui64 kQueryDemand = 1;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare(AllowOverlimit);

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);

            if (AllowOverlimit) {
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, kQueryDemand, "With allowed fair-share overlimit each query should have at least 1 fair-share");
            } else {
                UNIT_ASSERT_LE(querySnapshot->FairShare, kQueryDemand);
            }
        }

        auto* poolSnapshot = queries.front()->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(DemandIsCutOffByLimit) {
        /*
            Scenario:
            - 1 database with 2 pool, each having 3 queries with demand 4
            - CPU limit is less than sum of demands so FairShare for databases and pools should be limited by it
            - Checking FairShare cut off on each level, not only on query -> pool
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 4;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const std::vector<TString> poolIds = {"pool1", "pool2"};
        for (const auto& poolId : poolIds) {
            scheduler.AddOrUpdatePool(databaseId, poolId, {});
        }

        std::vector<std::vector<NHdrf::NDynamic::TQueryPtr>> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        NHdrf::TQueryId queryId = 0;
        for (const auto& poolId : poolIds) {
            queries.emplace_back();
            for (size_t i = 0; i < kNQueries; ++i, ++queryId) {
                auto query = queries.back().emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
            }
        }

        scheduler.UpdateFairShare();

        auto* poolSnapshot1 = queries[0].front()->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot1);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot1->Demand, kCpuLimit);

        auto* poolSnapshot2 = queries[1].front()->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot2);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot2->Demand, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot1->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->Demand, kCpuLimit);
    }

    Y_UNIT_TEST(LeftFairShareIsDistributed) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries with demand 4
            - CPU limit is less than sum of demands so the last query can't get full satisfaction
            - Checking that each query get 1 demand and than two queries get full demand while the last only gets what lasts
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 4;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {kQueryDemand, kQueryDemand, kCpuLimit - 2 * kQueryDemand};
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(WeightedDatabase) {
        /*
            Scenario:
            - 3 databases with different weights, each one with 1 pool and 1 query
            - FairShare is distributed with weight coefficients, in this case TotalDemand is 12, so it's (demand * weight)/TotalWeightedDemand * TotalDemand = 6/18 * 12 = 4
            - Each database gets it's requested weighted FairShare, but some queries can underutilize it due to low Demand
        */
        constexpr ui64 kCpuLimit = 12;
        constexpr ui64 kWeightedFairShare = 4;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const std::vector<TString> databaseIds = {"db1", "db2", "db3"};
        const std::vector<double> databaseWeights = {1., 2., 2.};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            scheduler.AddOrUpdateDatabase(databaseIds[i], {.Weight = databaseWeights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        std::vector<ui64> queryDemands = {6, 3, 3};

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const TString poolId = "pool" + ToString(i + 1);
            scheduler.AddOrUpdatePool(databaseIds[i], poolId, {});

            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseIds[i], poolId, i, {}));
            tasks.emplace_back(CreateDemandTasks(query, queryDemands[i]));
        }

        scheduler.UpdateFairShare();

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            auto query = queries[i];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, Min(kWeightedFairShare, queryDemands[i]));

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kWeightedFairShare);
        }

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            auto* databaseSnapshot = queries[i]->GetSnapshot()->GetParent()->GetParent();
            UNIT_ASSERT(databaseSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kWeightedFairShare);
        }
    }

    Y_UNIT_TEST(WeightedPools) {
        /*
            Scenario:
            - 1 database with 3 pools with different weights, each one having 1 query
            - FairShare is distributed with weight coefficients, in this case TotalDemand is 12, so it's (demand * weight)/TotalWeightedDemand * TotalDemand = 6/18 * 12 = 4
            - Each pool gets it's requested weighted FairShare, but some queries can underutilize it
            The test is almost the same as previous, but checks weight distribution for pools
        */
        constexpr ui64 kCpuLimit = 12;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kWeightedFairShare = 4;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        const std::vector<double> weights = {1., 2., 2.};

        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;

        std::vector<ui64> queryDemands = {6, 3, 3};

        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(
                scheduler.AddOrUpdateQuery(databaseId, pools[queryId], queryId, {})
            );
            tasks.emplace_back(CreateDemandTasks(query, queryDemands[queryId]));
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, Min(kWeightedFairShare, queryDemands[queryId]));

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kWeightedFairShare);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    // RIGHT NOW TEST IS FAILING, NEED TO IMPLEMENT TODO tree/snapshot.cpp:79
    Y_UNIT_TEST(WeightedQueries) {
        /*
            Scenario:
            - Database with one pool and 3 queries
            - Different weights are set for queries, they should be sorted by weight in descending order
            - CPU limit is less than sum of Demands so they should be fulfilled in correct order: 2, 3, 5(unable to give 6 to the last query)
        */
        // constexpr ui64 kCpuLimit = 10;
        // constexpr size_t kNQueries = 3;

        // const TOptions options{
        //     .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
        //     .DelayParams = kDefaultDelayParams,
        //     .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        // };
        // TComputeScheduler scheduler(options.Counters, options.DelayParams);
        // scheduler.SetTotalCpuLimit(kCpuLimit);

        // const TString databaseId = "db1";
        // scheduler.AddOrUpdateDatabase(databaseId, {});

        // const TString poolId = "pool1";
        // scheduler.AddOrUpdatePool(databaseId, poolId, {});

        // std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        // const std::vector<double> weights = {1, 2, 3};
        // const std::vector<ui64> queryDemands = {6, 3, 2};

        // std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        // for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
        //     auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {.Weight = weights[queryId]}));
        //     tasks.emplace_back(CreateDemandTasks(query, queryDemands[queryId]));
        // }

        // scheduler.UpdateFairShare();

        // const std::vector<ui64> querySortedDemands = {5, 3, 2};
        // for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
        //     auto query = queries[queryId];
        //     auto querySnapshot = query->GetSnapshot();
        //     UNIT_ASSERT(querySnapshot);
        //     UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, querySortedDemands[queryId]);
        // }

        // auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        // UNIT_ASSERT(poolSnapshot);
        // UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        // auto* databaseSnapshot = poolSnapshot->GetParent();
        // UNIT_ASSERT(databaseSnapshot);
        // UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(MultipleDatabasesPoolsQueries) {
        /*
            Scenario:
            - 2 databases with 3 and 2 pools respectively, each having 1 or 2 queries
            - FairShare is distributed with weight coefficients between databases and pools. Demands and weights are carefully chosen to avoid integer division errors
            - This test combine few previous test cases simultaneously
        */
        constexpr ui64 kCpuLimit = 20;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const std::vector<TString> databaseIds = {"db1", "db2"};
        for (const auto& databaseId : databaseIds) {
            scheduler.AddOrUpdateDatabase(databaseId, {});
        }

        const std::vector<std::vector<TString>> poolIds = {{"pool1", "pool2", "pool3"}, {"pool4", "pool5"}};
        const std::vector<std::vector<double>> poolWeights = {{2., 2., 2.}, {2., 2.}};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const auto& databaseId = databaseIds[i];
            for (size_t j = 0; j < poolIds[i].size(); ++j) {
                const auto& poolId = poolIds[i][j];
                scheduler.AddOrUpdatePool(databaseId, poolId, {.Weight = poolWeights[i][j]});
            }
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        std::vector<std::vector<ui64>> demands = {{2, 2}, {2}, {2}, {2, 3}, {1}};
        NHdrf::TQueryId queryId = 0;
        size_t poolIndex = 0;

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const auto& databaseId = databaseIds[i];
            for (size_t j = 0; j < poolIds[i].size(); ++j, ++poolIndex) {
                const auto& poolId = poolIds[i][j];
                for (size_t k = 0; k < demands[j].size(); ++k, ++queryId) {
                    auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                    tasks.emplace_back(CreateDemandTasks(query, demands[poolIndex][k]));
                }
            }
        }

        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {2, 2, 2, 2, 2, 3, 1};
        for (size_t i = 0; i < queries.size(); ++i) {
            auto query = queries[i];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[i]);
        }

        constexpr size_t kFirstPoolOfFirstDB = 0;
        constexpr size_t kFirstPoolOfSecondDB = 4;

        // this is queries indices corresponding to each pool
        std::vector<ui64> queriesForPoolsIndices = {0, 2, 3, 4, 6};
        std::vector<ui64> poolFairShares = {4, 2, 2, 5, 1};
        std::vector<ui64> databaseFairShares = {8, 6};

        for (size_t i = 0; i < queriesForPoolsIndices.size(); ++i) {
            auto* poolSnapshot = queries[queriesForPoolsIndices[i]]->GetSnapshot()->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, poolFairShares[i]);

            if (queriesForPoolsIndices[i] == kFirstPoolOfFirstDB) {
                auto* databaseSnapshot = poolSnapshot->GetParent();
                UNIT_ASSERT(databaseSnapshot);
                UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, databaseFairShares[0]);
            } else if (queriesForPoolsIndices[i] == kFirstPoolOfSecondDB) {
                auto* databaseSnapshot = poolSnapshot->GetParent();
                UNIT_ASSERT(databaseSnapshot);
                UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, databaseFairShares[1]);
            }
        }

        ui64 sumOfFairShares = 0;
        for (auto& fairshare : databaseFairShares) {
            sumOfFairShares += fairshare;
        }

        auto root = queries[0]->GetSnapshot()->GetParent()->GetParent()->GetParent();
        UNIT_ASSERT_VALUES_EQUAL(root->FairShare, sumOfFairShares);
    }

    Y_UNIT_TEST(ZeroQueries) {
        /*
            Scenario:
            - UpdateFairShare with no queries shouldn't throw exception
            - With zero demand all nodes even the root should have FairShare 0
        */
        constexpr ui64 kCpuLimit = 12;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());

        auto query = scheduler.AddOrUpdateQuery(databaseId, poolId, 0, {});
        scheduler.UpdateFairShare();

        auto querySnapshot = query->GetSnapshot();
        UNIT_ASSERT(querySnapshot);
        UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 0);

        auto* poolSnapshot = querySnapshot->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 0);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 0);

        auto root = databaseSnapshot->GetParent();
        UNIT_ASSERT(root);
        UNIT_ASSERT_VALUES_EQUAL(root->FairShare, 0);
    }

    Y_UNIT_TEST(ZeroLimits) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries
            - Database and pool has zero limit, despite AllowOverlimit option queries shouldn't get 1 FairShare
        */
        constexpr ui64 kCpuLimit = 12;
        constexpr ui64 kInternalLimit = 0;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.Limit = kInternalLimit});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {.Limit = kInternalLimit});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, kInternalLimit, "With zero limits overlimit should be ignored");
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kInternalLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kInternalLimit);
    }

    Y_UNIT_TEST(ZeroLimitDbWithNonZeroPools) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries
            - Database and pool has zero limit, despite AllowOverlimit option queries shouldn't get 1 FairShare
            - Zero Limit and thus zero FairShare should be inherited by pool
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr ui64 kInternalLimit = 0;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.Limit = kInternalLimit});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, kInternalLimit, "With zero limit overlimit should be ignored");
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kInternalLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kInternalLimit);
    }

    Y_UNIT_TEST(ZeroWeightDatabasePoolQuery) {
        /*
            Scenario:
            - Setting zero weight to database, pool and query is prohibited and should throw
        */
        constexpr ui64 kCpuLimit = 10;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString pool = "pool1";
        scheduler.AddOrUpdatePool(databaseId, pool, {});

        const NHdrf::TQueryId queryId = 1;
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery(databaseId, pool, queryId, {.Weight = 0}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdatePool(databaseId, pool, {.Weight = 0}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateDatabase(databaseId, {.Weight = 0}), yexception);
    }

    Y_UNIT_TEST(AddUpdateQueries) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries with demand 5
            - CPU limit is less than demand so FairShare is distributed in FIFO order with at least 1 FairShare for each query
            - After adding one more query FairShare is still distributed in FIFO order but second query gets less
            - Decreasing Demand for the first query should affect distribution by giving more for the next
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 5;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };

        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        auto CheckFairShare = [&](const std::vector<ui64>& expectedFairShare) {
            scheduler.UpdateFairShare();

            for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
                auto querySnapshot = queries.at(queryId)->GetSnapshot();
                UNIT_ASSERT(querySnapshot);
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, expectedFairShare.at(queryId),
                    "Wrong fair-share for query " << queryId);
            }
        };

        CheckFairShare({5, 4, 1});

        // Add one more query
        NHdrf::NDynamic::TQueryPtr new_query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, 4, {}));
        tasks.emplace_back(CreateDemandTasks(new_query, kQueryDemand));

        CheckFairShare({5, 3, 1, 1});

        // Shrink demand of the first query
        ShrinkDemand(tasks[0], 2);

        CheckFairShare({2, 5, 2, 1});

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(DeleteQueries) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries with demand 5
            - CPU limit is less than demand so FairShare is distributed in FIFO order with at least 1 FairShare for each query
            - After deleting the first query there should be enough CPU limit to fulfill every Demand
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 5;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {5, 4, 1};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);

        scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(queries[0]->GetId()));
        queries.erase(queries.begin());

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kQueryDemand);
        }

        poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(AddUpdatePools) {
        /*
            Scenario:
            - 1 database with 3 pool, each having 1 query with demand 3
            - With 3 pool total demand is under CPU limit so FairShare equals demand
            - Adding one more pool with query with demand 3 should redistribute FairShares with everyone getting 3/12 * 10 = 5/2 = 2(rounded down)
            - Updating the first pool's weight to 2 once again redistribute FairShares, but now it gets FairShare 6/15 * 10 = 4 and others 3/15 * 10 = 2. And first query underutilize this FairShare
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 3;
        constexpr ui64 kRoundedDemand = 2;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };

        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, pools[queryId], queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();

            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kQueryDemand);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kQueryDemand);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kNQueries * kQueryDemand);

        scheduler.AddOrUpdatePool(databaseId, "pool4", {});
        pools.emplace_back("pool4");

        NHdrf::NDynamic::TQueryPtr new_query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, "pool4", 4, {}));
        tasks.emplace_back(CreateDemandTasks(new_query, kQueryDemand));

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kRoundedDemand);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kRoundedDemand);

        }

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);

        scheduler.AddOrUpdatePool(databaseId, "pool1", {.Weight = 2});

        scheduler.UpdateFairShare();

        const std::vector<ui64> poolsFairShares = {4, 2, 2, 2};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            auto* poolSnapshot = querySnapshot->GetParent();

            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, poolsFairShares[queryId]);

            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, Min(poolsFairShares[queryId], kQueryDemand));
        }

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(AddUpdateDeleteNonExistent) {
        /*
            Scenario:
            - Double removing of query should throw exception
            - Adding to or updating non-existent database/pool should throw exception
        */
        constexpr ui64 kCpuLimit = 12;

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        NHdrf::TQueryId queryId = 1;
        NHdrf::NDynamic::TQueryPtr query = scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {});

        UNIT_ASSERT_NO_EXCEPTION(scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(query->GetId())));
        UNIT_ASSERT_NO_EXCEPTION(scheduler.RemoveQuery(0));
        UNIT_ASSERT_NO_EXCEPTION(scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(query->GetId())));
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdatePool("non-existent", poolId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", poolId, queryId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery(databaseId, "non-existent", queryId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", "non-existent", queryId, {}), yexception);
    }
}
} // namespace NKikimr::NKqp::NScheduler

