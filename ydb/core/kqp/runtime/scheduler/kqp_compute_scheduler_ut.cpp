#include "kqp_compute_scheduler_service.h"

#include "kqp_schedulable_base.h"
#include "kqp_schedulable_task.h"
#include "tree/dynamic.h"
#include "tree/snapshot.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/testlib/helpers.h>

#include <utility>

namespace NKikimr::NKqp::NScheduler {

namespace {
    // hardcoded from ydb/core/protos/table_service_config.proto
    constexpr TDelayParams kDefaultDelayParams{
        .MaxDelay = TDuration::MicroSeconds(3'000'000),
        .MinDelay = TDuration::MicroSeconds(10),
        .AttemptBonus = TDuration::MicroSeconds(5),
        .MaxRandomDelay = TDuration::MicroSeconds(100),
    };

    std::vector<TSchedulableTaskPtr> CreateDemandTasks(const NHdrf::NDynamic::TQueryPtr& query, ui64 demand) {
        std::vector<TSchedulableTaskPtr> tasks;

        // Currently Demand for query snapshot is set as (Demand + ActualDemand) / 2.
        // ActualDemand is set to 0 after each snapshot take, so to avoid updating is every time this workaround was implemented
        tasks.reserve(2 * demand);
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

Y_UNIT_TEST_SUITE(KqpComputeScheduler) {

    Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
        /*
            Scenario:
            - 1 database with 1 pool that has 3 queries with demand 2
            - CPU limit is greater than sum of demands so each database and pool should have FairShare equal to demand,
              and each query gets the whole FairShare of the pool
            - MaxDemand for pools and databases is a sum of children's max demands
        */

        constexpr ui64 kCpuLimit = 12;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        auto scheduler = std::make_unique<TComputeScheduler>(counters, options);
        scheduler->SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler->AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler->AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler->AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler->UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kNQueries * kQueryDemand);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->CpuMaxDemand.load(), kNQueries * kQueryDemand);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kNQueries * kQueryDemand);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->CpuMaxDemand.load(), kNQueries * kQueryDemand);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kNQueries * kQueryDemand);
    }

    /* Scenario:
        - 1 database with 1 pool that has 5 queries with demand 1
        - CPU limit is 4
        - All queries have 4 fair-share
    */
    Y_UNIT_TEST(QueriesWithFairShareEqualAll) {
        constexpr ui64 kCpuLimit = 4;
        constexpr size_t kNQueries = 5;
        constexpr ui64 kQueryDemand = 1;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
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
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kCpuLimit);
        }

        auto* poolSnapshot = queries.front()->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kCpuLimit);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(MaxDemandIsCutOffByLimit) {
        /*
            Scenario:
            - 1 database with 2 pool, each having 3 queries with max demand 4
            - CPU limit is less than sum of max demands so the max demand of databases and pools should be limited by it
            - Checking the max demand cut off on each level, not only on query -> pool
            - The limited demands are equal, so the CPU limit is split between the pools equally
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 4;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
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
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot1->CpuMaxDemand.load(), kCpuLimit);

        auto* poolSnapshot2 = queries[1].front()->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot2);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot2->CpuMaxDemand.load(), kCpuLimit);

        auto* databaseSnapshot = poolSnapshot1->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->CpuMaxDemand.load(), kCpuLimit);

        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot1->FairShare, kCpuLimit / 2);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot2->FairShare, kCpuLimit / 2);
    }

    Y_UNIT_TEST(WeightedDatabase) {
        /*
            Scenario:
            - 3 databases with different weights, each one with 1 pool and 1 query
            - The demand exceeds the CPU limit, so it is distributed by the weights - but db2 is able
              to use only 3, so it takes exactly that and the rest is split between the two remaining
              databases as 1:2, giving 3 and 6
            - Nobody is given more than it demands: an unusable share would idle instead of working
        */
        constexpr ui64 kCpuLimit = 12;
        const std::vector<ui64> kFairShares = {3, 3, 6};

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        auto scheduler = std::make_unique<TComputeScheduler>(counters, options);
        scheduler->SetTotalCpuLimit(kCpuLimit);

        const std::vector<TString> databaseIds = {"db1", "db2", "db3"};
        const std::vector<double> databaseWeights = {1., 2., 2.};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            scheduler->AddOrUpdateDatabase(databaseIds[i], {.Weight = databaseWeights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        std::vector<ui64> queryDemands = {6, 3, 7};

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const TString poolId = "pool" + ToString(i + 1);
            scheduler->AddOrUpdatePool(databaseIds[i], poolId, {});

            auto query = queries.emplace_back(scheduler->AddOrUpdateQuery(databaseIds[i], poolId, i, {}));
            tasks.emplace_back(CreateDemandTasks(query, queryDemands[i]));
        }

        scheduler->UpdateFairShare();

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const auto& query = queries[i];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kFairShares[i]);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kFairShares[i]);
        }

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            auto* databaseSnapshot = queries[i]->GetSnapshot()->GetParent()->GetParent();
            UNIT_ASSERT(databaseSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kFairShares[i]);
        }
    }

    Y_UNIT_TEST(WeightedPools) {
        /*
            Scenario:
            - 1 database with 3 pools with different weights, each one having 1 query
            - The demand exceeds the CPU limit, so it is distributed by the weights - but pool2 is able
              to use only 3, so it takes exactly that and the rest is split between the two remaining
              pools as 1:2, giving 3 and 6
            The test is almost the same as previous, but checks weight distribution for pools
        */
        constexpr ui64 kCpuLimit = 12;
        constexpr size_t kNQueries = 3;
        const std::vector<ui64> kFairShares = {3, 3, 6};

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        auto scheduler = std::make_unique<TComputeScheduler>(counters, options);
        scheduler->SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler->AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        const std::vector<double> weights = {1., 2., 2.};

        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler->AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;

        std::vector<ui64> queryDemands = {6, 3, 7};

        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(
                scheduler->AddOrUpdateQuery(databaseId, pools[queryId], queryId, {})
            );
            tasks.emplace_back(CreateDemandTasks(query, queryDemands[queryId]));
        }

        scheduler->UpdateFairShare();

        for (size_t queryId = 0; queryId < kNQueries; ++queryId) {
            const auto& query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, kFairShares[queryId]);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, kFairShares[queryId]);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(FairShareIsCappedByDemand) {
        /*
            Scenario:
            - 1 database with 2 pools, the demand exceeds the CPU limit
            - The weight of the first pool entitles it to 4 * 8 / 5 = 6, which is way above its demand
            - It takes exactly its demand instead, and the surplus is given to the second pool, so
              the whole CPU limit is put to work rather than idling inside an unusable share
        */
        constexpr ui64 kCpuLimit = 8;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {.Weight = 4});
        scheduler.AddOrUpdatePool(databaseId, "pool2", {.Weight = 1});

        auto query1 = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto query2 = scheduler.AddOrUpdateQuery(databaseId, "pool2", 2, {});
        auto tasks1 = CreateDemandTasks(query1, 2);
        auto tasks2 = CreateDemandTasks(query2, 10);

        scheduler.UpdateFairShare();

        auto* pool1 = query1->GetSnapshot()->GetParent();
        auto* pool2 = query2->GetSnapshot()->GetParent();

        UNIT_ASSERT_VALUES_EQUAL_C(pool1->FairShare, 2, "Nobody gets more than it demands");
        UNIT_ASSERT_VALUES_EQUAL_C(pool2->FairShare, 6, "The surplus is redistributed");
        UNIT_ASSERT_VALUES_EQUAL_C(pool1->FairShare + pool2->FairShare, kCpuLimit, "Nothing is left idling");
    }

    Y_UNIT_TEST(SmallWeightIsNotStarved) {
        /*
            Scenario:
            - 1 database with 2 pools with weights 1 and 10, both demand more than the CPU limit
            - The proportions are 8/11 = 0.7 and 80/11 = 7.3 - rounded down the first pool would get no CPU at all,
              but the CPU lost on rounding is given to the one with the largest fractional part
        */
        constexpr ui64 kCpuLimit = 8;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {.Weight = 1});
        scheduler.AddOrUpdatePool(databaseId, "pool2", {.Weight = 10});

        auto query1 = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto query2 = scheduler.AddOrUpdateQuery(databaseId, "pool2", 2, {});
        auto tasks1 = CreateDemandTasks(query1, 10);
        auto tasks2 = CreateDemandTasks(query2, 10);

        scheduler.UpdateFairShare();

        auto* pool1 = query1->GetSnapshot()->GetParent();
        auto* pool2 = query2->GetSnapshot()->GetParent();

        UNIT_ASSERT_VALUES_EQUAL_C(pool1->FairShare, 1, "The small weight still gets its rounded proportion");
        UNIT_ASSERT_VALUES_EQUAL(pool2->FairShare, 7);
    }

    Y_UNIT_TEST(RoundingLossIsDistributed) {
        /*
            Scenario:
            - 1 database with 3 pools with equal weights, all of them demand more than the CPU limit
            - The proportion of each pool is 8/3 = 2.67 - rounded down it's 2, and the 2 CPUs lost on rounding
              are given to the first two pools, so the whole CPU limit is distributed
            - With more pools than CPUs every CPU is still distributed, one per pool - but the ties are always
              resolved in the same order, so the last pools get nothing while the contention lasts
        */
        constexpr ui64 kCpuLimit = 8;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        auto addPools = [&](size_t count) {
            for (size_t i = 0; i < count; ++i) {
                const NHdrf::TQueryId queryId = queries.size();
                const TString poolId = "pool" + ToString(queryId);
                scheduler.AddOrUpdatePool(databaseId, poolId, {});
                auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                tasks.emplace_back(CreateDemandTasks(query, kCpuLimit));
            }
        };

        auto checkFairShares = [&](const std::vector<ui64>& expected) {
            scheduler.UpdateFairShare();

            ui64 total = 0;
            for (size_t i = 0; i < queries.size(); ++i) {
                auto* pool = queries[i]->GetSnapshot()->GetParent();
                UNIT_ASSERT_VALUES_EQUAL_C(pool->FairShare, expected.at(i), "Wrong fair-share for pool " << i);
                total += pool->FairShare;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(total, kCpuLimit, "Nothing is lost on rounding");
        };

        addPools(3);
        checkFairShares({3, 3, 2});
        checkFairShares({3, 3, 2}); // the same on every snapshot

        addPools(7);
        checkFairShares({1, 1, 1, 1, 1, 1, 1, 1, 0, 0});
        checkFairShares({1, 1, 1, 1, 1, 1, 1, 1, 0, 0});
    }

    /* Scenario:
        - 2 databases with 3 and 2 pools respectively, each having 1 or 2 queries
        - The total demand is under the CPU limit, so every database and pool gets exactly its demand
          (the weights don't matter without contention), and every query - the whole FairShare of its pool
        - The root distributes only the total demand, not the whole CPU limit
    */
    Y_UNIT_TEST(MultipleDatabasesPoolsQueries) {
        constexpr ui64 kCpuLimit = 20;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        auto scheduler = std::make_unique<TComputeScheduler>(counters, options);
        scheduler->SetTotalCpuLimit(kCpuLimit);

        const std::vector<TString> databaseIds = {"db1", "db2"};
        for (const auto& databaseId : databaseIds) {
            scheduler->AddOrUpdateDatabase(databaseId, {});
        }

        const std::vector<std::vector<TString>> poolIds = {{"pool1", "pool2", "pool3"}, {"pool4", "pool5"}};
        const std::vector<std::vector<double>> poolWeights = {{2., 2., 2.}, {2., 2.}};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const auto& databaseId = databaseIds[i];
            for (size_t j = 0; j < poolIds[i].size(); ++j) {
                const auto& poolId = poolIds[i][j];
                scheduler->AddOrUpdatePool(databaseId, poolId, {.Weight = poolWeights[i][j]});
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
                for (size_t k = 0; k < demands[poolIndex].size(); ++k, ++queryId) {
                    auto query = queries.emplace_back(scheduler->AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                    tasks.emplace_back(CreateDemandTasks(query, demands[poolIndex][k]));
                }
            }
        }

        scheduler->UpdateFairShare();

        constexpr size_t kFirstPoolOfFirstDB = 0;
        constexpr size_t kFirstPoolOfSecondDB = 4;

        // this is queries indices corresponding to each pool
        std::vector<ui64> queriesForPoolsIndices = {0, 2, 3, 4, 6};
        std::vector<ui64> poolFairShares = {4, 2, 2, 5, 1};
        std::vector<ui64> databaseFairShares = {8, 6};

        const std::vector<ui64> fairShares = {4, 4, 2, 2, 5, 5, 1};
        for (size_t i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[i]);
        }

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

        auto* root = queries[0]->GetSnapshot()->GetParent()->GetParent()->GetParent();
        UNIT_ASSERT_VALUES_EQUAL(root->FairShare, sumOfFairShares);
    }

    Y_UNIT_TEST(ZeroQueries) {
        /*
            Scenario:
            - UpdateFairShare with no queries shouldn't throw exception
            - With zero demand all nodes even the root should have FairShare 0
        */
        constexpr ui64 kCpuLimit = 12;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
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

        auto* root = databaseSnapshot->GetParent();
        UNIT_ASSERT(root);
        UNIT_ASSERT_VALUES_EQUAL(root->FairShare, 0);
    }

    Y_UNIT_TEST(ZeroLimits) {
        /*
            Scenario:
            - 1 database with 1 pool and 3 queries
            - Database and pool has zero limit, queries shouldn't get any FairShare
        */
        constexpr ui64 kCpuLimit = 12;
        constexpr ui64 kInternalLimit = 0;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.CpuLimit = kInternalLimit});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {.CpuLimit = kInternalLimit});

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
            UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, kInternalLimit, "With zero limits nothing is given");
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
            - Only the database has zero limit, the pool has none
            - Zero Limit and thus zero FairShare should be inherited by pool, so queries shouldn't get any FairShare
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr ui64 kInternalLimit = 0;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 2;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.CpuLimit = kInternalLimit});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler.UpdateFairShare();

        for (const auto& querie : queries) {
            auto querySnapshot = querie->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, kInternalLimit, "With zero limit nothing is given");
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

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
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
            - 1 database with 2 pools, the first one has 1 query with demand 2, the second one - 1 query with demand 10
            - The demand of the first pool is under its proportion, so it gets exactly its demand, and the rest goes to the second
            - Adding one more query with demand 1 to the first pool increases its demand, and the second pool gets less
            - Decreasing the demand of the first query gives it back to the second pool
            - Every query gets the whole FairShare of its pool
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});
        scheduler.AddOrUpdatePool(databaseId, "pool2", {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {}));
        tasks.emplace_back(CreateDemandTasks(queries.back(), 2));

        auto otherQuery = scheduler.AddOrUpdateQuery(databaseId, "pool2", 2, {});
        auto otherTasks = CreateDemandTasks(otherQuery, kCpuLimit);

        auto checkFairShares = [&](ui64 poolFairShare) {
            scheduler.UpdateFairShare();

            for (size_t i = 0; i < queries.size(); ++i) {
                auto querySnapshot = queries[i]->GetSnapshot();
                UNIT_ASSERT(querySnapshot);
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->GetParent()->FairShare, poolFairShare, "Wrong fair-share for the pool");
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, poolFairShare, "Wrong fair-share for query " << i);
            }

            auto otherQuerySnapshot = otherQuery->GetSnapshot();
            UNIT_ASSERT(otherQuerySnapshot);
            UNIT_ASSERT_VALUES_EQUAL_C(otherQuerySnapshot->GetParent()->FairShare, kCpuLimit - poolFairShare, "Wrong fair-share for the other pool");
        };

        checkFairShares(2);

        // Add one more query
        queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, "pool1", 3, {}));
        tasks.emplace_back(CreateDemandTasks(queries.back(), 1));

        checkFairShares(3);

        // Shrink demand of the first query
        ShrinkDemand(tasks[0], 1);

        checkFairShares(2);
    }

    Y_UNIT_TEST(DeleteQueries) {
        /*
            Scenario:
            - 1 database with 2 pools, the first one has 2 queries with demand 3, the second one - 1 query with demand 10
            - Both pools demand more than their proportions, so the CPU limit is split between them equally
            - After deleting the first query the demand of the first pool falls under its proportion,
              so it gets exactly its demand, and the rest goes to the second pool
            - Every query gets the whole FairShare of its pool
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr ui64 kQueryDemand = 3;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});
        scheduler.AddOrUpdatePool(databaseId, "pool2", {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 1; queryId <= 2; ++queryId) {
            queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, "pool1", queryId, {}));
            tasks.emplace_back(CreateDemandTasks(queries.back(), kQueryDemand));
        }

        auto otherQuery = scheduler.AddOrUpdateQuery(databaseId, "pool2", 3, {});
        auto otherTasks = CreateDemandTasks(otherQuery, kCpuLimit);

        auto checkFairShares = [&](ui64 poolFairShare) {
            scheduler.UpdateFairShare();

            for (size_t i = 0; i < queries.size(); ++i) {
                auto querySnapshot = queries[i]->GetSnapshot();
                UNIT_ASSERT(querySnapshot);
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->GetParent()->FairShare, poolFairShare, "Wrong fair-share for the pool");
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, poolFairShare, "Wrong fair-share for query " << i);
            }

            auto otherQuerySnapshot = otherQuery->GetSnapshot();
            UNIT_ASSERT(otherQuerySnapshot);
            UNIT_ASSERT_VALUES_EQUAL_C(otherQuerySnapshot->GetParent()->FairShare, kCpuLimit - poolFairShare, "Wrong fair-share for the other pool");
        };

        checkFairShares(kCpuLimit / 2);

        UNIT_ASSERT(scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(queries[0]->GetId())));
        queries.erase(queries.begin());
        tasks.erase(tasks.begin());

        checkFairShares(kQueryDemand);
    }

    Y_UNIT_TEST(AddUpdatePools) {
        /*
            Scenario:
            - 1 database with 3 pool, each having 1 query with demand 3
            - With 3 pool total demand is under CPU limit so FairShare equals demand
            - Adding one more pool with query with demand 3 should redistribute FairShares with everyone getting 10/4 = 2.5:
              rounded down it's 2 for each, and the 2 CPUs lost on rounding are given to the first two pools
            - Updating the first pool's weight to 2 once again redistribute FairShares: its proportion
              2/5 * 10 = 4 exceeds its demand, so it takes exactly 3 and the remaining 7 is split
              between the other three pools, giving 7/3 = 2 (rounded down) to each and the lost 1 CPU to the second pool
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr size_t kNQueries = 3;
        constexpr ui64 kQueryDemand = 3;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        auto scheduler = std::make_unique<TComputeScheduler>(counters, options);
        scheduler->SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler->AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        for (const auto& pool : pools) {
            scheduler->AddOrUpdatePool(databaseId, pool, {});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<std::vector<TSchedulableTaskPtr>> tasks;
        for (NHdrf::TQueryId queryId = 0; queryId < kNQueries; ++queryId) {
            auto query = queries.emplace_back(scheduler->AddOrUpdateQuery(databaseId, pools[queryId], queryId, {}));
            tasks.emplace_back(CreateDemandTasks(query, kQueryDemand));
        }

        scheduler->UpdateFairShare();

        for (const auto& query : queries) {
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

        scheduler->AddOrUpdatePool(databaseId, "pool4", {});
        pools.emplace_back("pool4");

        NHdrf::NDynamic::TQueryPtr new_query = queries.emplace_back(scheduler->AddOrUpdateQuery(databaseId, "pool4", 4, {}));
        tasks.emplace_back(CreateDemandTasks(new_query, kQueryDemand));

        auto checkFairShares = [&](const std::vector<ui64>& expected) {
            scheduler->UpdateFairShare();

            for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
                auto querySnapshot = queries[queryId]->GetSnapshot();
                UNIT_ASSERT(querySnapshot);
                UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, expected.at(queryId));

                auto* poolSnapshot = querySnapshot->GetParent();
                UNIT_ASSERT(poolSnapshot);
                UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, expected.at(queryId));
            }
        };

        checkFairShares({3, 3, 2, 2});

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);

        scheduler->AddOrUpdatePool(databaseId, "pool1", {.Weight = 2});

        checkFairShares({3, 3, 2, 2});

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, kCpuLimit);
    }

    Y_UNIT_TEST(AddUpdateDeleteNonExistent) {
        /*
            Scenario:
            - Double removing of query or removing of non-existent query doesn't throw, but is reported
            - Adding to or updating non-existent database/pool should throw exception
        */
        constexpr ui64 kCpuLimit = 12;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        NHdrf::TQueryId queryId = 1;
        NHdrf::NDynamic::TQueryPtr query = scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {});

        UNIT_ASSERT(scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(query->GetId())));
        UNIT_ASSERT(!scheduler.RemoveQuery(0));
        UNIT_ASSERT(!scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(query->GetId())));
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdatePool("non-existent", poolId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", poolId, queryId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery(databaseId, "non-existent", queryId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", "non-existent", queryId, {}), yexception);
    }

    Y_UNIT_TEST(StarvingPoolIsNotSatisfied) {
        /*
            Scenario:
            - 1 database with 2 pools, the second one has zero limit
            - The second pool wants CPU, but has no fair-share at all - its adjusted satisfaction is 0,
              while the first pool, which wants nothing more, is fully satisfied
        */
        constexpr ui64 kCpuLimit = 2;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});
        scheduler.AddOrUpdatePool(databaseId, "pool2", {.CpuLimit = 0});

        auto query1 = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto query2 = scheduler.AddOrUpdateQuery(databaseId, "pool2", 2, {});
        auto tasks1 = CreateDemandTasks(query1, kCpuLimit);
        auto tasks2 = CreateDemandTasks(query2, 1);

        scheduler.UpdateFairShare();
        tasks2.at(0)->IncreaseBurstThrottle(TDuration::MilliSeconds(1));
        Sleep(TDuration::MilliSeconds(1));
        scheduler.UpdateFairShare();

        UNIT_ASSERT_VALUES_EQUAL(query2->GetSnapshot()->GetParent()->FairShare, 0);

        auto satisfaction = [&](const TString& poolId) {
            return counters->GetKqpCounters()->GetSubgroup("schedulerPool", poolId)->GetCounter("AdjustedSatisfaction", true)->Val();
        };
        UNIT_ASSERT_VALUES_EQUAL(satisfaction("pool2"), 0);
        UNIT_ASSERT_GT(satisfaction("pool1"), 0);
    }

    Y_UNIT_TEST(ActualDemandCountsWantingTasks) {
        /*
            Scenario:
            - 1 pool with 1 query, which has max demand 4, and only 2 of its tasks want CPU - being throttled
            - The tasks want CPU at the moment of the snapshot, so they are counted, even if their time is not accounted yet
            - The actual demand is not used in the distribution of the fair-share yet
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});

        auto query = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto tasks = CreateDemandTasks(query, 4);
        tasks.at(0)->IncreaseThrottle();
        tasks.at(1)->IncreaseThrottle();

        scheduler.UpdateFairShare();

        auto* pool = query->GetSnapshot()->GetParent();
        UNIT_ASSERT_VALUES_EQUAL(pool->CpuActualDemand, 2);
        UNIT_ASSERT_VALUES_EQUAL_C(pool->GetParent()->CpuActualDemand, 2, "The database sums up the actual demands of its pools");
        UNIT_ASSERT_VALUES_EQUAL_C(pool->FairShare, pool->CpuMaxDemand.load(), "The fair-share is still based on the max demand");
    }

    Y_UNIT_TEST(ParkedTasksWantNothing) {
        /*
            Scenario:
            - 1 pool with 1 query, which has max demand 4, but none of its tasks is running or throttled - e.g. all of them
              are parked on the network
            - The actual demand of the query is 0, but the pool keeps at least 1 to be able to wake up
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});

        auto query = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto tasks = CreateDemandTasks(query, 4);

        scheduler.UpdateFairShare();

        auto querySnapshot = query->GetSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(querySnapshot->CpuActualDemand, 0);

        auto* pool = querySnapshot->GetParent();
        UNIT_ASSERT_VALUES_EQUAL_C(pool->CpuActualDemand, 1, "The pool with tasks keeps at least 1 CPU");
        UNIT_ASSERT_VALUES_EQUAL(pool->CpuMaxDemand.load(), 4);
    }

    Y_UNIT_TEST(ActualDemandCountsWantedTime) {
        /*
            Scenario:
            - 1 pool with 1 query which has 2 tasks, none of them is running or throttled at the moment of the snapshot
            - But during the period the tasks wanted CPU for a long time - so the actual demand is based on it,
              and it is limited by the number of tasks
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});

        auto query = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto tasks = CreateDemandTasks(query, 1);

        scheduler.UpdateFairShare();
        UNIT_ASSERT_VALUES_EQUAL_C(query->GetSnapshot()->GetParent()->CpuActualDemand, 1, "The pool with tasks keeps at least 1 CPU");

        tasks.at(0)->IncreaseBurstThrottle(TDuration::Hours(1));

        // The period between the snapshots shouldn't be zero
        Sleep(TDuration::MilliSeconds(1));
        scheduler.UpdateFairShare();

        UNIT_ASSERT_VALUES_EQUAL_C(query->GetSnapshot()->GetParent()->CpuActualDemand, tasks.size(), "Every task is able to use at most one CPU");
    }

    Y_UNIT_TEST(ActualDemandIsSticky) {
        /*
            Scenario:
            - 1 pool with 1 query which wants 4 CPUs, and then only 1
            - The actual demand of the pool falls only after it stays low for two snapshots in a row,
              so that the pool doesn't lose its share on a short pause between the bursts of work
            - The actual demand grows immediately
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});

        auto query = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto tasks = CreateDemandTasks(query, 4);
        for (size_t i = 0; i < 4; ++i) {
            tasks.at(i)->IncreaseThrottle();
        }

        auto actualDemand = [&] {
            scheduler.UpdateFairShare();
            return query->GetSnapshot()->GetParent()->CpuActualDemand;
        };

        UNIT_ASSERT_VALUES_EQUAL(actualDemand(), 4);

        for (size_t i = 1; i < 4; ++i) {
            tasks.at(i)->DecreaseThrottle();
        }

        // The previous actual demand is kept for one more snapshot
        UNIT_ASSERT_VALUES_EQUAL(actualDemand(), 4);
        UNIT_ASSERT_VALUES_EQUAL(actualDemand(), 1);

        // Grows immediately
        tasks.at(1)->IncreaseThrottle();
        tasks.at(2)->IncreaseThrottle();
        UNIT_ASSERT_VALUES_EQUAL(actualDemand(), 3);
    }

    Y_UNIT_TEST(ActualDemandIsSmoothedPerQuery) {
        /*
            Scenario:
            - 1 pool with 2 queries, the first one wants 4 CPUs, and then the second one wants 4 CPUs instead
            - The actual demand is smoothed per query and summed up above, so that the actual demand of the pool
              is always the sum of its queries' - and for one snapshot both queries keep their 4 CPUs
        */
        constexpr ui64 kCpuLimit = 10;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {});

        auto query1 = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});
        auto query2 = scheduler.AddOrUpdateQuery(databaseId, "pool1", 2, {});
        auto tasks1 = CreateDemandTasks(query1, 2);
        auto tasks2 = CreateDemandTasks(query2, 2);

        auto setWanting = [](std::vector<TSchedulableTaskPtr>& tasks, bool wanting) {
            for (auto& task : tasks) {
                wanting ? task->IncreaseThrottle() : task->DecreaseThrottle();
            }
        };

        auto checkActualDemands = [&](ui64 expected1, ui64 expected2) {
            scheduler.UpdateFairShare();

            auto querySnapshot1 = query1->GetSnapshot();
            auto querySnapshot2 = query2->GetSnapshot();
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot1->CpuActualDemand, expected1);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot2->CpuActualDemand, expected2);
            UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot1->GetParent()->CpuActualDemand, expected1 + expected2,
                "The actual demand of the pool is the sum of its queries'");
        };

        setWanting(tasks1, true);
        checkActualDemands(4, 0);

        setWanting(tasks1, false);
        setWanting(tasks2, true);
        checkActualDemands(4, 4); // the first query keeps its actual demand for one more snapshot
        checkActualDemands(0, 4);
    }

    Y_UNIT_TEST(ThrottleTimeIsNotLostOnStop) {
        /*
            Scenario:
            - The task fails to start because its pool has zero limit, and then stops without any other attempt
            - The time it was throttled is accounted on the stop - there is no next attempt to account it
        */
        constexpr ui64 kCpuLimit = 10;
        constexpr auto kThrottleTime = TDuration::MilliSeconds(10);

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };
        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, "pool1", {.CpuLimit = 0});

        auto query = scheduler.AddOrUpdateQuery(databaseId, "pool1", 1, {});

        // The attempt is made in the past, so there is no need to wait
        TSchedulableBase schedulable({.Query = query, .IsSchedulable = true});
        UNIT_ASSERT_C(schedulable.TryStartExecution(TMonotonic::Now() - kThrottleTime), "Should be throttled with zero limit");
        UNIT_ASSERT(schedulable.IsThrottled());

        schedulable.StopExecution();

        UNIT_ASSERT(!schedulable.IsThrottled());
        UNIT_ASSERT_GE(query->CpuBurstThrottle.load(), kThrottleTime.MicroSeconds());
    }

    Y_UNIT_TEST(StressTest) {
        constexpr ui64 kCpuLimit = 100;

        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        const TOptions options{
            .DelayParams = kDefaultDelayParams,
        };

        TComputeScheduler scheduler(counters, options);
        scheduler.SetTotalCpuLimit(kCpuLimit);

        const TString databaseId = "db1";
        const TString poolId = "pool1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::atomic<NHdrf::TQueryId> queryId = 1;
        std::atomic<bool> shutdown = false;

        auto updateFairShare = [&]() -> void {
            while(!shutdown) {
                scheduler.UpdateFairShare();
            }
        };

        struct TSchedulableActorMock : public TSchedulableBase {
            explicit TSchedulableActorMock(NHdrf::NDynamic::TQueryPtr query)
                : TSchedulableBase({.Query=std::move(query), .IsSchedulable=true}) {}

            void ExecuteAndPassAway(std::atomic<bool>& shutdown) {
                std::thread([&shutdown, this] {
                    std::optional<TDuration> delay;
                    while ((delay = TryStartExecution(TMonotonic::Now())) && !shutdown) {
                        Sleep(*delay);
                    }

                    if (!delay) {
                        StopExecution();
                    }
                }).join();
            }
        };

        auto instantQuery1Task = [&] {
            while(!shutdown) {
                auto query = scheduler.AddOrUpdateQuery(databaseId, poolId, queryId.fetch_add(1), {});
                TSchedulableActorMock(query).ExecuteAndPassAway(shutdown);
                scheduler.RemoveQuery(std::get<NHdrf::TQueryId>(query->GetId()));
            }
        };

        std::thread updateThread(updateFairShare);
        std::list<std::thread> queryThreads;

        // Make sure to use excessive number of threads to cause more jittering and yields
        for (auto i = 0u; i < std::thread::hardware_concurrency() * 4; ++i) {
            queryThreads.emplace_back(instantQuery1Task);
        }

        Sleep(TDuration::Minutes(1));
        shutdown = true;

        updateThread.join();
        for (auto& thread : queryThreads) {
            thread.join();
        }

        // TODO: check proper counters' values
    }

}

namespace {

    // The scheduler adds pools only right under the databases - so the deeper hierarchy is built from the tree itself:
    //
    //   root
    //   └── db
    //       ├── poolA
    //       │   ├── poolA1: query1
    //       │   └── poolA2: query2
    //       └── poolB: query3
    //
    struct THierarchy {
        explicit THierarchy(ui64 totalLimit, const NHdrf::TStaticAttributes& poolAttrs = {})
            : Root(std::make_shared<NHdrf::NDynamic::TRoot>(MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>())))
        {
            Root->TotalLimit = totalLimit;

            auto database = std::make_shared<NHdrf::NDynamic::TDatabase>("db");
            Root->AddDatabase(database);

            auto poolA = AddPool(database, "poolA", poolAttrs);
            auto poolB = AddPool(database, "poolB");
            Query1 = AddQuery(AddPool(poolA, "poolA1"), 1);
            Query2 = AddQuery(AddPool(poolA, "poolA2"), 2);
            Query3 = AddQuery(poolB, 3);
        }

        // The same as TComputeScheduler::UpdateFairShare() does
        void UpdateFairShare() {
            auto snapshot = NHdrf::NSnapshot::TRootPtr(Root->TakeSnapshot());
            snapshot->Update(Root->GetSnapshot());
            Root->SetSnapshot(snapshot);
        }

        NHdrf::NSnapshot::TPool* PoolA1() const { return Query1->GetSnapshot()->GetParent(); }
        NHdrf::NSnapshot::TPool* PoolA2() const { return Query2->GetSnapshot()->GetParent(); }
        NHdrf::NSnapshot::TPool* PoolA() const { return PoolA1()->GetParent(); }
        NHdrf::NSnapshot::TPool* PoolB() const { return Query3->GetSnapshot()->GetParent(); }
        NHdrf::NSnapshot::TPool* Database() const { return PoolA()->GetParent(); }
        NHdrf::NSnapshot::TPool* RootSnapshot() const { return Database()->GetParent(); }

        NHdrf::NDynamic::TRootPtr Root;
        NHdrf::NDynamic::TQueryPtr Query1;
        NHdrf::NDynamic::TQueryPtr Query2;
        NHdrf::NDynamic::TQueryPtr Query3;

    private:
        static NHdrf::NDynamic::TPoolPtr AddPool(const NHdrf::NDynamic::TPoolPtr& parent, const TString& poolId, const NHdrf::TStaticAttributes& attrs = {}) {
            auto pool = std::make_shared<NHdrf::NDynamic::TPool>(poolId, TIntrusivePtr<TKqpCounters>(), attrs); // no counters
            parent->AddPool(pool);
            return pool;
        }

        static NHdrf::NDynamic::TQueryPtr AddQuery(const NHdrf::NDynamic::TPoolPtr& parent, NHdrf::TQueryId queryId) {
            auto query = std::make_shared<NHdrf::NDynamic::TQuery>(queryId, &kDefaultDelayParams, true);
            parent->AddQuery(query);
            return query;
        }
    };

    void Throttle(std::vector<TSchedulableTaskPtr>& tasks, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            tasks.at(i)->IncreaseThrottle();
        }
    }

    void Unthrottle(std::vector<TSchedulableTaskPtr>& tasks, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            tasks.at(i)->DecreaseThrottle();
        }
    }

} // namespace

Y_UNIT_TEST_SUITE(KqpComputeSchedulerHierarchy) {

    Y_UNIT_TEST(ActualDemandIsSummedUp) {
        /*
            Scenario:
            - query1 wants 2 CPUs, query2 has tasks but wants nothing, query3 wants 3 CPUs
            - Every leaf pool gets the actual demand of its query - poolA2 keeps at least 1 CPU,
              since it has tasks - and every element above gets the sum of its children's
        */
        THierarchy hierarchy(16);

        auto tasks1 = CreateDemandTasks(hierarchy.Query1, 2);
        auto tasks2 = CreateDemandTasks(hierarchy.Query2, 2);
        auto tasks3 = CreateDemandTasks(hierarchy.Query3, 2);
        Throttle(tasks1, 2);
        Throttle(tasks3, 3);

        hierarchy.UpdateFairShare();

        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA1()->CpuActualDemand, 2);
        UNIT_ASSERT_VALUES_EQUAL_C(hierarchy.PoolA2()->CpuActualDemand, 1, "The pool with tasks keeps at least 1 CPU");
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA()->CpuActualDemand, 3);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolB()->CpuActualDemand, 3);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Database()->CpuActualDemand, 6);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.RootSnapshot()->CpuActualDemand, 6);
    }

    Y_UNIT_TEST(ActualDemandIsSmoothedAtTheBottom) {
        /*
            Scenario:
            - query1 wants 4 CPUs, and then query3 in the other branch wants 4 CPUs instead, query2 has no tasks at all
            - The actual demand is smoothed only per query, so for one snapshot both of them keep their 4 CPUs,
              and every element above - up to the root - is the sum of its children's
            - Then query1 wants nothing, but its pool keeps at least 1 CPU, and that is summed up as well
        */
        THierarchy hierarchy(16);

        auto tasks1 = CreateDemandTasks(hierarchy.Query1, 2);
        auto tasks3 = CreateDemandTasks(hierarchy.Query3, 2);

        auto check = [&](ui64 poolA1, ui64 poolB) {
            hierarchy.UpdateFairShare();

            UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA1()->CpuActualDemand, poolA1);
            UNIT_ASSERT_VALUES_EQUAL_C(hierarchy.PoolA2()->CpuActualDemand, 0, "The pool without tasks wants nothing");
            UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA()->CpuActualDemand, poolA1);
            UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolB()->CpuActualDemand, poolB);
            UNIT_ASSERT_VALUES_EQUAL(hierarchy.Database()->CpuActualDemand, poolA1 + poolB);
            UNIT_ASSERT_VALUES_EQUAL(hierarchy.RootSnapshot()->CpuActualDemand, poolA1 + poolB);
        };

        Throttle(tasks1, 4);
        check(4, 1);

        Unthrottle(tasks1, 4);
        Throttle(tasks3, 4);
        check(4, 4); // query1 keeps its actual demand for one more snapshot
        check(1, 4);
    }

    Y_UNIT_TEST(ActualDemandIsCutOffByIntermediateLimit) {
        /*
            Scenario:
            - poolA has limit 2, while its children want 2 CPUs each
            - The actual demand of poolA is cut off by its limit, so it's less than the sum of its children's -
              and the elements above sum up the cut off value
        */
        THierarchy hierarchy(16, {.CpuLimit = 2});

        auto tasks1 = CreateDemandTasks(hierarchy.Query1, 2);
        auto tasks2 = CreateDemandTasks(hierarchy.Query2, 2);
        auto tasks3 = CreateDemandTasks(hierarchy.Query3, 2);
        Throttle(tasks1, 2);
        Throttle(tasks2, 2);
        Throttle(tasks3, 1);

        hierarchy.UpdateFairShare();

        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA1()->CpuActualDemand, 2);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA2()->CpuActualDemand, 2);
        UNIT_ASSERT_VALUES_EQUAL_C(hierarchy.PoolA()->CpuActualDemand, 2, "Cut off by the limit");
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolB()->CpuActualDemand, 1);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Database()->CpuActualDemand, 3);
    }

    Y_UNIT_TEST(FairShareIsDistributedDownTheHierarchy) {
        /*
            Scenario:
            - The CPU limit is 8, query1 and query2 have max demand 4 each, query3 - max demand 2
            - The database distributes by max-min: poolB takes its whole max demand 2, and poolA gets the rest 6
            - poolA distributes its 6 between its children equally - 3 and 3
            - Every query gets the whole fair-share of its pool
        */
        THierarchy hierarchy(8);

        auto tasks1 = CreateDemandTasks(hierarchy.Query1, 4);
        auto tasks2 = CreateDemandTasks(hierarchy.Query2, 4);
        auto tasks3 = CreateDemandTasks(hierarchy.Query3, 2);

        hierarchy.UpdateFairShare();

        UNIT_ASSERT_VALUES_EQUAL(hierarchy.RootSnapshot()->FairShare, 8);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Database()->FairShare, 8);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolB()->FairShare, 2);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA()->FairShare, 6);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA1()->FairShare, 3);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.PoolA2()->FairShare, 3);

        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Query1->GetSnapshot()->FairShare, 3);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Query2->GetSnapshot()->FairShare, 3);
        UNIT_ASSERT_VALUES_EQUAL(hierarchy.Query3->GetSnapshot()->FairShare, 2);
    }

}

} // namespace NKikimr::NKqp::NScheduler
