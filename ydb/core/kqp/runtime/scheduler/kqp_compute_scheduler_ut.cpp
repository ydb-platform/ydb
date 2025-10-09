#include "kqp_compute_scheduler_service.h"

#include "fwd.h"
#include "tree/dynamic.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NKqp::NScheduler {

Y_UNIT_TEST_SUITE(TKqpScheduler) {

namespace {
    // hardcoded from ydb/core/protos/table_service_config.proto
    constexpr TDelayParams kDefaultDelayParams{
        .MaxDelay = TDuration::MicroSeconds(3'000'000),
        .MinDelay = TDuration::MicroSeconds(10),
        .AttemptBonus = TDuration::MicroSeconds(5),
        .MaxRandomDelay = TDuration::MicroSeconds(100),
    };

    constexpr TDuration kDefaultUpdateFairSharePeriod = TDuration::MilliSeconds(500);
}

    Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
        /*
            Each query has demand 2. And fair share 6 is under total limit 12.
        */

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(12);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 2;
        }

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 2);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 6);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 6);
    }

    Y_UNIT_TEST_TWIN(QueriesWithFairShareOverlimit, AllowOverlimit) {
        /*
            Total limit is 4 but due to fair share overlimit rule each query should have 1 fair-share.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(4);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 5; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 1;
        }

        scheduler.UpdateFairShare(AllowOverlimit);

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);

            if (AllowOverlimit) {
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, 1, "With allowed fair-share overlimit each query should have at least 1 fair-share");
            } else {
                UNIT_ASSERT_LE(querySnapshot->FairShare, 1);
            }
        }
        
        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 4);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 4);
    }

    Y_UNIT_TEST(DemandIsCutOffByLimit) {
        /*
            Total limit is 10 but each query has demand 12..
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const std::vector<TString> poolIds = {"pool1", "pool2"};
        for (const auto& poolId : poolIds) {
            scheduler.AddOrUpdatePool(databaseId, poolId, {});
        }
    
        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        NHdrf::TQueryId queryId = 0;
        for (const auto& poolId : poolIds) {
            for (size_t i = 0; i < 3; ++i, ++queryId) {
                auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                query->Demand = 4;
            }
        }

        scheduler.UpdateFairShare();

        auto* poolSnapshot1 = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot1);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot1->Demand, 10);

        auto* poolSnapshot2 = queries[3]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot2);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot2->Demand, 10);
    
        auto* databaseSnapshot = poolSnapshot1->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->Demand, 10);
    }

    Y_UNIT_TEST(LeftFairShareIsDistributed) {
        /*
            Total limit is 10 but due to fair share overlimit rule each query gets 1 fair-share.
            Left fair-share is distributed between queries in FIFO order.
        */

        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);
        
        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 4;
        }

        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {4, 4, 2};
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 10);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);
    }

    Y_UNIT_TEST(WeightedDatabase) {
        /*
            Total limit is 12. Database has weight 2. Queries have demands 6, 3, 3.
            Weighted fair share for each query is 6, so each query will get (6 * 12) / 18 = 4 fair-share.
            Each query should have demands 4, 3, 3.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(12);

        const std::vector<TString> databaseIds = {"db1", "db2", "db3"};
        const std::vector<double> databaseWeights = {1., 2., 2.};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            scheduler.AddOrUpdateDatabase(databaseIds[i], {.Weight = databaseWeights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        std::vector<ui64> queryDemands = {6, 3, 3};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const TString poolId = "pool" + ToString(i + 1);
            scheduler.AddOrUpdatePool(databaseIds[i], poolId, {});

            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseIds[i], poolId, i, {}));
            query->Demand = queryDemands[i];
        }

        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {4, 3, 3};
        for (size_t i = 0; i < databaseIds.size(); ++i) {
            auto query = queries[i];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[i]);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 4);
        }

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            auto* databaseSnapshot = queries[i]->GetSnapshot()->GetParent()->GetParent();
            UNIT_ASSERT(databaseSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 4);
        }
    }

    Y_UNIT_TEST(WeightedPools) {
        /*
            Total limit is 12. Pools have weights 1, 2, 2. Queries have demands 6, 3, 3.
            Weighted fair share for each pool is 6, so each pool will get (6 * 12) / 18 = 4 fair-share.
            Each query should have demands 4, 3, 3.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(12);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        const std::vector<double> weights = {1., 2., 2.};

        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;

        // to not face integer rounding errors, each will get 4 fair-share
        std::vector<ui64> demands = {6, 3, 3};
        
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(
                scheduler.AddOrUpdateQuery(databaseId, pools[queryId], queryId, {.Weight = weights[queryId]})
            );
            query->Demand = demands[queryId];  
        }

        scheduler.UpdateFairShare();

        demands = {4, 3, 3};
        for (size_t queryId = 0; queryId < 3; ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, demands[queryId]);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 4);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 12);
    }

    Y_UNIT_TEST(WeightedQueries) {
        /*
            Check that queries weight doesn't affect fair share.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(12);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        const std::vector<double> weights = {1, 2, 3};
        const std::vector<ui64> demands = {6, 3, 2};

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {.Weight = weights[queryId]}));
            query->Demand = demands[queryId];
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, demands[queryId]);

        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 11);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 11);
    }

    Y_UNIT_TEST(MultipleDatabasesPoolsQueries) {
        /*
            Check that fair share is distributed correctly when there are multiple databases, pools and queries.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(20);

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
        std::vector<std::vector<ui64>> demands = {{2, 2}, {2}, {2}, {2, 3}, {1}};
        NHdrf::TQueryId queryId = 0;
        size_t poolIndex = 0;

        for (size_t i = 0; i < databaseIds.size(); ++i) {
            const auto& databaseId = databaseIds[i];
            for (size_t j = 0; j < poolIds[i].size(); ++j, ++poolIndex) {
                const auto& poolId = poolIds[i][j];
                for (size_t k = 0; k < demands[j].size(); ++k, ++queryId) {
                    auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
                    query->Demand = demands[poolIndex][k];
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

        std::vector<ui64> poolIndexes = {0, 2, 3, 4, 6};
        std::vector<ui64> poolFairShares = {4, 2, 2, 5, 1};
        std::vector<ui64> databaseFairShares = {8, 6};

        for (size_t i = 0; i < poolIndexes.size(); ++i) {
            auto* poolSnapshot = queries[poolIndexes[i]]->GetSnapshot()->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, poolFairShares[i]);

            if (poolIndexes[i] == 0) {
                auto* databaseSnapshot = poolSnapshot->GetParent();
                UNIT_ASSERT(databaseSnapshot);
                UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, databaseFairShares[0]);
            } else if (poolIndexes[i] == 4) {
                auto* databaseSnapshot = poolSnapshot->GetParent();
                UNIT_ASSERT(databaseSnapshot);
                UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, databaseFairShares[1]);
            }
        }
    }

    Y_UNIT_TEST(ZeroQueries) {
        /*
            Check that update fair share works when there are no queries
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());
    }

    Y_UNIT_TEST(ZeroLimits) {
        /*
            Trying to set zero limit should still give 1 fair share for each query.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.Limit = 0});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {.Limit = 0});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 2;
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 1);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 0);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 0);
    }

    Y_UNIT_TEST(ZeroLimitDbWithNonZeroPools) {
        /*
            Trying to set zero limit for database with non-zero pools should still give at least 1 fair share for each query.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {.Limit = 0});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 3;
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto querySnapshot = queries[queryId]->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 1);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 0);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 0);
    }

    Y_UNIT_TEST(ZeroWeightDatabasePoolQuery) {
        /*
            Trying to set zero weight for database, pool or query should throw an exception.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

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
            Trying to add or update query should correctly redistribute fair share.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
    
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        
        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 5;
        }

        scheduler.UpdateFairShare();

        std::vector<ui64> fairShares = {5, 4, 1};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        NHdrf::NDynamic::TQueryPtr new_query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, 4, {}));
        new_query->Demand = 5;
        scheduler.UpdateFairShare();

        // distribution in FIFO ordering
        fairShares = {5, 3, 1, 1};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        queries[0]->Demand = 2;
        scheduler.UpdateFairShare();

        fairShares = {2, 5, 2, 1};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }

        auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 10);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);
    }

    Y_UNIT_TEST(DeleteQueries) {
        /*
            Trying to delete query should correctly redistribute fair share.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        
        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 5;
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
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 10);

        auto* databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);

        scheduler.RemoveQuery(queries[0]);

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 5);
        }

        poolSnapshot = queries[0]->GetSnapshot()->GetParent();
        UNIT_ASSERT(poolSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 10);

        databaseSnapshot = poolSnapshot->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);
    }

    Y_UNIT_TEST(AddUpdatePools) {
        /*
            Trying to add or update pool should correctly redistribute fair share.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };

        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        
        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, pools[queryId - 1], queryId, {}));
            query->Demand = 3;
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 3);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 3);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 9);

        scheduler.AddOrUpdatePool(databaseId, "pool4", {});
        pools.emplace_back("pool4");

        NHdrf::NDynamic::TQueryPtr new_query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, "pool4", 4, {}));
        new_query->Demand = 3;

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 2);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 2);

        }

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);

        scheduler.AddOrUpdatePool(databaseId, "pool1", {.Weight = 2});
        scheduler.UpdateFairShare();

        const std::vector<ui64> queriesFairShares = {3, 2, 2, 2};
        const std::vector<ui64> poolsFairShares = {4, 2, 2, 2};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, queriesFairShares[queryId]);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, poolsFairShares[queryId]);
        }

        databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);
    }

    Y_UNIT_TEST(DeleteNonExistent) {
        /*
            Trying to delete non-existent query should throw an exception.
        */
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = kDefaultUpdateFairSharePeriod
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        
        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        NHdrf::NDynamic::TQueryPtr query = scheduler.AddOrUpdateQuery(databaseId, poolId, 1, {});

        UNIT_ASSERT_NO_EXCEPTION(scheduler.RemoveQuery(query));
        UNIT_ASSERT_EXCEPTION(scheduler.RemoveQuery(nullptr), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.RemoveQuery(query), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdatePool("non-existent", poolId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", poolId, 1, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery(databaseId, "non-existent", 1, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", "non-existent", 1, {}), yexception);
    }
}
} // namespace NKikimr::NKqp::NScheduler

