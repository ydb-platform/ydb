#include "kqp_compute_scheduler_service.h"

#include "fwd.h"
#include "tree/dynamic.h"

#include <library/cpp/testing/unittest/registar.h>

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
}

    Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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

    Y_UNIT_TEST(QueriesWithFairShareOverlimit) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(4);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 1; queryId <= 5; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
            query->Demand = 1;
        }

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);

            if (TComputeScheduler::ALLOW_FAIRSHARE_OVERLIMIT) {
                UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, 1, "With allowed fair-share overlimit each query should have at leat 1 fair-share");
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

    Y_UNIT_TEST(LeftFairShareIsDistributed) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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

    Y_UNIT_TEST(WeightedPools) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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

        // to not face integer rounding errors, each will have 4 fair-share
        const std::vector<ui64> demands = {6, 3, 3};
        
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(
                scheduler.AddOrUpdateQuery(databaseId, pools[queryId], queryId, {.Weight = weights[queryId]})
            );
            query->Demand = demands[queryId];  
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 0; queryId < 3; ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, Min(4ul, demands[queryId]));

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 4);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 12);
    }

    Y_UNIT_TEST(ZeroQueries) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());
    }

    Y_UNIT_TEST(ZeroWeightPools) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});

        std::vector<TString> pools = {"pool1", "pool2", "pool3"};
        const std::vector<double> weights = {0, 1, 1};
        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;
        for (NHdrf::TQueryId queryId = 0; queryId < 3; ++queryId) {
            auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, pools[queryId], queryId, {}));
            query->Demand = 5;
        }

        UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());

        for (size_t i = 0; i < queries.size(); ++i) {
            auto querySnapshot = queries[i]->GetSnapshot();
            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT(poolSnapshot);

            if (i != 0) {
                UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 5);
                UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 5);
            } else {
                // should we give 1 fair-share to zero-weight pools?
                if (TComputeScheduler::ALLOW_FAIRSHARE_OVERLIMIT) {
                    UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 1);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 0);
                }
                UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 0);
            }
        }
    }

    Y_UNIT_TEST(AddUpdateQueries) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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

        // fistribution in FIFO ordering
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
    }

    Y_UNIT_TEST(DeleteQueries) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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

        scheduler.RemoveQuery(queries[0]);

        scheduler.UpdateFairShare();

        for (const auto& query : queries) {
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 5);
        }
    }

    Y_UNIT_TEST(AddUpdatePools) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
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
        }

        
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
        }

        scheduler.AddOrUpdatePool(databaseId, "pool1", {.Weight = 2});
        scheduler.UpdateFairShare();

        const std::vector<ui64> fairShares = {3, 2, 2, 2};
        for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
            auto query = queries[queryId];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, fairShares[queryId]);
        }
    }

    Y_UNIT_TEST(DeleteNonExistent) {
        const TOptions options{
            .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            .DelayParams = kDefaultDelayParams,
            .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
        };
        TComputeScheduler scheduler(options.Counters, options.DelayParams);
        scheduler.SetTotalCpuLimit(10);

        const TString databaseId = "db1";
        scheduler.AddOrUpdateDatabase(databaseId, {});
        
        const TString poolId = "pool1";
        scheduler.AddOrUpdatePool(databaseId, poolId, {});

        NHdrf::NDynamic::TQueryPtr query = scheduler.AddOrUpdateQuery(databaseId, poolId, 1, {});
        scheduler.RemoveQuery(query);

        UNIT_ASSERT_EXCEPTION(scheduler.RemoveQuery(nullptr), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.RemoveQuery(query), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdatePool("non-existent", poolId, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", poolId, 1, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery(databaseId, "non-existent", 1, {}), yexception);
        UNIT_ASSERT_EXCEPTION(scheduler.AddOrUpdateQuery("non-existent", "non-existent", 1, {}), yexception);
    }
}
} // namespace NKikimr::NKqp::NScheduler

