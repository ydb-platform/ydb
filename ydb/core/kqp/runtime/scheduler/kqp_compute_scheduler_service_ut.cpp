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

    // Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
    //     const TOptions options{
    //         .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
    //         .DelayParams = kDefaultDelayParams,
    //         .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
    //     };
    //     TComputeScheduler scheduler(options.Counters, options.DelayParams);
    //     scheduler.SetTotalCpuLimit(12);

    //     const TString databaseId = "db1";
    //     scheduler.AddOrUpdateDatabase(databaseId, {});

    //     const TString poolId = "pool1";
    //     scheduler.AddOrUpdatePool(databaseId, poolId, {});

    //     std::vector<NHdrf::NDynamic::TQueryPtr> queries;
    //     for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
    //         auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
    //         query->Demand = 2;
    //     }

    //     scheduler.UpdateFairShare();

    //     for (const auto& query : queries) {
    //         auto querySnapshot = query->GetSnapshot();
    //         UNIT_ASSERT(querySnapshot);
    //         UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 2);
    //     }

    //     auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
    //     UNIT_ASSERT(poolSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 6);

    //     auto* databaseSnapshot = poolSnapshot->GetParent();
    //     UNIT_ASSERT(databaseSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 6);
    // }

    // Y_UNIT_TEST(QueriesWithFairShareOverlimit) {
    //     const TOptions options{
    //         .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
    //         .DelayParams = kDefaultDelayParams,
    //         .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
    //     };
    //     TComputeScheduler scheduler(options.Counters, options.DelayParams);
    //     scheduler.SetTotalCpuLimit(4);

    //     const TString databaseId = "db1";
    //     scheduler.AddOrUpdateDatabase(databaseId, {});

    //     const TString poolId = "pool1";
    //     scheduler.AddOrUpdatePool(databaseId, poolId, {});

    //     std::vector<NHdrf::NDynamic::TQueryPtr> queries;
    //     for (NHdrf::TQueryId queryId = 1; queryId <= 5; ++queryId) {
    //         auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
    //         query->Demand = 1;
    //     }

    //     scheduler.UpdateFairShare();

    //     for (const auto& query : queries) {
    //         auto querySnapshot = query->GetSnapshot();
    //         UNIT_ASSERT(querySnapshot);

    //         if (TComputeScheduler::ALLOW_FAIRSHARE_OVERLIMIT) {
    //             UNIT_ASSERT_VALUES_EQUAL_C(querySnapshot->FairShare, 1, "With allowed fair-share overlimit each query should have at leat 1 fair-share");
    //         } else {
    //             UNIT_ASSERT_LE(querySnapshot->FairShare, 1);
    //         }
    //     }
        
    //     auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
    //     UNIT_ASSERT(poolSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 4);

    //     auto* databaseSnapshot = poolSnapshot->GetParent();
    //     UNIT_ASSERT(databaseSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 4);
    // }

    // Y_UNIT_TEST(LeftFairShareIsDistributed) {
    //     const TOptions options{
    //         .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
    //         .DelayParams = kDefaultDelayParams,
    //         .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
    //     };
    //     TComputeScheduler scheduler(options.Counters, options.DelayParams);
    //     scheduler.SetTotalCpuLimit(10);
        
    //     const TString databaseId = "db1";
    //     scheduler.AddOrUpdateDatabase(databaseId, {});

    //     const TString poolId = "pool1";
    //     scheduler.AddOrUpdatePool(databaseId, poolId, {});

    //     std::vector<NHdrf::NDynamic::TQueryPtr> queries;
    //     for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
    //         auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, poolId, queryId, {}));
    //         query->Demand = 4;
    //     }

    //     scheduler.UpdateFairShare();

    //     for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
    //         auto querySnapshot = queries[queryId - 1]->GetSnapshot();
    //         UNIT_ASSERT(querySnapshot);

    //         if (queryId != 3) {
    //             UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 4);
    //         } else {
    //             UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 2);
    //         }
    //     }

    //     auto* poolSnapshot = queries[0]->GetSnapshot()->GetParent();
    //     UNIT_ASSERT(poolSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 10);

    //     auto* databaseSnapshot = poolSnapshot->GetParent();
    //     UNIT_ASSERT(databaseSnapshot);
    //     UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 10);
    // }

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
        const std::vector<double> weights = {1., 2., 3.};

        for (size_t i = 0; i < pools.size(); ++i) {
            scheduler.AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
        }

        std::vector<NHdrf::NDynamic::TQueryPtr> queries;

        // to not face integer rounding errors, each will have 1 fair-share
        const std::vector<ui64> demands = {6, 3, 2};
        
        for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
            auto query = queries.emplace_back(
                scheduler.AddOrUpdateQuery(databaseId, pools[queryId - 1], queryId, {.Weight = weights[queryId - 1]})
            );
            query->Demand = demands[queryId - 1];  
        }

        scheduler.UpdateFairShare();

        for (size_t queryId = 1; queryId <= 3; ++queryId) {
            auto query = queries[queryId - 1];
            auto querySnapshot = query->GetSnapshot();
            UNIT_ASSERT(querySnapshot);
            UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 3);

            auto* poolSnapshot = querySnapshot->GetParent();
            UNIT_ASSERT(poolSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 3);
        }

        auto* databaseSnapshot = queries[0]->GetSnapshot()->GetParent()->GetParent();
        UNIT_ASSERT(databaseSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(databaseSnapshot->FairShare, 11);
    }

    // Y_UNIT_TEST(ZeroQueries) {
    //     const TOptions options{
    //         .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
    //         .DelayParams = kDefaultDelayParams,
    //         .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
    //     };
    //     TComputeScheduler scheduler(options.Counters, options.DelayParams);
    //     scheduler.SetTotalCpuLimit(10);

    //     const TString databaseId = "db1";
    //     scheduler.AddOrUpdateDatabase(databaseId, {});

    //     const TString poolId = "pool1";
    //     scheduler.AddOrUpdatePool(databaseId, poolId, {});

    //     UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());
    // }

    // Y_UNIT_TEST(ZeroWeightPools) {
    //     const TOptions options{
    //         .Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
    //         .DelayParams = kDefaultDelayParams,
    //         .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
    //     };
    //     TComputeScheduler scheduler(options.Counters, options.DelayParams);
    //     scheduler.SetTotalCpuLimit(10);

    //     const TString databaseId = "db1";
    //     scheduler.AddOrUpdateDatabase(databaseId, {});

    //     std::vector<TString> pools = {"pool1", "pool2", "pool3"};
    //     const std::vector<double> weights = {0, 1, 1};
    //     for (size_t i = 0; i < pools.size(); ++i) {
    //         scheduler.AddOrUpdatePool(databaseId, pools[i], {.Weight = weights[i]});
    //     }

    //     std::vector<NHdrf::NDynamic::TQueryPtr> queries;
    //     for (NHdrf::TQueryId queryId = 1; queryId <= 3; ++queryId) {
    //         auto query = queries.emplace_back(scheduler.AddOrUpdateQuery(databaseId, pools[queryId - 1], queryId, {}));
    //         query->Demand = 5;
    //     }

    //     UNIT_ASSERT_NO_EXCEPTION(scheduler.UpdateFairShare());

    //     for (size_t i = 0; i < queries.size(); ++i) {
    //         auto querySnapshot = queries[i]->GetSnapshot();
    //         auto* poolSnapshot = querySnapshot->GetParent();
    //         UNIT_ASSERT(querySnapshot);
    //         UNIT_ASSERT(poolSnapshot);

    //         if (i != 0) {
    //             UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 5);
    //             UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 5);
    //         } else {
    //             // should we give 1 fair-share to zero-weight pools?
    //             if (TComputeScheduler::ALLOW_FAIRSHARE_OVERLIMIT) {
    //                 UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 1);
    //             } else {
    //                 UNIT_ASSERT_VALUES_EQUAL(querySnapshot->FairShare, 0);
    //             }
    //             UNIT_ASSERT_VALUES_EQUAL(poolSnapshot->FairShare, 0);
    //         }
    //     }
    // }
}
} // namespace NKikimr::NKqp::NScheduler

