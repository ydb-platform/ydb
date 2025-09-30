#include "kqp_compute_scheduler_service.h"

#include "fwd.h"
#include "tree/dynamic.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NScheduler {

    Y_UNIT_TEST_SUITE(TKqpScheduler) {
        Y_UNIT_TEST(SingleDatabasePoolQueryStructure) {
            NMonitoring::TDynamicCounterPtr dynamicCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            const TOptions options{
                .Counters = MakeIntrusive<TKqpCounters>(dynamicCounters),
                // hardcoded from ydb/core/protos/table_service_config.proto
                .DelayParams = {
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
                .UpdateFairSharePeriod = TDuration::MilliSeconds(500)
            };

            TComputeScheduler scheduler(options.Counters, options.DelayParams);
            scheduler.SetTotalCpuLimit(10);

            const TString databaseId = "db1";
            const TString poolId = "pool1";
            const NHdrf::TQueryId queryId1 = 1;
            const NHdrf::TQueryId queryId2 = 2;
            const NHdrf::TQueryId queryId3 = 3;

            scheduler.AddOrUpdateDatabase(databaseId, {});
            scheduler.AddOrUpdatePool(databaseId, poolId, {});
            std::vector<NHdrf::NDynamic::TQueryPtr> queries;
            for (const auto& queryId : {queryId1, queryId2, queryId3}) {
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
    }
} // namespace NKikimr::NKqp::NScheduler

