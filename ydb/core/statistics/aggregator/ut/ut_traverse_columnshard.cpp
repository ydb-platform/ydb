#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(TraverseColumnShard) {

    Y_UNIT_TEST(TraverseColumnTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeResolve) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        int eventCount = 0;
        auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvResolveKeySetResult>([&](auto&) {
            eventCount++;
        });

        runtime.WaitFor("TEvResolveKeySetResult", [&]{ return eventCount == 3; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeReqDistribution) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvHive::TEvRequestTabletDistribution>([&](auto&){
            eventSeen = true;
        });

        runtime.WaitFor("TEvRequestTabletDistribution", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>([&](auto&){
            eventSeen = true;
        });

        runtime.WaitFor("TEvAggregateStatistics", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeSave) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>([&](auto&){
            eventSeen = true;
        });

        runtime.WaitFor("TEvAggregateStatisticsResponse", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletInAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvStatisticsRequest>([&](auto&){
            observerCount++;
        });

        runtime.WaitFor("5th TEvStatisticsRequest", [&]{ return observerCount == 5; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableHiveDistributionZeroNodes) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvHive::TEvResponseTabletDistribution>(
            [&](TEvHive::TEvResponseTabletDistribution::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                auto& record = ev->Get()->Record;

                NKikimrHive::TEvResponseTabletDistribution newRecord;
                std::vector<ui64> unknownTablets;

                for (auto& node : record.GetNodes()) {
                    auto* newNode = newRecord.AddNodes();
                    newNode->SetNodeId(node.GetNodeId());
                    int index = 0;
                    for (auto tabletId : node.GetTabletIds()) {
                        if (index < 7) {
                            newNode->AddTabletIds(tabletId);
                        } else {
                            unknownTablets.push_back(tabletId);
                        }
                        ++index;
                    }
                }
                auto* unknownNode = newRecord.AddNodes();
                unknownNode->SetNodeId(0);
                for (auto tabletId : unknownTablets) {
                    unknownNode->AddTabletIds(tabletId);
                }

                record.Swap(&newRecord);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableHiveDistributionAbsentNodes) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvHive::TEvResponseTabletDistribution>(
            [&](TEvHive::TEvResponseTabletDistribution::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                auto& record = ev->Get()->Record;

                NKikimrHive::TEvResponseTabletDistribution newRecord;

                for (auto& node : record.GetNodes()) {
                    auto* newNode = newRecord.AddNodes();
                    newNode->SetNodeId(node.GetNodeId());
                    int index = 0;
                    for (auto tabletId : node.GetTabletIds()) {
                        if (index < 7) {
                            newNode->AddTabletIds(tabletId);
                        }
                        ++index;
                    }
                }

                record.Swap(&newRecord);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableAggrStatUnavailableNode) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>(
            [&](TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                auto& record = ev->Get()->Record;

                NKikimrStat::TEvAggregateStatisticsResponse newRecord;
                newRecord.SetRound(record.GetRound());
                newRecord.MutableColumns()->Swap(record.MutableColumns());

                auto* failedTablet = newRecord.AddFailedTablets();
                failedTablet->SetError(NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNAVAILABLE_NODE);
                failedTablet->SetTabletId(72075186224037900);
                failedTablet->SetNodeId(2);

                record.Swap(&newRecord);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 11); // 10 for first round, 1 for second
    }

    Y_UNIT_TEST(TraverseColumnTableAggrStatNonLocalTablet) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>(
            [&](TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                auto& record = ev->Get()->Record;

                NKikimrStat::TEvAggregateStatisticsResponse newRecord;
                newRecord.SetRound(record.GetRound());
                newRecord.MutableColumns()->Swap(record.MutableColumns());

                auto* failedTablet = newRecord.AddFailedTablets();
                failedTablet->SetError(NKikimrStat::TEvAggregateStatisticsResponse::TYPE_NON_LOCAL_TABLET);
                failedTablet->SetTabletId(72075186224037900);
                failedTablet->SetNodeId(3);

                record.Swap(&newRecord);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(60));

        ValidateCountMinColumnshard(runtime, tableInfo.PathId, 11); // 10 for first round, 1 for second
    }

}

} // NStat
} // NKikimr
