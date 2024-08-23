#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/testlib/actors/block_events.h>

namespace NKikimr {
namespace NStat {

// TODO: check for arbitrary set of values of type T (including frequent duplicates)
// numbers (1..N) were count as a sketch. Check sketch properties
bool CheckCountMinSketch(const std::shared_ptr<TCountMinSketch>& sketch, const ui32 N) {
    UNIT_ASSERT(sketch->GetElementCount() == N);
    const double eps = 1. / sketch->GetWidth();
    const double delta = 1. / (1 << sketch->GetDepth());
    size_t failedEstimatesCount = 0;
    for (ui32 i = 0; i < N; ++i) {
        const ui32 trueCount = 1;  // true count of value i
        auto probe = sketch->Probe((const char *)&i, sizeof(i));
        if (probe > trueCount + eps * N) {
            failedEstimatesCount++;
        }
    }
    Cerr << ">>> failedEstimatesCount = " << failedEstimatesCount << Endl;
    return failedEstimatesCount < delta * N;
}

Y_UNIT_TEST_SUITE(TraverseColumnShard) {

    Y_UNIT_TEST(TraverseColumnTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);

        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
    }

    Y_UNIT_TEST(TraverseColumnTableRebootColumnshard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        runtime.SimulateSleep(TDuration::Seconds(30));

        RebootTablet(runtime, tableInfo.ShardIds[0], sender);

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);

        UNIT_ASSERT(CheckCountMinSketch(countMin, 1000000));
    }    

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeResolve) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvTxProxySchemeCache::TEvResolveKeySetResult> block(runtime);

        runtime.WaitFor("1st TEvResolveKeySetResult", [&]{ return block.size() >= 1; });
        block.Unblock(1);
        runtime.WaitFor("2nd TEvResolveKeySetResult", [&]{ return block.size() >= 1; });
        block.Unblock(1);
        runtime.WaitFor("3rd TEvResolveKeySetResult", [&]{ return block.size() >= 1; });

        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        block.Unblock();
        block.Stop();

        runtime.SimulateSleep(TDuration::Seconds(10));

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeReqDistribution) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvHive::TEvRequestTabletDistribution>([&](auto& ev){
            eventSeen = true;
            ev.Reset();
        });

        runtime.WaitFor("TEvRequestTabletDistribution", [&]{ return eventSeen; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>([&](auto& ev){
            eventSeen = true;
            ev.Reset();
        });

        runtime.WaitFor("TEvAggregateStatistics", [&]{ return eventSeen; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeSave) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>([&](auto& ev){
            eventSeen = true;
            ev.Reset();
        });

        runtime.WaitFor("TEvAggregateStatisticsResponse", [&]{ return eventSeen; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletInAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();

        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvStatisticsRequest>([&](auto& ev){
            if (++observerCount >= 5) {
                ev.Reset();
            }
        });

        runtime.WaitFor("5th TEvStatisticsRequest", [&]{ return observerCount >= 5; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
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

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
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

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(CheckCountMinSketch(countMin, ColumnTableRowsNumber));
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

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        Cerr << "probe = " << probe << Endl;
        const double eps = 1. / countMin->GetWidth();
        UNIT_ASSERT(probe <= 1 + eps * ColumnTableRowsNumber * 1.1);  // 10 for first round, 1 for second
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

        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        Cerr << "probe = " << probe << Endl;
        const double eps = 1. / countMin->GetWidth();
        UNIT_ASSERT(probe <= 1 + eps * ColumnTableRowsNumber * 1.1);  // 10 for first round, 1 for second
    }

}

} // NStat
} // NKikimr
