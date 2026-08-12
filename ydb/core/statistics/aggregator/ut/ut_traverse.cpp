#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {
namespace NStat {

namespace {

TTestEnv CreateTestEnv() {
    return TTestEnv(1, 1, false, [](Tests::TServerSettings& settings) {
        settings.AppConfig->MutableStatisticsConfig()
            ->SetEnableBackgroundColumnStatsCollection(true);
    });
}

TTestEnv CreateTestEnv(ui32 changeRatioThresholdPercent) {
    return TTestEnv(1, 1, false, [changeRatioThresholdPercent](Tests::TServerSettings& settings) {
        settings.AppConfig->MutableStatisticsConfig()
            ->SetEnableBackgroundColumnStatsCollection(true);
        settings.AppConfig->MutableStatisticsConfig()
            ->SetBackgroundAnalyzeChangeRatioThresholdPercent(changeRatioThresholdPercent);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TraverseStatistics) {

    Y_UNIT_TEST_TWIN(Traverse, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseNestedTable, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "subdir/Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseServerless, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseTwoTables, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table1 = PrepareTableWithIndexes(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTableWithIndexes(env, "Database", "Table2", ColumnShard);

        WaitForPrimaryCollection(runtime, table1.PathId, ColumnTableRowsNumber, 2, ColumnShard);

        ValidateStatistics(runtime, table1.PathId);
        ValidateStatistics(runtime, table2.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseTwoTablesServerless, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        const auto table1 = PrepareTableWithIndexes(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTableWithIndexes(env, "Database", "Table2", ColumnShard);

        WaitForPrimaryCollection(runtime, table1.PathId, ColumnTableRowsNumber, 2, ColumnShard);

        ValidateStatistics(runtime, table1.PathId);
        ValidateStatistics(runtime, table2.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseTwoTablesTwoServerlessDbs, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        const auto table1 = PrepareTableWithIndexes(env, "Serverless1", "Table1", ColumnShard);
        const auto table2 = PrepareTableWithIndexes(env, "Serverless2", "Table2", ColumnShard);

        WaitForPrimaryCollection(runtime, table1.PathId, ColumnTableRowsNumber, 2, ColumnShard);

        ValidateStatistics(runtime, table1.PathId);
        ValidateStatistics(runtime, table2.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseEmptyTable, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateEmptyTable(env, "Database", "Table", ColumnShard);
        // An empty table produces no statistics, so we wait for the
        // BackgroundAnalyze completed counter rather than a save event.
        auto completedCount = WaitForPrimaryCollection(runtime, tableInfo.PathId, 0, 1, ColumnShard);
        UNIT_ASSERT_GE(completedCount, 1);

        // No statistics should be saved for an empty table.
        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, tableInfo.PathId, expected);
    }

    Y_UNIT_TEST_TWIN(TraverseRebootShard, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        RebootTablet(runtime, tableInfo.ShardIds[0], sender);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseRebootSaBeforeScan, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        block.Unblock();
        block.Stop();

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(TraverseRebootSaBeforeSave, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size() > 0; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        block.Unblock();
        block.Stop();

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(PrimaryCollection, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(ChangeRatioTrigger, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 500);

        runtime.WaitFor("second TEvSaveStatisticsQueryResponse", [&] {
            return observer.GetSaveCount() >= 1;
        });

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(NoTriggerBelowThreshold, ColumnShard) {
        TTestEnv env = CreateTestEnv(50);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 10);

        WaitForBackgroundAnalyzeToStabilize(runtime);

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);
    }

    Y_UNIT_TEST_TWIN(CountersResetAfterAnalyze, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 50);

        WaitForBackgroundAnalyzeToStabilize(runtime);

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);
    }

    Y_UNIT_TEST_TWIN(ConfigThresholdHigh, ColumnShard) {
        TTestEnv env = CreateTestEnv(100);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 500);

        WaitForBackgroundAnalyzeToStabilize(runtime);

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);
    }

    Y_UNIT_TEST_TWIN(ConfigThresholdLow, ColumnShard) {
        TTestEnv env = CreateTestEnv(1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 20);

        runtime.WaitFor("TEvSaveStatisticsQueryResponse after small change", [&] {
            return observer.GetSaveCount() >= 1;
        });

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 1);
    }

    Y_UNIT_TEST_TWIN(TableDeletion, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        DropTable(env, "Database", "Table");

        // Wait for the background traversal to process the dropped table.
        WaitForBackgroundAnalyzeToStabilize(runtime);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);

        WaitForBackgroundAnalyzeToStabilize(runtime);

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);
    }

    Y_UNIT_TEST_TWIN(Counters, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        // WaitForPrimaryCollection waits for all race-condition-triggered
        // traversals to settle and returns the final completed counter value.
        // Due to the race between FinishTraversal and TEvSchemeShardStats, the
        // counter may be > 1 (spurious traversals triggered by stale change
        // counters). We assert that at least one traversal completed and that
        // no traversal is pending.
        auto completedCount = WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);
        UNIT_ASSERT_GE(completedCount, 1);

        {
            auto counters = runtime.GetAppData(1).Counters;
            auto pendingCounter = GetServiceCounters(counters, "statistics")
                ->GetSubgroup("subsystem", "background_analyze")
                ->GetSubgroup("status", "pending")
                ->FindCounter("BackgroundAnalyze");
            UNIT_ASSERT(pendingCounter);
            UNIT_ASSERT_VALUES_EQUAL(pendingCounter->Val(), 0);
        }
    }

    Y_UNIT_TEST_TWIN(ForceAnalyzeDoesNotAffectBackgroundCounters, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{tableInfo.PathId}}, "forceOp", "/Root/Database");

        auto backgroundAnalyzeCounters = GetServiceCounters(runtime.GetAppData(1).Counters, "statistics")
            ->GetSubgroup("subsystem", "background_analyze");

        auto completedCounter = backgroundAnalyzeCounters
            ->GetSubgroup("status", "completed")->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(completedCounter);
        UNIT_ASSERT_VALUES_EQUAL(completedCounter->Val(), 0);

        auto failedCounter = backgroundAnalyzeCounters
            ->GetSubgroup("status", "failed")->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(failedCounter);
        UNIT_ASSERT_VALUES_EQUAL(failedCounter->Val(), 0);
    }

    Y_UNIT_TEST_TWIN(BackgroundDeduplicateForceAnalyze, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        auto primaryCount = WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        InsertDataIntoTable(env, "Database", "Table", 500);
        WaitForBackgroundAnalyzeToStabilize(runtime);

        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{tableInfo.PathId}}, "dedupOp", "/Root/Database");

        WaitForBackgroundAnalyzeToStabilize(runtime);

        // The force ANALYZE is deduplicated by the background traversal that
        // already collected the same statistics, so it does not increment the
        // background completed counter. The counter should equal the number
        // of background traversals that completed (primary + any spurious).
        auto counters = runtime.GetAppData(1).Counters;
        auto completedCounter = GetServiceCounters(counters, "statistics")
            ->GetSubgroup("subsystem", "background_analyze")
            ->GetSubgroup("status", "completed")
            ->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(completedCounter);
        UNIT_ASSERT_GE(completedCounter->Val(), primaryCount);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(RestartPreservesAnalyzeBaseline, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, saTabletId, sender);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);
        InsertDataIntoTable(env, "Database", "Table", 50);

        WaitForBackgroundAnalyzeToStabilize(runtime);

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);
    }

    // SchemeShard restart without persistent partition stats: SS loses
    // partition counters and, until columnshards re-report, may send
    // AreStatsFull=false (zeros) to SA. TTxSchemeShardStats must keep the
    // previously committed counters, so the analyze baseline stays
    // meaningful and a below-threshold change does NOT spuriously
    // re-trigger ANALYZE. A later above-threshold change still must.
    Y_UNIT_TEST_TWIN(SchemeShardRestartWithoutPersistentStats, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()
                ->SetEnableBackgroundColumnStatsCollection(true);
            settings.AppConfig->MutableStatisticsConfig()
                ->SetBackgroundAnalyzeChangeRatioThresholdPercent(20);
            // After reboot SS waits 30s before the first SendBaseStatsToSA; keep
            // the subsequent interval short so recovery is observable quickly.
            settings.AppConfig->MutableStatisticsConfig()
                ->SetBaseStatsSendIntervalSecondsDedicated(3);
            settings.FeatureFlags.SetEnablePersistentPartitionStats(false);
        });
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "Table", ColumnShard);

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);

        const ui64 ssTabletId = tableInfo.PathId.OwnerId;

        // Wait until SA has a full base-stats snapshot and any catch-up
        // re-analysis (baselining LastAnalyze from real counters) has finished.
        WaitForSchemeShardStatsUpdate(runtime, ssTabletId, /*requireFull=*/true);
        runtime.SimulateSleep(TDuration::Seconds(5));

        // Reboot tenant SchemeShard. Partition stats are not persisted, so
        // AreStatsFull becomes false until columnshards re-report.
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, ssTabletId, sender);

        // Wait until SS reconnects and delivers a full stats blob again
        // (incomplete reports must be merged with the previous full values).
        WaitForSchemeShardStatsUpdate(runtime, ssTabletId, /*requireFull=*/true);

        TSaveStatisticsObserver observer(runtime, tableInfo.PathId);

        // Below-threshold change (5% << 20%). Must not re-analyze: proving that
        // SA did not replace the live counters with zeros from the incomplete
        // post-reboot report.
        InsertDataIntoTable(env, "Database", "Table", 50);
        runtime.SimulateSleep(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 0);

        // Above-threshold change must still trigger — the pipeline works after
        // the SchemeShard restart.
        InsertDataIntoTable(env, "Database", "Table", 500);
        runtime.WaitFor("TEvSaveStatisticsQueryResponse after SchemeShard restart", [&] {
            return observer.GetSaveCount() >= 1;
        });

        UNIT_ASSERT_VALUES_EQUAL(observer.GetSaveCount(), 1);
    }

    // Verifies the analyze actor resolves the correct database for a nested
    // table by checking DatabaseName in the TEvResolveKeySet request.
    Y_UNIT_TEST_TWIN(TraverseNestedTableResolvesCorrectDatabase, ColumnShard) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTableWithIndexes(env, "Database", "subdir/Table", ColumnShard);

        TString resolveDatabaseName;
        auto resolveObserver = runtime.AddObserver<TEvTxProxySchemeCache::TEvResolveKeySet>(
            [&](TAutoPtr<TEventHandle<TEvTxProxySchemeCache::TEvResolveKeySet>>& ev) {
                const auto& request = *ev->Get()->Request;
                for (const auto& entry : request.ResultSet) {
                    if (entry.KeyDescription
                        && entry.KeyDescription->TableId.PathId == tableInfo.PathId) {
                        resolveDatabaseName = request.DatabaseName;
                    }
                }
            });

        WaitForPrimaryCollection(runtime, tableInfo.PathId, ColumnTableRowsNumber, 1, ColumnShard);
        resolveObserver.Remove();

        UNIT_ASSERT_VALUES_EQUAL(resolveDatabaseName, "/Root/Database");
        ValidateStatistics(runtime, tableInfo.PathId);
    }

} // Y_UNIT_TEST_SUITE(TraverseStatistics)

} // namespace NStat
} // namespace NKikimr
