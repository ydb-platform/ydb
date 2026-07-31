#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NStat {

namespace {

TTestEnv CreateTestEnv(ui32 changeRatioThresholdPercent = 20) {
    return TTestEnv(1, 1, false, [changeRatioThresholdPercent](Tests::TServerSettings& settings) {
        settings.AppConfig->MutableStatisticsConfig()
            ->SetEnableBackgroundColumnStatsCollection(true);
        settings.AppConfig->MutableStatisticsConfig()
            ->SetBackgroundAnalyzeChangeRatioThresholdPercent(changeRatioThresholdPercent);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(BackgroundChangeRatio) {

    // 1. Primary collection: table never analyzed -> background traversal picks it up immediately
    //    (no 24h wait)
    Y_UNIT_TEST(PrimaryCollection) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the background traversal to collect statistics.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Verify statistics were actually collected.
        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(countMin);
        UNIT_ASSERT_VALUES_EQUAL(countMin->GetElementCount(), ColumnTableRowsNumber);
    }

    // 2. Change ratio trigger: insert/update/delete rows exceeding threshold -> ANALYZE is triggered
    Y_UNIT_TEST(ChangeRatioTrigger) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert a large number of rows to exceed the 20% change ratio threshold.
        // ColumnTableRowsNumber = 1000, so we need > 200 changes.
        InsertDataIntoTable(env, "Database", "Table", 500);

        // Wait for the next background traversal triggered by the change ratio.
        // We observe a new TEvSaveStatisticsQueryResponse for the same path.
        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        // Wait for the second save (first was the primary collection).
        runtime.WaitFor("second TEvSaveStatisticsQueryResponse", [&] {
            return saveCount >= 1;
        });

        observer.Remove();

        // Verify statistics were re-collected.
        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(countMin);
    }

    // 3. No trigger below threshold: small changes below threshold -> no ANALYZE triggered
    Y_UNIT_TEST(NoTriggerBelowThreshold) {
        // Use a high threshold (50%) so small changes don't trigger.
        TTestEnv env = CreateTestEnv(50);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert a small number of rows — well below the 50% threshold.
        // ColumnTableRowsNumber = 1000, threshold = 50%, so we need < 500 changes.
        // We insert only 10 rows (1% change ratio).
        InsertDataIntoTable(env, "Database", "Table", 10);

        // Wait a bit and verify no new save was triggered.
        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        // Wait for a reasonable time to see if a traversal is triggered.
        // The ScheduleTraversalPeriod is 24h, so without the change ratio
        // trigger, no traversal should happen.
        runtime.SimulateSleep(TDuration::Seconds(10));

        observer.Remove();

        // No new save should have occurred (change ratio is below threshold).
        UNIT_ASSERT_VALUES_EQUAL(saveCount, 0);
    }

    // 4. Change counters reset after ANALYZE: after ANALYZE completes,
    //    LastAnalyzeRowUpdates and LastAnalyzeRowDeletes are updated ->
    //    subsequent small changes do not trigger another ANALYZE
    Y_UNIT_TEST(CountersResetAfterAnalyze) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert a moderate number of rows that would exceed the 20% threshold
        // if the counters had NOT been reset.
        // After the primary collection, LastAnalyzeRowUpdates and
        // LastAnalyzeRowDeletes are set to the current values. So only
        // NEW changes since the last ANALYZE count toward the ratio.
        InsertDataIntoTable(env, "Database", "Table", 50); // 5% change ratio — below 20%

        // Wait and verify no new save was triggered.
        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(10));

        observer.Remove();

        // No new save should have occurred (change ratio since last ANALYZE is below threshold).
        UNIT_ASSERT_VALUES_EQUAL(saveCount, 0);
    }

    // 5. Config threshold: change BackgroundAnalyzeChangeRatioThresholdPercent -> verify behavior changes
    Y_UNIT_TEST(ConfigThresholdHigh) {
        // With a 100% threshold, even large changes should not trigger.
        TTestEnv env = CreateTestEnv(100);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert 500 rows — 50% change ratio, below the 100% threshold.
        InsertDataIntoTable(env, "Database", "Table", 500);

        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(10));

        observer.Remove();

        // No new save should have occurred (50% < 100% threshold).
        UNIT_ASSERT_VALUES_EQUAL(saveCount, 0);
    }

    Y_UNIT_TEST(ConfigThresholdLow) {
        // With a 1% threshold, even small changes should trigger.
        TTestEnv env = CreateTestEnv(1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert 20 rows — 2% change ratio, above the 1% threshold.
        InsertDataIntoTable(env, "Database", "Table", 20);

        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        runtime.WaitFor("TEvSaveStatisticsQueryResponse after small change", [&] {
            return saveCount >= 1;
        });

        observer.Remove();

        // A new save should have occurred (2% > 1% threshold).
        UNIT_ASSERT_VALUES_EQUAL(saveCount, 1);
    }

    // 6. Table deletion: delete table -> verify change tracking is cleaned up
    Y_UNIT_TEST(TableDeletion) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial background traversal.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Drop the table.
        DropTable(env, "Database", "Table");

        // Wait a bit for the SA to process the scheme shard stats update
        // (which should remove the table from ScheduleTraversals).
        runtime.SimulateSleep(TDuration::Seconds(5));

        // Verify that no new traversal is triggered for the deleted table.
        // We can't easily check the internal ScheduleTraversals map, but we
        // can verify that no TEvSaveStatisticsQueryResponse is sent for the
        // deleted table's pathId.
        bool saveOccurred = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                saveOccurred = true;
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(10));

        observer.Remove();

        UNIT_ASSERT(!saveOccurred);
    }

    // 7. Counters: verify BackgroundAnalyze dynamic counter with status label is reported
    //    This test verifies that the monitoring counters are set correctly.
    //    We check the dynamic counters via GetSubgroup("status", ...).
    Y_UNIT_TEST(Counters) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the background traversal to complete.
        // ReportAnalyzeCounters() is called in TTxSchemeShardStats::Complete(),
        // which sets the pending counter. After the traversal completes,
        // FinishTraversal() increments the completed counter and
        // ReportAnalyzeCounters() resets the pending counter to 0
        // (since the table is no longer stale after being analyzed).
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // WaitForSavedStatistics returns when TEvSaveStatisticsQueryResponse is
        // observed, but FinishTraversal (which increments the completed counter)
        // runs in a subsequent TTxFinishTraversal transaction. Wait a bit for
        // that transaction to complete.
        runtime.SimulateSleep(TDuration::Seconds(1));

        // After the traversal, the completed counter should show at least 1.
        {
            auto counters = runtime.GetAppData(1).Counters;
            auto completedCounter = GetServiceCounters(counters, "statistics")
                ->GetSubgroup("subsystem", "background_analyze")
                ->GetSubgroup("status", "completed")
                ->FindCounter("BackgroundAnalyze");
            UNIT_ASSERT(completedCounter);
            UNIT_ASSERT_VALUES_EQUAL(completedCounter->Val(), 1);
        }

        // After the traversal, the pending counter should be 0
        // (the table is no longer stale after being analyzed).
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

    // 8. The BackgroundAnalyze completed/failed counters must track background
    //    traversals only. A force (user-initiated) ANALYZE shares FinishTraversal()
    //    with the background path, so it must not inflate these counters.
    Y_UNIT_TEST(ForceAnalyzeDoesNotAffectBackgroundCounters) {
        // Background collection disabled: the only traversal is the force one.
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        // Run a force ANALYZE to completion.
        Analyze(runtime, saTabletId, {{tableInfo.PathId}}, "forceOp", "/Root/Database");

        auto backgroundAnalyzeCounters = GetServiceCounters(runtime.GetAppData(1).Counters, "statistics")
            ->GetSubgroup("subsystem", "background_analyze");

        // Force ANALYZE must not touch the background completed/failed counters.
        auto completedCounter = backgroundAnalyzeCounters
            ->GetSubgroup("status", "completed")->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(completedCounter);
        UNIT_ASSERT_VALUES_EQUAL(completedCounter->Val(), 0);

        auto failedCounter = backgroundAnalyzeCounters
            ->GetSubgroup("status", "failed")->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(failedCounter);
        UNIT_ASSERT_VALUES_EQUAL(failedCounter->Val(), 0);
    }

    // 9. Deduplication: when a background traversal completes for a table that
    //    also has a pending force (user-initiated) ANALYZE, the force request
    //    should be marked as done — the statistics were just collected.
    Y_UNIT_TEST(BackgroundDeduplicatesForceAnalyze) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the initial (primary) background traversal to complete.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Insert enough rows to exceed the 20% threshold and trigger a
        // background traversal.
        InsertDataIntoTable(env, "Database", "Table", 500);

        // Wait for the second background traversal (triggered by the change
        // ratio) to complete. After it finishes, the table's statistics are
        // up to date.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        // Now send a force ANALYZE for the same table. Since the background
        // traversal just collected the statistics, the force request should
        // be deduplicated — the table is no longer stale.
        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        // Run the force ANALYZE. It should complete successfully (the table
        // was just analyzed, so the force traversal finds nothing to do or
        // the statistics are already current).
        Analyze(runtime, saTabletId, {{tableInfo.PathId}}, "dedupOp", "/Root/Database");

        // Wait for any remaining transactions to settle.
        runtime.SimulateSleep(TDuration::Seconds(1));

        // The BackgroundAnalyze{status="completed"} counter must be at least 2:
        //   1 = primary collection (never-analyzed table)
        //   2 = change-ratio triggered re-analysis
        // The force ANALYZE must not have produced an additional background
        // completion (it uses the force path, not the background path).
        auto counters = runtime.GetAppData(1).Counters;
        auto completedCounter = GetServiceCounters(counters, "statistics")
            ->GetSubgroup("subsystem", "background_analyze")
            ->GetSubgroup("status", "completed")
            ->FindCounter("BackgroundAnalyze");
        UNIT_ASSERT(completedCounter);
        UNIT_ASSERT_VALUES_EQUAL(completedCounter->Val(), 2);

        // Verify statistics were actually collected.
        auto countMin = ExtractCountMin(runtime, tableInfo.PathId);
        UNIT_ASSERT(countMin);
    }

    // 10. Restart persistence: the analyze baseline (LastAnalyzeRowUpdates
    //     and LastAnalyzeRowDeletes) and LastUpdateTime live only in the
    //     SA-local ScheduleTraversals table. After a primary collection, an
    //     SA tablet restart must reload all of them, so a subsequent
    //     below-threshold change still does NOT trigger a re-analysis.
    //     If either field were lost on reload, the table would look either
    //     "never analyzed" (baseline -> Max<ui64>()) or time-stale
    //     (LastUpdateTime -> 0) and be re-analyzed immediately.
    Y_UNIT_TEST(RestartPreservesAnalyzeBaseline) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareColumnTableWithIndexes(env, "Database", "Table", 1);

        // Wait for the primary background traversal: this persists
        // LastUpdateTime, LastAnalyzeRowUpdates, and LastAnalyzeRowDeletes
        // for the table.
        WaitForSavedStatistics(runtime, tableInfo.PathId);

        ui64 saTabletId = 0;
        ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        // Restart the SA tablet, forcing a reload from the local database.
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, saTabletId, sender);

        // Insert a below-threshold number of rows (5% << 20%). This only
        // stays below threshold if the reloaded baseline still reflects the
        // primary collection; a lost baseline would treat the table as
        // never-analyzed and re-collect regardless.
        InsertDataIntoTable(env, "Database", "Table", 50);

        size_t saveCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](auto& ev) {
            if (ev->Get()->PathId == tableInfo.PathId) {
                ++saveCount;
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(10));

        observer.Remove();

        // No re-analysis: both persisted fields survived the restart.
        UNIT_ASSERT_VALUES_EQUAL(saveCount, 0);
    }

} // Y_UNIT_TEST_SUITE(BackgroundChangeRatio)

} // namespace NStat
} // namespace NKikimr
