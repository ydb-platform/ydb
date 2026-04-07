#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapOptimizer) {
    Y_UNIT_TEST(SpecialSliceToOneLayer) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        {
            auto alterQuery =
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 400000, "portions_count_available" : 2},
                             {"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_count_available" : 1, "default_selector_name" : "slice"},
                             {"class_name" : "OneLayer", "expected_portion_size" : 40000, "size_limit_guarantee" : 100000000, "bytes_limit_fraction" : 1}],
                 "selectors" : [{"class_name" : "Transparent", "name" : "default"}, {"class_name" : "Snapshot", "name" : "slice", "interval" : {"finish_seconds_utc" : 0}}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        for (ui32 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * 1000, 10000);
            if (i % 10 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[109000u;]])");
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.level") AS Uint64) AS LEVEL, CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.selectivity.default.records_count") AS Uint64) AS RECORDS_COUNT
                FROM `/Root/olapStore/olapTable/.sys/primary_index_optimizer_stats`
                ORDER BY LEVEL
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto rows = CollectRows(it);
            AFL_VERIFY(rows.size() == 3);
            AFL_VERIFY(0 == GetUint64(rows[0].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[0].at("RECORDS_COUNT")) == 0);
            AFL_VERIFY(1 == GetUint64(rows[1].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[1].at("RECORDS_COUNT")) >= 440000);
            AFL_VERIFY(GetUint64(rows[1].at("RECORDS_COUNT")) <= 550000);
            AFL_VERIFY(2 == GetUint64(rows[2].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[2].at("RECORDS_COUNT")) == 0);

            for (auto&& i : rows) {
                Cerr << GetUint64(i.at("LEVEL")) << "/" << GetUint64(i.at("RECORDS_COUNT")) << Endl;
            }
        }

        {
            auto alterQuery =
                TStringBuilder() << Sprintf(
                    R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 100000, "portions_count_available" : 1},
                             {"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 100000, "portions_count_available" : 1, "default_selector_name" : "slice"},
                             {"class_name" : "OneLayer", "expected_portion_size" : 40000, "size_limit_guarantee" : 100000000, "bytes_limit_fraction" : 1}],
                 "selectors" : [{"class_name" : "Snapshot", "name" : "default"}, {"class_name" : "Snapshot", "name" : "slice", "interval" : {"finish_seconds_utc" : %d}}]}`);
            )",
                    Now().Seconds());
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        csController->WaitCompactions(TDuration::Seconds(10));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[109000u;]])");
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.level") AS Uint64) AS LEVEL, Details
                FROM `/Root/olapStore/olapTable/.sys/primary_index_optimizer_stats`
                ORDER BY LEVEL
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << result << Endl;
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.level") AS Uint64) AS LEVEL,
                       CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.selectivity.default.records_count") AS Uint64) AS RECORDS_COUNT_DEFAULT,
                       CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.selectivity.slice.records_count") AS Uint64) AS RECORDS_COUNT_SLICE
                FROM `/Root/olapStore/olapTable/.sys/primary_index_optimizer_stats`
                ORDER BY LEVEL
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto rows = CollectRows(it);
            for (auto&& i : rows) {
                Cerr << GetUint64(i.at("LEVEL")) << "/" << GetUint64(i.at("RECORDS_COUNT_DEFAULT")) << "/"
                     << GetUint64(i.at("RECORDS_COUNT_SLICE")) << Endl;
            }
            AFL_VERIFY(0 == GetUint64(rows[0].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[0].at("RECORDS_COUNT_DEFAULT")) == 0);
            AFL_VERIFY(1 == GetUint64(rows[1].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[1].at("RECORDS_COUNT_DEFAULT")) == 0);
            AFL_VERIFY(2 == GetUint64(rows[2].at("LEVEL")));
            AFL_VERIFY(GetUint64(rows[2].at("RECORDS_COUNT_SLICE")) == 109000);
        }
    }
    Y_UNIT_TEST(MultiLayersOptimization) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 100000, "portions_count_available" : 1},
                             {"class_name" : "OneLayer", "expected_portion_size" : 40000, "size_limit_guarantee" : 100000, "bytes_limit_fraction" : 0},
                             {"class_name" : "OneLayer", "expected_portion_size" : 80000, "size_limit_guarantee" : 200000, "bytes_limit_fraction" : 0},
                             {"class_name" : "OneLayer", "expected_portion_size" : 160000, "size_limit_guarantee" : 300000, "bytes_limit_fraction" : 0},
                             {"class_name" : "OneLayer", "expected_portion_size" : 320000, "size_limit_guarantee" : 600000, "bytes_limit_fraction" : 1}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        for (ui32 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * 1000, 10000);
            if (i % 10 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[109000u;]])");
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.level") AS Uint64) AS LEVEL, CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.selectivity.default.blob_bytes") AS Uint64) AS BYTES
                FROM `/Root/olapStore/olapTable/.sys/primary_index_optimizer_stats`
                ORDER BY LEVEL
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto rows = CollectRows(it);
            ui32 levelIdx = 0;
            const std::vector<ui32> maxVal = { 100000, 100000, 200000, 300000, 100000000 };
            for (auto&& i : rows) {
                AFL_VERIFY(levelIdx == GetUint64(i.at("LEVEL")));
                AFL_VERIFY(GetUint64(i.at("BYTES")) < maxVal[levelIdx]);
                Cerr << GetUint64(i.at("LEVEL")) << "/" << GetUint64(i.at("BYTES")) << Endl;
                ++levelIdx;
            }
        }

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 100000, "portions_count_available" : 1},
                             {"class_name" : "Zero"}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OptimizationByTime) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "20s", "expected_blobs_size" : 1073741824, "portions_count_available" : 2},
                             {"class_name" : "Zero"}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000, 10);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 2000, 10);

        csController->WaitCompactions(TDuration::Seconds(25));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Kind == "SPLIT_COMPACTED"
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[1u;]])");
        }
    }

    Y_UNIT_TEST(OptimizationAfterDeletion) {
        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
        csController->SetOverrideMaxReadStaleness(TDuration::Seconds(1));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "20s", "expected_blobs_size" : 1073741824, "portions_count_available" : 2},
                             {"class_name" : "Zero"}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);

        for (ui32 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 100 * i, 1000);
        }

        {
            auto queryClient = kikimr.GetQueryClient();
            auto it = queryClient.ExecuteQuery("DELETE FROM `/Root/olapStore/olapTable`", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);

        csController->WaitCompactions(TDuration::Seconds(25));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    Rows
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([])");
        }
    }

    Y_UNIT_TEST(CompactionTaskLimits) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        {
            auto alterQuery =
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_size_limit" : 400000, "portions_count_available" : 2,
                              "compaction_task_memory_limit" : 1000000000, "compaction_task_portions_count_limit" : 5},
                             {"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_count_available" : 1},
                             {"class_name" : "OneLayer", "expected_portion_size" : 40000, "size_limit_guarantee" : 100000000, "bytes_limit_fraction" : 1}]}`);
            )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        for (ui32 i = 0; i < 50; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * 1000, 10000);
            if (i % 10 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[59000u;]])");
        }

        {
            auto describeResult = kikimr.GetTestClient().Ls("/Root/olapStore/olapTable");
            UNIT_ASSERT(describeResult);
            UNIT_ASSERT(describeResult->Record.HasPathDescription());
            UNIT_ASSERT(describeResult->Record.GetPathDescription().HasColumnTableDescription());

            const auto& columnTableDesc = describeResult->Record.GetPathDescription().GetColumnTableDescription();
            UNIT_ASSERT(columnTableDesc.HasSchema());
            UNIT_ASSERT(columnTableDesc.GetSchema().HasOptions());
            UNIT_ASSERT(columnTableDesc.GetSchema().GetOptions().HasCompactionPlannerConstructor());
            UNIT_ASSERT(columnTableDesc.GetSchema().GetOptions().GetCompactionPlannerConstructor().HasLCBuckets());

            const auto& lcBuckets = columnTableDesc.GetSchema().GetOptions().GetCompactionPlannerConstructor().GetLCBuckets();
            UNIT_ASSERT_VALUES_EQUAL(lcBuckets.LevelsSize(), 3);

            // Check level 0 (first Zero level)
            UNIT_ASSERT(lcBuckets.GetLevels(0).HasZeroLevel());
            UNIT_ASSERT(lcBuckets.GetLevels(0).GetZeroLevel().HasCompactionTaskMemoryLimit());
            UNIT_ASSERT(lcBuckets.GetLevels(0).GetZeroLevel().HasCompactionTaskPortionsCountLimit());
            UNIT_ASSERT_VALUES_EQUAL(lcBuckets.GetLevels(0).GetZeroLevel().GetCompactionTaskMemoryLimit(), 1000000000);
            UNIT_ASSERT_VALUES_EQUAL(lcBuckets.GetLevels(0).GetZeroLevel().GetCompactionTaskPortionsCountLimit(), 5);

            // Check level 1 (second Zero level)
            UNIT_ASSERT(lcBuckets.GetLevels(1).HasZeroLevel());
            UNIT_ASSERT(!lcBuckets.GetLevels(1).GetZeroLevel().HasCompactionTaskMemoryLimit());
            UNIT_ASSERT(!lcBuckets.GetLevels(1).GetZeroLevel().HasCompactionTaskPortionsCountLimit());

            // Check level 2 (OneLayer level)
            UNIT_ASSERT(lcBuckets.GetLevels(2).HasOneLayer());
        }
    }

    Y_UNIT_TEST(TilingCompactionOneShard) {
        // Regression test for the tiling compaction pipeline: accumulator → LastLevel.
        //
        // MaxPortionSize=30000 bytes (30 KB) is set so each write produces small portions.
        // Compaction output is also ~30 KB per portion (same MaxPortionSize cap applies globally).
        //
        // Settings:
        //   accumulator_portion_size_limit=40000 (40 KB):
        //     Fresh write portions (~30 KB) are BELOW this → go to accumulator.
        //     Compaction output portions (~30 KB) are also below this → go back to accumulator.
        //     After a second compaction round, the accumulator has enough data to produce
        //     portions that exceed the limit (multiple rounds merge into larger batches).
        //
        //   accumulator_trigger_bytes=200000 (200 KB):
        //     Accumulator fires after ~7 portions (7 × 30 KB ≈ 210 KB > 200 KB).
        //
        //   portion_expected_size=500000 (500 KB):
        //     Target output size — larger than MaxPortionSize so the splitter produces
        //     the largest possible portions (~30 KB each, capped by MaxPortionSize).
        //
        // The test verifies that SPLIT_COMPACTED portions exist after writing enough data,
        // proving the tiling pipeline routes portions through accumulator → LastLevel.
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(100000));

        // One store shard, one table shard — all data in a single granule.
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        // Switch the table to the new tiling compaction planner.
        // accumulator_portion_size_limit=40000: fresh write portions (~30 KB) go to accumulator.
        // accumulator_trigger_bytes=200000: fires after ~7 portions.
        // portion_expected_size=500000: target output size (actual output capped at ~30 KB by MaxPortionSize).
        {
            auto tableClient = kikimr.GetTableClient();
            auto alterQuery = TString(
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, )"
                R"(`COMPACTION_PLANNER.CLASS_NAME`=`tiling`, )"
                R"(`COMPACTION_PLANNER.FEATURES`=`{"accumulator_portion_size_limit":40000, "accumulator_trigger_bytes":200000, "portion_expected_size":100000}`))"
            );
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        // Write enough data to trigger the accumulator multiple times.
        // 50 writes × 1000 rows × ~30 KB/portion ≈ 1.5 MB >> 200 KB trigger.
        for (ui32 i = 0; i < 5000; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i, 100, false, 10000);
            Cout << i << Endl;
        }

        // Give the background compaction actor time to run (wakeup period = 1 s).
        Sleep(TDuration::Seconds(10));

        // Dump all portion kinds so we can see what the tiling optimizer produced.
        {
            auto tableClient = kikimr.GetTableClient();
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT Kind, COUNT(*) AS cnt
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                GROUP BY Kind
                ORDER BY Kind
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto rows = CollectRows(it);
            for (auto& row : rows) {
                Cout << "Kind=" << GetUtf8(row.at("Kind")) << " count=" << GetUint64(row.at("cnt")) << Endl;
            }
        }

        // Verify that at least some portions were compacted (Kind == "SPLIT_COMPACTED"),
        // proving that the accumulator fired and promoted portions to LastLevel.
        {
            auto tableClient = kikimr.GetTableClient();
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*) FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Kind == "SPLIT_COMPACTED"
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto rows = CollectRows(it);
            UNIT_ASSERT_C(!rows.empty(), "No portion stats returned");
            const ui64 compactedCount = GetUint64(rows[0].at("column0"));
            Cout << "SPLIT_COMPACTED portions: " << compactedCount << Endl;
            UNIT_ASSERT_C(compactedCount > 0, "Expected at least one SPLIT_COMPACTED portion — accumulator did not fire or promote portions");
        }


        // {
        //     auto tableClient = kikimr.GetTableClient();
        //     auto it = tableClient.StreamExecuteScanQuery(R"(
        //         --!syntax_v1
        //         SELECT COUNT(*) as rows FROM `/Root/olapStore/olapTable/`
        //     )").GetValueSync();
        //     UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        //     auto rows = CollectRows(it);
        //     UNIT_ASSERT_C(!rows.empty(), "No portion stats returned");
        //     const ui64 total_rows = GetUint64(rows[0].at("rows"));
        //     Cout << "total rows: " << total_rows << Endl;
        //     UNIT_ASSERT_C(total_rows == 500000, "Too few rows");
        // }

        AFL_VERIFY(false);
    }
}

}   // namespace NKikimr::NKqp
