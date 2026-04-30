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
        // Regression test for the tiling++ compaction pipeline.
        //
        // Phase 1 — aging disabled:
        //   After load, portions must be distributed across the accumulator (level 0),
        //   at least one middle level (level >= 2), and the last level (level 1).
        //
        // Phase 2 — aging enabled (with a short promote time):
        //   After waiting, all portions must end up at the last level (level 1) because
        //   PromoteExpiredPortions repeatedly steps each portion one level closer to it.
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(100000));

        // One store shard, one table shard — all data in a single granule.
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        auto tableClient = kikimr.GetTableClient();

        auto alterPlanner = [&](const TString& features) {
            auto alterQuery = TStringBuilder()
                << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, )"
                << R"(`COMPACTION_PLANNER.CLASS_NAME`=`tiling++`, )"
                << R"(`COMPACTION_PLANNER.FEATURES`=`)" << features << R"(`))";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        };

        auto countByLevel = [&]() -> THashMap<ui64, ui64> {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT CompactionLevel, COUNT(*) AS cnt
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                GROUP BY CompactionLevel
                ORDER BY CompactionLevel
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto rows = CollectRows(it);
            THashMap<ui64, ui64> result;
            Cout << "=== CompactionLevel distribution ===" << Endl;
            for (auto& row : rows) {
                const ui64 level = GetUint64(row.at("CompactionLevel"));
                const ui64 cnt = GetUint64(row.at("cnt"));
                result[level] = cnt;
                Cout << "  level=" << level << " count=" << cnt << Endl;
            }
            return result;
        };

        // Phase 1: aging disabled.
        //
        // Implementation note: the `CompactionLevel` field reported by the
        // .sys/primary_index_portion_stats view is the TargetLevel of the task
        // that produced each portion. Mapping for tiling++:
        //   - Fresh write / Accumulator-task output → 0
        //   - LastLevel-task output                → 1
        //   - MiddleLevel-task output (LevelIdx≥2) → that LevelIdx
        //
        // Strategy:
        //   1. Write many DISJOINT key regions; each one's accumulator output
        //      goes to LastLevel.Portions (measure==0 — disjoint), so we end
        //      up with several disjoint LastLevel.Portions.
        //   2. Then write WIDE batches whose ranges span the whole region set;
        //      accumulator output for these has measure≈N (overlaps N disjoint
        //      LastLevel.Portions). With K=2 and N≥2 → routed to a middle level.
        //   3. After enough such middle-level portions stack on the same range,
        //      MiddleLevel fires (trigger_height=2) → its output has level≥2.
        //
        // We disable LastLevel compaction (overload threshold huge) so the
        // disjoint LastLevel.Portions are not merged into a single one.
        // NOTE on Accumulator firing: Accumulator::DoGetOptimizationTasks emits
        // a task only when the accumulated bytes exceed
        // accumulator_compaction_bytes OR accumulator_compaction_portions.
        // Defaults are huge (64 MB / 1000), so with the test's small writes the
        // task is never produced. We override both to small values.
        // CompactionLevel reflects the TargetLevel of the LAST task that
        // produced the portion; it does NOT change when the portion is later
        // re-routed by Place(). So to see portions at level 1 we need
        // LastLevel compaction to actually run; for level ≥ 2 we need
        // MiddleLevel compaction to run.
        //
        // Strategy: make accumulator_portion_size_limit so tiny that every
        // incoming write bypasses the Accumulator entirely and is routed by
        // overlap measurement (LastLevel.Measure):
        //   - Step-A small writes overlap within a region → first lands in
        //     LastLevel.Portions, subsequent overlapping writes land in
        //     LastLevel.Candidates → LastLevel compaction fires → level=1.
        //   - Step-B wide writes overlap N≈regionCount disjoint LastLevel
        //     portions → with K=2, measuredLevel = ⌊log_2(N)⌋+1 ≥ 2 →
        //     routed to a middle level → MiddleLevel compaction fires
        //     once height ≥ middle_level_trigger_height → level≥2.
        Cerr << "*** PHASE1: alterPlanner begin ***" << Endl;
        alterPlanner(
            R"({"accumulator_portion_size_limit":1, )"
            R"("portion_expected_size":50000, )"
            R"("last_level_compaction_portions":4, )"
            R"("last_level_compaction_bytes":1000000, )"
            R"("last_level_candidate_portions_overload":2, )"
            R"("middle_level_trigger_height":2, )"
            R"("middle_level_overload_height":2, )"
            R"("k":2, )"
            R"("aging_enabled":false})");
        Cerr << "*** PHASE1: alterPlanner done ***" << Endl;

        // Step A: write several disjoint regions. Each region's small writes
        // fit inside the accumulator (under 40 KB each) and sum to enough to
        // trigger by portion count; the accumulator output for that region is
        // disjoint from all other regions, so it lands in LastLevel.Portions.
        const ui64 regionStride = 1'000'000'000ULL; // 1e9 us between regions
        const ui32 regionCount = 4;
        for (ui32 region = 0; region < regionCount; ++region) {
            const ui64 base = static_cast<ui64>(region) * regionStride;
            for (ui32 j = 0; j < 3; ++j) {
                Cerr << "*** PHASE1: StepA write region=" << region << " j=" << j << " ***" << Endl;
                WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, base + j, 200, false, 100);
            }
        }
        // NOTE: csController->WaitCompactions(d) waits for `d` seconds of quiet
        // (resets its timer on every new compaction). With aggressive accumulator
        // settings compactions fire continuously, so it loops forever — we use
        // a bounded Sleep instead to give compactions time to drain a bit.
        Cerr << "*** PHASE1: StepA Sleep(15s) begin ***" << Endl;
        Sleep(TDuration::Seconds(15));
        Cerr << "*** PHASE1: StepA Sleep done ***" << Endl;

        // Step B: write wide batches spanning all the regions.
        const ui64 wideStep = (regionCount * regionStride) / 1000ULL;
        for (ui32 i = 0; i < 12; ++i) {
            Cerr << "*** PHASE1: StepB wide write i=" << i << " ***" << Endl;
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i, 1000, false, wideStep);
        }

        Cerr << "*** PHASE1: StepB Sleep(20s) begin ***" << Endl;
        Sleep(TDuration::Seconds(20));
        Cerr << "*** PHASE1: StepB Sleep done ***" << Endl;

        // Step C: a final small write right before measurement.
        // Fresh writes have CompactionLevel = 0 in primary_index_portion_stats,
        // and we only sleep briefly so this portion is still uncompacted when
        // we read the table. This guarantees the accumulator (level 0) bucket
        // is non-empty without disturbing the level-1 / level≥2 portions.
        Cerr << "*** PHASE1: StepC fresh write ***" << Endl;
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 999'999'999'999ULL, 100, false, 1);
        Sleep(TDuration::Seconds(1));

        {
            Cerr << "*** PHASE1: countByLevel begin ***" << Endl;
            auto byLevel = countByLevel();
            const ui64 accumulator = byLevel.Value(0, 0);
            const ui64 lastLevel = byLevel.Value(1, 0);
            ui64 middle = 0;
            for (const auto& [level, cnt] : byLevel) {
                if (level >= 2) {
                    middle += cnt;
                }
            }
            UNIT_ASSERT_C(accumulator > 0,
                "Phase 1: expected portions at the accumulator (level 0), got 0");
            UNIT_ASSERT_C(middle > 0,
                "Phase 1: expected portions at a middle level (level >= 2), got 0");
            UNIT_ASSERT_C(lastLevel > 0,
                "Phase 1: expected portions at the last level (level 1), got 0");
        }

        // Phase 2: enable aging with a very short promote time. The optimizer's
        // periodic actualization (driven by SetOverridePeriodicWakeupActivationPeriod
        // = 1s) calls PromoteExpiredPortions on each tick, which steps every
        // non-level-1 portion one level closer to the last level. Eventually all
        // portions reach the last level and LastLevel compaction merges them.
        // Just wait long enough for many ticks to fire.
        Cerr << "*** PHASE2: alterPlanner begin ***" << Endl;
        alterPlanner(
            R"({"k":2, )"
            R"("aging_enabled":true, )"
            R"("aging_promote_time_seconds":1, )"
            R"("aging_max_portion_promotion":100000})");
        Cerr << "*** PHASE2: alterPlanner done ***" << Endl;

        // Add a final portion that intersects every existing portion (from key 0
        // up past the Step-C portion at ~1e12). After aging promotes all
        // existing portions into LastLevel.Portions (disjoint), this overlapping
        // wide portion lands as a Candidate against all of them, forcing
        // LastLevel compaction to merge them all. The merged output gets
        // CompactionLevel=1 persisted to the .sys view.
        Cerr << "*** PHASE2: intersect-everything write ***" << Endl;
        // 1000 rows with step 2e9 → spans [0, 2e12], covering Step-A regions
        // ([0, 4e9)) and the Step-C portion at 999_999_999_999.
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 0, 1000, false, 2'000'000'000ULL);

        // Wait long enough for repeated promotions and compactions to drain everything to
        // the last level (level 1). MiddleLevel portions step down by one each cycle;
        // we have at most ~8 portions to drain, so 30s of 1Hz ticks is plenty,
        // followed by the LastLevel merge triggered by the wide overlapping portion.
        Cerr << "*** PHASE2: Sleep(30s) begin ***" << Endl;
        Sleep(TDuration::Seconds(30));
        Cerr << "*** PHASE2: Sleep done ***" << Endl;

        {
            Cerr << "*** PHASE2: countByLevel begin ***" << Endl;
            auto byLevel = countByLevel();
            UNIT_ASSERT_C(!byLevel.empty(), "Phase 2: no portions remain");
            for (const auto& [level, cnt] : byLevel) {
                UNIT_ASSERT_C(level == 1,
                    TStringBuilder() << "Phase 2: expected all portions at last level (1), found "
                                     << cnt << " portion(s) at level " << level);
            }
        }
    }
}

}   // namespace NKikimr::NKqp
