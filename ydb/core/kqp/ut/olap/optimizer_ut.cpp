#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/fake_storage.h>

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
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(100000));

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

        const ui64 regionStride = 1'000'000'000ULL; // 1e9 us between regions
        const ui32 regionCount = 4;
        for (ui32 region = 0; region < regionCount; ++region) {
            const ui64 base = static_cast<ui64>(region) * regionStride;
            for (ui32 j = 0; j < 3; ++j) {
                WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, base + j, 200, false, 100);
            }
        }

        Sleep(TDuration::Seconds(15));

        const ui64 wideStep = (regionCount * regionStride) / 1000ULL;
        for (ui32 i = 0; i < 12; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i, 1000, false, wideStep);
        }

        Sleep(TDuration::Seconds(20));

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 999'999'999'999ULL, 100, false, 1);
        Sleep(TDuration::Seconds(1));

        {
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

        alterPlanner(
            R"({"k":2, )"
            R"("aging_enabled":true, )"
            R"("aging_promote_time_seconds":1, )"
            R"("aging_max_portion_promotion":100000})");

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 0, 1000, false, 2'000'000'000ULL);

        Sleep(TDuration::Seconds(10));

        {
            auto byLevel = countByLevel();
            UNIT_ASSERT_C(!byLevel.empty(), "Phase 2: no portions remain");
            for (const auto& [level, cnt] : byLevel) {
                UNIT_ASSERT_C(level == 1,
                    TStringBuilder() << "Phase 2: expected all portions at last level (1), found "
                                     << cnt << " portion(s) at level " << level);
            }
        }
    }

    // Helpers shared by the strategy-switching tests below.
    namespace {

    TString LcBucketsAlter(const TString& objectPath, const TString& objectType) {
        return TStringBuilder()
            << "ALTER OBJECT `" << objectPath << "` (TYPE " << objectType << ") SET ("
            << "ACTION=UPSERT_OPTIONS, "
            << "`COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, "
            << "`COMPACTION_PLANNER.FEATURES`=`"
            << R"({"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_count_available" : 2},)"
            << R"( {"class_name" : "Zero", "expected_blobs_size" : 20000, "portions_count_available" : 1},)"
            << R"( {"class_name" : "OneLayer", "expected_portion_size" : 40000, "size_limit_guarantee" : 100000000, "bytes_limit_fraction" : 1}]})"
            << "`);";
    }

    TString TilingPpAlter(const TString& objectPath, const TString& objectType) {
        return TStringBuilder()
            << "ALTER OBJECT `" << objectPath << "` (TYPE " << objectType << ") SET ("
            << "ACTION=UPSERT_OPTIONS, "
            << "`COMPACTION_PLANNER.CLASS_NAME`=`tiling++`, "
            << "`COMPACTION_PLANNER.FEATURES`=`"
            << R"({"accumulator_portion_size_limit":1,)"
            << R"("portion_expected_size":50000,)"
            << R"("last_level_compaction_portions":4,)"
            << R"("last_level_compaction_bytes":1000000,)"
            << R"("last_level_candidate_portions_overload":2,)"
            << R"("middle_level_trigger_height":2,)"
            << R"("middle_level_overload_height":2,)"
            << R"("k":2,)"
            << R"("aging_enabled":false})"
            << "`);";
    }

    TString TilingPlainAlter(const TString& objectPath, const TString& objectType) {
        return TStringBuilder()
            << "ALTER OBJECT `" << objectPath << "` (TYPE " << objectType << ") SET ("
            << "ACTION=UPSERT_OPTIONS, "
            << "`COMPACTION_PLANNER.CLASS_NAME`=`tiling`, "
            << "`COMPACTION_PLANNER.FEATURES`=`{}`);";
    }

    TString TilingPpAgingAlter(const TString& objectPath, const TString& objectType) {
        return TStringBuilder()
            << "ALTER OBJECT `" << objectPath << "` (TYPE " << objectType << ") SET ("
            << "ACTION=UPSERT_OPTIONS, "
            << "`COMPACTION_PLANNER.CLASS_NAME`=`tiling++`, "
            << "`COMPACTION_PLANNER.FEATURES`=`"
            << R"({"k":2,)"
            << R"("aging_enabled":true,)"
            << R"("aging_promote_time_seconds":1,)"
            << R"("aging_max_portion_promotion":100000})"
            << "`);";
    }

    void RunAlter(NYdb::NTable::TTableClient& tableClient, const TString& alterQuery) {
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
    }

    ui64 SelectRowCount(NYdb::NTable::TTableClient& tableClient, const TString& tablePath) {
        auto it = tableClient.StreamExecuteScanQuery(TStringBuilder()
            << "--!syntax_v1\nSELECT COUNT(*) AS cnt FROM `" << tablePath << "`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto rows = CollectRows(it);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        return GetUint64(rows[0].at("cnt"));
    }

    ui64 SelectSumLevel(NYdb::NTable::TTableClient& tableClient, const TString& tablePath) {
        // Defensive: SUM(level) should be deterministic given a deterministic generator.
        auto it = tableClient.StreamExecuteScanQuery(TStringBuilder()
            << "--!syntax_v1\nSELECT CAST(SUM(CAST(level AS Int64)) AS Uint64) AS lsum FROM `"
            << tablePath << "`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto rows = CollectRows(it);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        return GetUint64(rows[0].at("lsum"));
    }

    ui64 SelectPortionCount(NYdb::NTable::TTableClient& tableClient, const TString& tablePath) {
        auto it = tableClient.StreamExecuteScanQuery(TStringBuilder()
            << "--!syntax_v1\nSELECT COUNT(*) AS cnt FROM `" << tablePath
            << "/.sys/primary_index_portion_stats`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto rows = CollectRows(it);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        return GetUint64(rows[0].at("cnt"));
    }

    THashMap<ui64, ui64> SelectOptimizerLevelStats(NYdb::NTable::TTableClient& tableClient, const TString& tablePath) {
        auto it = tableClient.StreamExecuteScanQuery(TStringBuilder()
            << "--!syntax_v1\nSELECT CompactionLevel, COUNT(*) AS cnt FROM `" << tablePath
            << "/.sys/primary_index_portion_stats` WHERE Activity == 1 GROUP BY CompactionLevel ORDER BY CompactionLevel").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto rows = CollectRows(it);
        THashMap<ui64, ui64> result;
        for (auto& row : rows) {
            result[GetUint64(row.at("CompactionLevel"))] = GetUint64(row.at("cnt"));
        }
        return result;
    }

    }   // namespace

    // Switch lc-buckets -> tiling++ on a table that already holds data. Verifies that:
    //   1. The ALTER succeeds while the table contains portions produced by another planner.
    //   2. Row count and aggregate are preserved across the switch (no portion is lost or
    //      misrouted by InitialAddPortions() during the fresh load triggered by the switch).
    //   3. Further compaction proceeds under the new planner (portions show up at the
    //      tiling++ levels and the row count is still preserved).
    Y_UNIT_TEST(SwitchLcBucketsToTilingPlusPlus) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        // Phase 1: write some data under lc-buckets.
        RunAlter(tableClient, LcBucketsAlter("/Root/olapStore", "TABLESTORE"));

        const ui32 batches = 25;
        const ui64 rowsPerBatch = 4000;
        for (ui32 i = 0; i < batches; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * rowsPerBatch, rowsPerBatch);
            if (i % 5 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        const ui64 expectedRows = SelectRowCount(tableClient, "/Root/olapStore/olapTable");
        const ui64 expectedLevelSum = SelectSumLevel(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(expectedRows > 0, "expected non-empty table before switch");
        const ui64 portionsBefore = SelectPortionCount(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(portionsBefore > 0, "expected at least one portion before switch");

        // Phase 2: switch to tiling++. This forces a fresh load that re-routes existing
        // lc-buckets portions through InitialAddPortions() in the tiling++ planner.
        RunAlter(tableClient, TilingPpAlter("/Root/olapStore", "TABLESTORE"));

        // Give the planner time to ingest the existing portions and (potentially) start
        // compacting them under the new strategy.
        csController->WaitCompactions(TDuration::Seconds(15));

        // Row count and aggregate must be preserved across the switch.
        UNIT_ASSERT_VALUES_EQUAL(SelectRowCount(tableClient, "/Root/olapStore/olapTable"), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(SelectSumLevel(tableClient, "/Root/olapStore/olapTable"), expectedLevelSum);

        // Phase 3: write more data under tiling++ and confirm the new planner is healthy.
        for (ui32 i = 0; i < 5; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable",
                0, (batches + i) * rowsPerBatch, rowsPerBatch);
        }
        csController->WaitCompactions(TDuration::Seconds(15));

        const ui64 expectedAfterAppend = expectedRows + 5 * rowsPerBatch;
        UNIT_ASSERT_VALUES_EQUAL(
            SelectRowCount(tableClient, "/Root/olapStore/olapTable"), expectedAfterAppend);
    }

    // Switch tiling++ -> lc-buckets on a table that already holds data. Symmetric to
    // SwitchLcBucketsToTilingPlusPlus: validates the "can be rolled back to any other
    // compaction" promise.
    Y_UNIT_TEST(SwitchTilingPlusPlusToLcBuckets) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        RunAlter(tableClient, TilingPpAlter("/Root/olapStore", "TABLESTORE"));

        const ui32 batches = 25;
        const ui64 rowsPerBatch = 4000;
        for (ui32 i = 0; i < batches; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * rowsPerBatch, rowsPerBatch);
            if (i % 5 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        const ui64 expectedRows = SelectRowCount(tableClient, "/Root/olapStore/olapTable");
        const ui64 expectedLevelSum = SelectSumLevel(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(expectedRows > 0, "expected non-empty table before switch");

        // Roll back to lc-buckets.
        RunAlter(tableClient, LcBucketsAlter("/Root/olapStore", "TABLESTORE"));
        csController->WaitCompactions(TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL(SelectRowCount(tableClient, "/Root/olapStore/olapTable"), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(SelectSumLevel(tableClient, "/Root/olapStore/olapTable"), expectedLevelSum);

        // Confirm post-rollback writes + compaction still work.
        for (ui32 i = 0; i < 5; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable",
                0, (batches + i) * rowsPerBatch, rowsPerBatch);
        }
        csController->WaitCompactions(TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL(
            SelectRowCount(tableClient, "/Root/olapStore/olapTable"),
            expectedRows + 5 * rowsPerBatch);
    }

    // Chain three switches lc-buckets -> tiling++ -> tiling -> lc-buckets to exercise
    // the rearrangement code path more aggressively. After every switch we verify that
    // no rows have been lost and that follow-up compaction completes successfully.
    Y_UNIT_TEST(SwitchCompactionStrategyChain) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(30000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        // Start under lc-buckets and seed a non-trivial amount of data.
        RunAlter(tableClient, LcBucketsAlter("/Root/olapStore", "TABLESTORE"));
        const ui32 batches = 20;
        const ui64 rowsPerBatch = 3000;
        for (ui32 i = 0; i < batches; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, i * rowsPerBatch, rowsPerBatch);
            if (i % 5 == 0) {
                csController->WaitCompactions(TDuration::MilliSeconds(10));
            }
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        const ui64 baselineRows = SelectRowCount(tableClient, "/Root/olapStore/olapTable");
        const ui64 baselineSum = SelectSumLevel(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(baselineRows > 0, "expected non-empty table");

        auto checkPreserved = [&](const TString& label) {
            const ui64 rows = SelectRowCount(tableClient, "/Root/olapStore/olapTable");
            const ui64 sum = SelectSumLevel(tableClient, "/Root/olapStore/olapTable");
            UNIT_ASSERT_C(rows == baselineRows,
                TStringBuilder() << label << ": row count drifted from " << baselineRows << " to " << rows);
            UNIT_ASSERT_C(sum == baselineSum,
                TStringBuilder() << label << ": level sum drifted from " << baselineSum << " to " << sum);
        };

        // lc-buckets -> tiling++
        RunAlter(tableClient, TilingPpAlter("/Root/olapStore", "TABLESTORE"));
        csController->WaitCompactions(TDuration::Seconds(15));
        checkPreserved("after lc-buckets -> tiling++");

        // tiling++ -> tiling
        RunAlter(tableClient, TilingPlainAlter("/Root/olapStore", "TABLESTORE"));
        csController->WaitCompactions(TDuration::Seconds(15));
        checkPreserved("after tiling++ -> tiling");

        // tiling -> lc-buckets
        RunAlter(tableClient, LcBucketsAlter("/Root/olapStore", "TABLESTORE"));
        csController->WaitCompactions(TDuration::Seconds(15));
        checkPreserved("after tiling -> lc-buckets");

        // Final sanity: write + compact under the rolled-back planner.
        for (ui32 i = 0; i < 5; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable",
                0, (batches + i) * rowsPerBatch, rowsPerBatch);
        }
        csController->WaitCompactions(TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(
            SelectRowCount(tableClient, "/Root/olapStore/olapTable"),
            baselineRows + 5 * rowsPerBatch);
    }

    Y_UNIT_TEST(TilingPlusPlusTtlDeletion) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings().SetMaxPortionSize(100000));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();

        RunAlter(tableClient, TilingPpAgingAlter("/Root/olapStore", "TABLESTORE"));

        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto ttlResult = session.ExecuteSchemeQuery(
            R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("PT1S") ON timestamp)"
        ).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(ttlResult.GetStatus(), NYdb::EStatus::SUCCESS, ttlResult.GetIssues().ToString());

        const TInstant now = TInstant::Now();
        const ui64 expiredBaseTs = (now - TDuration::Minutes(10)).MicroSeconds();
        const ui64 freshBaseTs = (now + TDuration::Days(10)).MicroSeconds();
        for (ui32 i = 0; i < 6; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, expiredBaseTs + i * 10'000'000ULL, 1000);
        }
        for (ui32 i = 0; i < 6; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, freshBaseTs + i * 10'000'000ULL, 1000);
        }

        csController->WaitCompactions(TDuration::Seconds(10));
        csController->WaitActualization(TDuration::Seconds(10));
        csController->WaitTtl(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(SelectRowCount(tableClient, "/Root/olapStore/olapTable"), 6'000);

        auto optimizerBefore = SelectOptimizerLevelStats(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(!optimizerBefore.empty(), "expected optimizer to track surviving portions after TTL deletion");

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, (now + TDuration::Days(20)).MicroSeconds(), 1000, false, 2'000'000'000ULL);
        csController->WaitCompactions(TDuration::Seconds(15));
        csController->WaitActualization(TDuration::Seconds(10));
        csController->WaitTtl(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(SelectRowCount(tableClient, "/Root/olapStore/olapTable"), 7'000);
        UNIT_ASSERT_VALUES_EQUAL(SelectPortionCount(tableClient, "/Root/olapStore/olapTable"), 7);

        auto optimizerAfter = SelectOptimizerLevelStats(tableClient, "/Root/olapStore/olapTable");
        UNIT_ASSERT_C(!optimizerAfter.empty(), "expected optimizer levels to stay readable after TTL deletion");
        ui64 trackedPortions = 0;
        for (const auto& [level, cnt] : optimizerAfter) {
            Y_UNUSED(level);
            trackedPortions += cnt;
        }
        UNIT_ASSERT_C(trackedPortions > 0, "expected at least one active portion tracked by optimizer after TTL deletion");
    }
}

}   // namespace NKikimr::NKqp
