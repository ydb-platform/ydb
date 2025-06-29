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
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr = TKikimrSettings().SetAppConfig(appConfig);

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

        for (ui32 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 100 * i, 1000);
        }

        {
            auto queryClient = kikimr.GetQueryClient();
            auto it = queryClient.ExecuteQuery("DELETE FROM `/Root/olapStore/olapTable`", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        csController->WaitCompactions(TDuration::Seconds(25));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    Rows
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([])");
        }
    }
}

}   // namespace NKikimr::NKqp
