#include <ydb/core/formats/arrow/ssa_runtime_version.h>

#include "helpers/aggregation.h"

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/fmt/include/fmt/format.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapAggregations) {
    Y_UNIT_TEST(Aggregation) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[0u;]])");
        }

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*), MAX(`resource_id`), MAX(`timestamp`), MIN(LENGTH(`message`))
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;["40999"];[3004999u];[1036u]]])");
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");
        }
    }

    Y_UNIT_TEST(AggregationCountPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetInsertFinishedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());

        {
            TString query = R"(
                --!syntax_v1
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
            )";
            auto opStartTime = Now();
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << "!!!\nPushdown query execution time: " << (Now() - opStartTime).MilliSeconds() << "\n!!!\n";
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST(AggregationCountGroupByPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptUseFinalizeByKey;
                SELECT
                    level, COUNT(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )";
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "WideCombiner" }, "TableFullScan");
//            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST_TWIN(DisableBlockEngineInAggregationWithSpilling, AllowSpilling) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE);
        if (AllowSpilling) {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingNodes("Aggregation");
        } else {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingNodes("None");
        }
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto client = kikimr.GetQueryClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
            )";

            auto res = StreamExplainQuery(query, client);
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto plan = CollectStreamResult(res);

            bool hasWideCombiner = plan.QueryStats->Getquery_ast().Contains("WideCombiner");
            UNIT_ASSERT_C(hasWideCombiner == AllowSpilling, plan.QueryStats->Getquery_ast());
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdown, UseLlvm) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = fmt::format(R"(
                --!syntax_v1
                PRAGMA ydb.UseLlvm = "{}";

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )", UseLlvm ? "true" : "false");
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "Condense" }, "");
#endif
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdownBackwardCompatibility, EnableLlvm) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = fmt::format(R"(
                --!syntax_v1
                PRAGMA Kikimr.EnableLlvm = "{}";

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )", EnableLlvm ? "true" : "false");
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "Condense" }, "");
#endif
        }
    }

    Y_UNIT_TEST(CountAllNoPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");
        }
    }

    Y_UNIT_TEST(Filter_NotAllUsedFieldsInResultSet) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultDistinctCountRI_GroupByL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(DISTINCT resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]]")
            ;
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountAll_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE level = 2
                )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("Condense")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountL_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            // See https://github.com/ydb-platform/ydb/issues/7299 for explanation, why resultCount = 3
            .MutableLimitChecker().SetExpectedResultCount(3)
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountT_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(timestamp)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("CombineCore")
            .AddExpectedPlanOptions("KqpOlapFilter")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultL_FilterL_OrderL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level > 1
                ORDER BY level
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountExpr) {
        auto g = NColumnShard::TLimits::MaxBlobSizeGuard(10000);
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(level + 2)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[0u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[5u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;1u];[5;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;0u];[7;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;1u];[6;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNull) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNullMix) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u];[[5];1u;1u;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_NoPushdownOnDisabledEmitAggApply) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    PRAGMA DisableEmitAggApply;
                    SELECT
                        COUNT(level)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(AggregationAndFilterPushdownOnDiffCols) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(`timestamp`)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level), MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[2.];[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[3.]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4.]];[5;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5.]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#];[[5];5.;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[46000;]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[15]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_SumL_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[0]];[[1];[4600]];[[2];[9200]];[[3];[13800]];[[4];[18400]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MAX(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[4]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MIN(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"10000\"]];[[1];[\"10001\"]];[[2];[\"10002\"]];[[3];[\"10003\"]];[[4];[\"10004\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MAX(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"40995\"]];[[1];[\"40996\"]];[[2];[\"40997\"]];[[3];[\"40998\"]];[[4];[\"40999\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ProjectionOrder) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    resource_id, level, count(*) as c
                FROM `/Root/olapStore/olapTable`
                GROUP BY resource_id, level
                ORDER BY c, resource_id DESC LIMIT 3
            )")
            .SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            .SetExpectedReadNodeType("TableFullScan");
        testCase.FillExpectedAggregationGroupByPlanOptions();
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id=1
            )")
            .SetExpectedReply("[[[1]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id > 5
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 5 AND 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id = 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(NoErrorOnLegacyPragma) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "false";
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(BlocksRead) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                PRAGMA Kikimr.OptEnableOlapPushdown = "false";

                SELECT
                    id, resource_id
                FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Blocks_NoAggPushdown) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                SELECT
                    COUNT(DISTINCT id)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[10u]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_GetValue) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.col1") = "val1" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val1"];#]])");

        TestTableWithNulls({testCase});
    }

    Y_UNIT_TEST(Json_GetValue_Minus) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.'col-abc'"), JSON_VALUE(jsondoc, "$.'col-abc'") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.'col-abc'") = "val-abc" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val-abc"];#]])");

        TestTableWithNulls({testCase});
    }

    Y_UNIT_TEST(Json_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1" RETURNING String), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.col1" RETURNING String) = "val1" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val1"];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;[16];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1") = "val1" AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1" RETURNING String) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1" RETURNING String) = "val1" AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int"), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;[16]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_Exists) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE
                    JSON_EXISTS(jsonval, "$.col1") AND level = 1;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonExists")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;[%true];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_Exists) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE
                    JSON_EXISTS(jsondoc, "$.col1") AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 5U
            .AddExpectedPlanOptions("KqpOlapApply")
#elif SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonExists")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;[%true]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_Query) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_QUERY(jsonval, "$.col1" WITH UNCONDITIONAL WRAPPER),
                    JSON_QUERY(jsondoc, "$.col1" WITH UNCONDITIONAL WRAPPER)
                FROM `/Root/tableWithNulls`
                WHERE
                    level = 1;
            )")
            .AddExpectedPlanOptions("Udf")
            .SetExpectedReply(R"([[1;["[\"val1\"]"];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(BlockGenericWithDistinct) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(DISTINCT id)
                FROM `/Root/tableWithNulls`
                WHERE level = 5 AND Cast(id AS String) = "5";
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply("[[1u]]");
        TestTableWithNulls({ testCase }, /* generic */ true);
    }

    Y_UNIT_TEST(BlockGenericSimpleAggregation) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(*), SUM(id)
                FROM `/Root/tableWithNulls`
                WHERE level = 5
                GROUP BY level
                ORDER BY level;
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply(R"([[[5];1u;5]])");

        TestTableWithNulls({ testCase }, /* generic */ true);
    }

    Y_UNIT_TEST(BlockGenericSelectAll) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, resource_id, level
                FROM `/Root/tableWithNulls`
                WHERE level != 5 OR level IS NULL
                ORDER BY id, resource_id, level;
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply(R"([[1;#;[1]];[2;#;[2]];[3;#;[3]];[4;#;[4]];[6;["6"];#];[7;["7"];#];[8;["8"];#];[9;["9"];#];[10;["10"];#]])");

        TestTableWithNulls({ testCase }, /* generic */ true);
    }
}

}
