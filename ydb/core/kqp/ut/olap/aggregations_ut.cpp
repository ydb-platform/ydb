
#include "helpers/aggregation.h"

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/fmt/include/fmt/format.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapAggregations) {
    Y_UNIT_TEST(Aggregation) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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

        {
            auto alterQuery = TStringBuilder() <<
                R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();

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

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();
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
        while (csController->GetCompactionFinishedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());

        {
            TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

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
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "Aggregate-TableFullScan");
        }
    }

    Y_UNIT_TEST(AggregationCountGroupByPushdown) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
            CheckPlanForAggregatePushdown(query, tableClient, { AGG_OPERATOR_NAMES }, "Aggregate-TableFullScan");
//            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
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

            const auto expectedAggregateNodeNames = AllowSpilling ? AGG_OPERATOR_NAMES : "BlockMergeFinalizeHashed";

            bool hasExpectedAggregateNode = CheckOperatorPresentInAst(plan.QueryStats->Getquery_ast(), expectedAggregateNodeNames);
            UNIT_ASSERT_C(hasExpectedAggregateNode, plan.QueryStats->Getquery_ast());
        }
    }

    Y_UNIT_TEST(BlockAggregationIsChosenByDefaultForSimpleQueries) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingNodes("None");
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

        const auto aggregators = {"COUNT", "SUM", "SOME", "MIN", "MAX"};

        for (const auto& aggregator : aggregators) {
            TString query = TStringBuilder()
                << "SELECT " << aggregator << "(level)"
                << R"(
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                )";

            auto res = StreamExplainQuery(query, client);
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto plan = CollectStreamResult(res);

            UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("BlockMergeFinalizeHashed"), plan.QueryStats->Getquery_ast());
            UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("DqPhyHashCombine"), plan.QueryStats->Getquery_ast());
            UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideCombiner"), plan.QueryStats->Getquery_ast());
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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

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
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "Aggregate-TableFullScan");
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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

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
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "Aggregate-TableFullScan");
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
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                    SELECT
                        COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE level = 2
                )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(2)
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountL_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("TKqpOlapAgg")
            // See https://github.com/ydb-platform/ydb/issues/7299 for explanation, why resultCount = 3
            .MutableLimitChecker().SetExpectedResultCount(3)
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountT_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    COUNT(timestamp)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(2)
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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[0u]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[5u]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    COUNT(`timestamp`)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    AVG(level), MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[2.];[0]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[3.]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    SUM(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[46000;]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_Null_Count) {
        TAggregationTestCase testCase;
        testCase
            .SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    SUM(level), COUNT(*), AVG(level)
                FROM `/Root/tableWithNulls`
            )")
            .SetExpectedReply("[[[15];10u;[3.]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[15]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

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
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[0]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT
                    MAX(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[4]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");

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
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        testCase.FillExpectedAggregationGroupByPlanOptions();
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id=1
            )")
            .SetExpectedReply("[[[1]]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id > 5
            )")
            .SetExpectedReply("[[#]]")
            .AddExpectedPlanOptions("TKqpOlapAgg");
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
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[1;["val1"];#]])");
        TestTableWithNulls({testCase});
    }


    Y_UNIT_TEST(Json_GetValue_Minus) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.'col-abc'"), JSON_VALUE(jsondoc, "$.'col-abc'") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.'col-abc'") = "val-abc" AND id = 1;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[1;["val-abc"];#]])");

        TestTableWithNulls({testCase});
    }

    Y_UNIT_TEST(Json_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1" RETURNING String), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.col1" RETURNING String) = "val1" AND id = 1;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[1;["val1"];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 1;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[1;[16];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1") = "val1" AND id = 6;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1" RETURNING String) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1" RETURNING String) = "val1" AND id = 6;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int"), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 6;
            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
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
            .AddExpectedPlanOptions("KqpOlapJsonExists")
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
            .AddExpectedPlanOptions("KqpOlapJsonExists")
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

    Y_UNIT_TEST(MixedJsonAndOlapApply) {
        TAggregationTestCase testCase;
        //(R"({"col1": "val1", "col-abc": "val-abc", "obj": {"obj_col2_int": 16}})"
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.\"col-abc\"") FROM `/Root/tableWithNulls`
                WHERE id = 1
                    AND  JSON_VALUE(jsonval, "$.col1") = "val1"
                    AND  JSON_VALUE(jsonval, "$.\"col-abc\"") ilike "%A%b%"
                    AND JSON_EXISTS(jsonval, "$.obj.obj_col2_int")

            )")
            .AddExpectedPlanOptions("KqpOlapJsonValue")
            .AddExpectedPlanOptions("KqpOlapJsonExists")
            .AddExpectedPlanOptions("KqpOlapApply")
            .SetExpectedReply(R"([[1;["val-abc"]]])")
        ;

        TestTableWithNulls({testCase});
    }

    // JSON_VALUE must be computed by the column shard (`KqpOlapJsonValue`) and only its value must be passed
    // into `KqpOlapApply`, instead of the whole JSON column.
    Y_UNIT_TEST(JsonValuePushedIntoOlapApply) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        auto tableClient = kikimr.GetTableClient();

        const auto countOccurrences = [](const TString& text, const TString& pattern) {
            ui32 count = 0;
            for (size_t pos = text.find(pattern); pos != TString::npos; pos = text.find(pattern, pos + 1)) {
                ++count;
            }
            return count;
        };

        struct TCase {
            TString Query;
            TString ExpectedReply;
            // Each JSON_VALUE of the predicate is computed once by `KqpOlapJsonValue` and shared by all `KqpOlapApply` callables.
            ui32 ExpectedJsonValues;
        };

        const std::vector<TCase> cases = {
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsonval, "$.\"col-abc\"") ILIKE "%A%b%"
                    ORDER BY id;
                )",
                R"([[1];[2];[3];[4];[5]])",
                1
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsondoc, "$.\"col-abc\"") ILIKE "%A%b%"
                    ORDER BY id;
                )",
                R"([[6];[7];[8];[9];[10]])",
                1
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int") AS Int32) + 1 > 16 AND id < 3
                    ORDER BY id;
                )",
                R"([[1];[2]])",
                1
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsonval, "$.col1") || JSON_VALUE(jsonval, "$.\"col-abc\"") LIKE "val1%abc" AND id > 3
                    ORDER BY id;
                )",
                R"([[4];[5]])",
                2
            },
        };

        for (const auto& testCase : cases) {
            auto explainResult = StreamExplainQuery(testCase.Query, tableClient);
            UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
            const auto ast = TString(CollectStreamResult(explainResult).QueryStats->Getquery_ast());
            Cerr << "AST: " << ast << Endl;

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != TString::npos, "Predicate is not pushed down. Query: " << testCase.Query);
            UNIT_ASSERT_C(ast.find("KqpOlapApply") != TString::npos, "Predicate is not pushed by KqpOlapApply. Query: " << testCase.Query);
            UNIT_ASSERT_VALUES_EQUAL_C(countOccurrences(ast, "KqpOlapJsonValue"), testCase.ExpectedJsonValues,
                "Unexpected number of KqpOlapJsonValue callables. Query: " << testCase.Query);
            // Only `id` is selected, so JSON must not be processed outside of `KqpOlapJsonValue` at all.
            UNIT_ASSERT_C(ast.find("Json2.") == TString::npos,
                "JSON is processed by Json2 UDF instead of KqpOlapJsonValue. Query: " << testCase.Query);

            auto it = tableClient.StreamExecuteScanQuery(testCase.Query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), testCase.ExpectedReply);
        }
    }

    // `KqpOlapJsonValue` kernel differs from `JsonValue` for non-Utf8 RETURNING types (e.g. it does not support dates
    // and uses lenient `SqlValueConvertToUtf8` for String), so such JSON_VALUE must stay inside `KqpOlapApply` lambda.
    Y_UNIT_TEST(JsonValueWithReturningTypeIsNotPushedIntoOlapApply) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        auto tableClient = kikimr.GetTableClient();

        struct TCase {
            TString Query;
            TString ExpectedReply;
        };

        // "obj_col2_int" is a number 16 in the JSON.
        const std::vector<TCase> cases = {
            {
                // Strict `SqlValueUtf8` returns NULL for a number, so nothing must match.
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING String) ILIKE "%1%6%"
                    ORDER BY id;
                )",
                R"([])"
            },
            {
                // Kernel does not support date types at all.
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Date) AS Utf8) LIKE "1970%17"
                    ORDER BY id;
                )",
                R"([[1];[2];[3];[4];[5]])"
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int32) AS Utf8) ILIKE "%1%6%"
                    ORDER BY id;
                )",
                R"([[1];[2];[3];[4];[5]])"
            },
        };

        for (const auto& testCase : cases) {
            auto explainResult = StreamExplainQuery(testCase.Query, tableClient);
            UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
            const auto ast = TString(CollectStreamResult(explainResult).QueryStats->Getquery_ast());
            Cerr << "AST: " << ast << Endl;

            auto it = tableClient.StreamExecuteScanQuery(testCase.Query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), testCase.ExpectedReply);

            UNIT_ASSERT_C(ast.find("KqpOlapApply") != TString::npos, "Predicate is not pushed by KqpOlapApply. Query: " << testCase.Query);
            UNIT_ASSERT_C(ast.find("KqpOlapJsonValue") == TString::npos,
                "JSON_VALUE with non-Utf8 RETURNING type must not be pushed as KqpOlapJsonValue. Query: " << testCase.Query);
        }
    }

    // Pushing JSON_VALUE as `KqpOlapJsonValue` before `KqpOlapApply` must not widen the set of predicates which are pushed down:
    // these predicates have never been pushed and are still computed by KQP (via `Json2` UDFs) over the whole JSON column.
    Y_UNIT_TEST(JsonValueExternalArgsDoNotWidenPushdown) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        auto tableClient = kikimr.GetTableClient();

        struct TCase {
            TString Query;
            TString ExpectedReply;
        };

        const std::vector<TCase> cases = {
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsonval, "$.col1") IS NULL AND id < 8
                    ORDER BY id;
                )",
                R"([[6];[7]])"
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsondoc, "$.col1") IS NOT NULL
                    ORDER BY id;
                )",
                R"([[6];[7];[8];[9];[10]])"
            },
            {
                R"(
                    SELECT id FROM `/Root/tableWithNulls`
                    WHERE JSON_VALUE(jsonval, "$.col1") IN ("val1", "val2")
                    ORDER BY id;
                )",
                R"([[1];[2];[3];[4];[5]])"
            },
        };

        for (const auto& testCase : cases) {
            auto explainResult = StreamExplainQuery(testCase.Query, tableClient);
            UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
            const auto ast = TString(CollectStreamResult(explainResult).QueryStats->Getquery_ast());
            Cerr << "AST: " << ast << Endl;

            auto it = tableClient.StreamExecuteScanQuery(testCase.Query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), testCase.ExpectedReply);

            UNIT_ASSERT_C(ast.find("KqpOlapJsonValue") == TString::npos, "Unexpected KqpOlapJsonValue pushdown. Query: " << testCase.Query);
            UNIT_ASSERT_C(ast.find("KqpOlapApply") == TString::npos, "Unexpected KqpOlapApply pushdown. Query: " << testCase.Query);
            UNIT_ASSERT_C(ast.find("Json2.") != TString::npos, "JSON_VALUE is expected to be computed by KQP. Query: " << testCase.Query);
        }
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

    Y_UNIT_TEST(FloatSum) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `olap_table` (
                        id Uint64 NOT NULL,
                        value Float,
                        PRIMARY KEY (id)
                    ) WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                    R"(
                        INSERT INTO `olap_table` (id, value) VALUES (1u, 0.4f);
                        INSERT INTO `olap_table` (id, value) VALUES (2u, 0.85f);
                        INSERT INTO `olap_table` (id, value) VALUES (3u, 11.3f);
                        INSERT INTO `olap_table` (id, value) VALUES (4u, 7.15f);
                        INSERT INTO `olap_table` (id, value) VALUES (5u, 0.3f);
                    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(R"(
                --!syntax_v1
                SELECT SUM(value) FROM `olap_table`
                WHERE id = 1
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            TString result = FormatResultSetYson(status.GetResultSet(0));
            CompareYson(result, R"([[[0.400000006;]]])");
        }
    }
}

}
