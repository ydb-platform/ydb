#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_plan_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

void TestRangePushdown(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/t1` (
            a Int64 NOT NULL,
            b Int64 NOT NULL,
            c Int64,
            primary key(a, b)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    schemaQ = R"(
        CREATE TABLE `/Root/t2` (
            a Int64 NOT NULL,
            b Int64 NOT NULL,
            c Int64,
            primary key(a, b)
        )
    )";
    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i)
            .AddMember("c").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rows1;
    rows1.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows1.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i)
            .AddMember("c").Int64(i)
            .EndStruct();
    }
    rows1.EndList();

    resultUpsert = db.BulkUpsert("/Root/t2", rows1.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> results = {
        R"([[1;1]])",
        R"([[2;2];[3;3];[4;4];[5;5];[6;6];[7;7];[8;8];[9;9]])",
        R"([[2;2];[3;3];[4;4];[5;5];[6;6];[7;7];[8;8]])",
        R"([[2;2];[3;3];[4;4];[5;5];[6;6];[7;7];[8;8]])",
        R"([[1;1]])",
        R"([[1;1]])",
        R"([[1;1];[3;3];[5;5];[7;7];[9;9]])",
        R"([[3;3];[5;5];[7;7];[9;9]])",
        R"([[8;8];[9;9]])",
    };

    std::vector<std::string> queries = {
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a == 1 and t1.b = 1 order by t1.a;
        )",
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > 1 order by t1.a;
        )",
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > 1 and t1.a < 9 order by t1.a;
        )",
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > 1 and t1.c < 9 order by t1.a;
        )",
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a = 1 and t1.c = 1 order by t1.a;
        )",
        // FIXME: This is a fullscan for t2 table, because we do not push t2.a = 1 and t2.b = 1
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a WHERE t1.a = 1 and t2.b = 1 order by t1.a;
        )",
        // IN lists longer than 5 items are not expanded into an Or chain by peephole.
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a IN (1, 3, 5, 7, 9, 11) order by t1.a;
        )",
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a IN (1, 3, 5, 7, 9, 11) and t1.c > 1 order by t1.a;
        )",
        // IS NOT NULL over a non optional key column.
        R"(
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a IS NOT NULL and t1.a > 7 order by t1.a;
        )",
    };

    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.find("RangeFinalize") != TString::npos, "Ranges not pushed for query: " << query);

        result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                     .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }

    const std::vector<std::vector<std::pair<std::string, ui64>>> paramsVector{
        {{"$param0", 1}, {"$param1", 1}}, {{"$param0", 1}, {"$param1", 9}}, {{"$param0", 1}, {"$param1", 9}}, {{"$param0", 1}, {"$param1", 1}}};

    queries = {
        R"(
            declare $param0 as Int64;
            declare $param1 as Int64;
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a == $param0 and t1.b = $param1 order by t1.a;
        )",
        R"(
            declare $param0 as Int64;
            declare $param1 as Int64;
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > $param0 and t1.a < $param1 order by t1.a;
        )",
        R"(
            declare $param0 as Int64;
            declare $param1 as Int64;
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > $param0 and t1.c < $param1 order by t1.a;
        )",
        R"(
            declare $param0 as Int64;
            declare $param1 as Int64;
            SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a = $param0 and t1.c = $param1 order by t1.a;
        )",
    };

    results = {
        R"([[1;1]])",
        R"([[2;2];[3;3];[4;4];[5;5];[6;6];[7;7];[8;8]])",
        R"([[2;2];[3;3];[4;4];[5;5];[6;6];[7;7];[8;8]])",
        R"([[1;1]])",
    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto& query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.find("RangeFinalize") != TString::npos, "Ranges not pushed");

        auto params = paramsVector[i];
        // clang-format off
        auto qParams = TParamsBuilder()
            .AddParam(params[0].first)
                .Int64(params[0].second)
            .Build()
            .AddParam(params[1].first)
                .Int64(params[1].second)
            .Build()
        .Build();
        // clang-format on

        result =
            session
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), qParams, NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
    }
}

void TestOlapProjectionPushdown(bool explain) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    // Fallback is enabled to be able to insert values by `INSERT VALUES`.
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(!explain);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto tableClient = kikimr.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto queryClient = kikimr.GetQueryClient();
    auto result = queryClient.GetSession().GetValueSync();
    NStatusHelpers::ThrowOnError(result);
    auto session2 = result.GetSession();

    auto res = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/foo` (
            a Int64	NOT NULL,
            b Int32,
            timestamp Timestamp,
            jsonDoc JsonDocument,
            jsonDoc1 JsonDocument,
            primary key(a)
        )
        PARTITION BY HASH(a)
        WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT(res.IsSuccess());

    if (!explain) {
        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (1, 1, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"a.b.c" : "a1", "b.c.d" : "b1", "c.d.e" : "c1"}'), JsonDocument('{"a" : "1.1", "b" : "1.2", "c" : "1.3"}'));
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (2, 11, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"a.b.c" : "a2", "b.c.d" : "b2", "c.d.e" : "c2"}'), JsonDocument('{"a" : "2.1", "b" : "2.2", "c" : "2.3"}'));
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (3, 11, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"b.c.a" : "a3", "b.c.d" : "b3", "c.d.e" : "c3"}'), JsonDocument('{"x" : "3.1", "y" : "1.2", "z" : "1.3"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());
    }

    std::vector<TString> queries = {
        R"(
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA YqlSelect = 'force';

            SELECT a, JSON_VALUE(jsonDoc,"$.\"a.b.c\"") as result FROM `/Root/foo`
            where b > 10
            order by a;
        )",
        R"(
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA YqlSelect = 'force';

            SELECT a, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as result, JSON_VALUE(jsonDoc1, "$.\"x\"") as result1 FROM `/Root/foo`
            where b > 10
            order by a;
        )",
        R"(
            PRAGMA kikimr.OptEnableOlapPushdownProjections="true";
            PRAGMA YqlSelect = 'force';

            SELECT a, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as result
            FROM `/Root/foo`
            WHERE timestamp = Timestamp("1970-01-01T00:00:03.000001Z")
            ORDER BY a
            LIMIT 1;
        )",
        R"(
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA YqlSelect = 'force';

            SELECT a, (JSON_VALUE(jsonDoc, "$.\"a.b.c\"") in ["a1", "a3", "a4"]) as col1, CAST(JSON_VALUE(jsonDoc1, "$.\"a\"") as Double) as col2
            FROM `/Root/foo`
            ORDER BY a;
        )",
        /* Multiple projection for same column is not supported in new RBO.
        R"(
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA YqlSelect = 'force';

            SELECT a, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as result, JSON_VALUE(jsonDoc, "$.\"c.d.e\"") as result1 FROM `/Root/foo`
            where b > 10
            order by a;
        )",
        R"(
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA YqlSelect = 'force';

            SELECT (JSON_VALUE(jsonDoc, "$.\"a.b.c\"") in ["a1", "a3", "a4"]) as col1,
                   CAST(JSON_VALUE(jsonDoc1, "$.\"a\"") as Double) as col2,
                   CAST(JSON_VALUE(jsonDoc1, "$.\"b\"") as Double) as col3
            FROM `/Root/foo`
            ORDER BY col2;
        )",
        */
    };

    const std::vector<TString> results = {
         R"([[2;["a2"]];[3;#]])",
         R"([[2;["a2"];#];[3;#;["3.1"]]])",
         R"([[1;["a1"]]])",
         R"([[1;[%true];[1.1]];[2;[%false];[2.1]];[3;#;#]])"
    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto query = queries[i];

        if (explain) {
            auto result =
                session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapProjections") != std::string::npos, TStringBuilder() << "Projections not pushed down. Query: " << query);
            UNIT_ASSERT_C(ast.find("KqpOlapProjection") != std::string::npos, TStringBuilder() << "Projection not pushed down. Query: " << query);

            if (i == 0) {
                UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan");
                const auto plan = TString{*result.GetStats()->GetPlan()};
                const auto simplifiedPlan = GetSimplifiedPlan(plan);
                const auto* readOp = FindOperatorByStringField(simplifiedPlan, "Table", "foo");
                UNIT_ASSERT_C(readOp, plan);
                UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "a"), plan);
                UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "b"), plan);
                UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "jsonDoc"), plan);
                UNIT_ASSERT_C(!StringArrayFieldContains(*readOp, "ReadColumns", "timestamp"), plan);
                UNIT_ASSERT_C(!StringArrayFieldContains(*readOp, "ReadColumns", "jsonDoc1"), plan);
            }
        } else {
            auto result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            TString output = FormatResultSetYson(result.GetResultSet(0));
            //Cout << output << Endl;
            CompareYson(output, results[i]);
        }
    }
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(TopSortPushedToRowReadAndKept) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeRes = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Uint64,
                b String,
                PRIMARY KEY (a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeRes.IsSuccess(), schemeRes.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (ui64 i = 0; i < 5; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Uint64(i)
                .AddMember("b").String(TStringBuilder() << "v" << i)
                .EndStruct();
        }
        rows.EndList();
        auto upsertRes = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertRes.IsSuccess(), upsertRes.GetIssues().ToString());

        auto explainAst = [&](const TString& query) -> TString {
            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return TString(result.GetAst());
        };

        // ORDER BY a (PK) LIMIT 3: order is pushed into the read ("Sorted"), but the
        // WideTopSort operator must stay in the AST.
        {
            auto ast = explainAst("SELECT a FROM `/Root/t1` ORDER BY a LIMIT 3;");
            UNIT_ASSERT_C(ast.Contains("'\"Sorted\""),
                "Expected the \"Sorted\" pushdown into the read settings, AST: " << ast);
            UNIT_ASSERT_C(ast.Contains("WideTopSort"),
                "WideTopSort must stay after the TopSort pushdown to the row read "
                "(merge connection for row storage is not produced yet), AST: " << ast);
        }

        // ORDER BY a DESC (PK) LIMIT 3: the ascending direction is not pushed here, so
        // WideTopSort stays and the read settings must not carry "Sorted".
        {
            auto ast = explainAst("SELECT a FROM `/Root/t1` ORDER BY a DESC LIMIT 3;");
            UNIT_ASSERT_C(!ast.Contains("'\"Sorted\""),
                "ASC-only pushdown; DESC must not push \"Sorted\" into the read, AST: " << ast);
            UNIT_ASSERT_C(ast.Contains("WideTopSort"),
                "WideTopSort must stay for ORDER BY PK DESC LIMIT, AST: " << ast);
        }

        // Negative control: ORDER BY b (non-PK) LIMIT 3 -> no pushdown, WideTopSort stays,
        // the read settings must not carry "Sorted".
        {
            auto ast = explainAst("SELECT a FROM `/Root/t1` ORDER BY b LIMIT 3;");
            UNIT_ASSERT_C(!ast.Contains("'\"Sorted\""),
                "no pushdown for ORDER BY non-PK; read must not carry \"Sorted\", AST: " << ast);
            UNIT_ASSERT_C(ast.Contains("WideTopSort"),
                "WideTopSort must stay for ORDER BY non-PK LIMIT, AST: " << ast);
        }
    }

    Y_UNIT_TEST_TWIN(RangePushdown, ColumnStore) {
        TestRangePushdown(ColumnStore);
    }

    Y_UNIT_TEST(RangePushdownExplain) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Int64,
                primary key(a, b)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        {
            auto db = kikimr.GetQueryClient();
            auto res = db.GetSession().GetValueSync();
            NStatusHelpers::ThrowOnError(res);
            auto session = res.GetSession();

            auto result =
                session.ExecuteQuery(
                    R"(
                        SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.a > 1 and t1.a < 9 order by t1.a;
                    )",
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto plan = TString{*result.GetStats()->GetPlan()};
            Cout << plan << Endl;
            NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, true, Cout, 0);
            queryPlanPrinter.Print(plan);
        }
    }

    Y_UNIT_TEST(Indexes_newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        // appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Table` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const std::vector<std::tuple<i32, i32, TString, TString>> data = {
            {0, 0, "0", "1"}, {0, 0, "1", "2"}, {0, 1, "0", "3"}, {0, 1, "1", "4"},
            {1, 0, "0", "5"}, {1, 0, "1", "6"}, {1, 1, "0", "7"}, {1, 1, "1", "8"},
        };

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [key, subKey1, subKey2, value] : data) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("Key").OptionalInt32(key)
                .AddMember("SubKey1").OptionalInt32(subKey1)
                .AddMember("SubKey2").OptionalString(subKey2)
                .AddMember("Value1").OptionalString(value)
                .AddMember("Value2").OptionalString(value)
                .EndStruct();
        }
        rows.EndList();
        auto upsertResult = db.BulkUpsert("/Root/Table", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        for (const auto& addIndex : {
                 "ALTER TABLE `/Root/Table` ADD INDEX Index12 GLOBAL ON (SubKey1, SubKey2);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index21 GLOBAL ON (SubKey2, Value1);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index212 GLOBAL ON (SubKey2) COVER (Value2);",
             }) {
            result = session.ExecuteSchemeQuery(addIndex).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            // R"(
            //     -- принудительно выбирается индекс с помощью конструкции VIEW, sort is free
            //     SELECT t.SubKey1
            //     FROM `/Root/Table` VIEW `Index12` as t
            //     WHERE t.SubKey2 = "0"
            //     order by t.SubKey1;
            // )",
            R"(
                -- выбирается индекс Index12 (not PK), point prefix для него 1, thus re-sort is expected
                SELECT *
                FROM `/Root/Table`
                WHERE SubKey1 = 1 and SubKey2 > "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- не выбирается никакой индекс, так как PK основной таблицы имеет самый длинный point prefix, sort is free
                SELECT * 
                FROM Table `/Root/Table`
                WHERE Key = 0 and SubKey1 = 0 And SubKey2 = "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212 (not PK or Index21), since Index212 has order SubKey2 and Key, thus re-sort is expected
                SELECT * 
                FROM Table `/Root/Table`
                WHERE Key = 0 and SubKey2 = "1"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- должен использоваться Index12, sort is free
                SELECT * 
                FROM Table 
                WHERE Key >= 0 and SubKey1 = 0 And SubKey2 = "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index12, sort is free
                SELECT * 
                FROM Table 
                WHERE SubKey1 > 0
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index21 (not Index212), both tie on score, Index21 is lexicographically first, thus re-sort is expected
                SELECT *
                FROM Table
                WHERE SubKey2 = "1"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212, still re-order is expected
                SELECT Value2
                FROM Table
                WHERE SubKey2 = "0"
                ORDER BY Value2;
            )",
            R"(
                -- используется Index12, thus re-sort is expected
                SELECT * 
                FROM Table 
                WHERE SubKey1 = 0
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212 (not Index21), since the sort is free due to key order of covering Index212
                SELECT Key, SubKey1, SubKey2
                FROM Table
                WHERE SubKey2 = "1"
                ORDER BY SubKey2, Key, SubKey1
                LIMIT 2;
            )",
        };

        std::vector<std::string> results = {
            // R"([[[0]];[[0]];[[1]];[[1]]])",
            R"([[[0];[1];["1"];["4"];["4"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[[0];[0];["0"];["1"];["1"]]])",
            R"([[[0];[0];["1"];["2"];["2"]];[[0];[1];["1"];["4"];["4"]]])",
            R"([[[0];[0];["0"];["1"];["1"]];[[1];[0];["0"];["5"];["5"]]])",
            R"([[[0];[1];["0"];["3"];["3"]];[[0];[1];["1"];["4"];["4"]];[[1];[1];["0"];["7"];["7"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[[0];[0];["1"];["2"];["2"]];[[0];[1];["1"];["4"];["4"]];[[1];[0];["1"];["6"];["6"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[["1"]];[["3"]];[["5"]];[["7"]]])",
            R"([[[0];[0];["0"];["1"];["1"]];[[0];[0];["1"];["2"];["2"]];[[1];[0];["0"];["5"];["5"]];[[1];[0];["1"];["6"];["6"]]])",
            R"([[[0];[0];["1"]];[[0];[1];["1"]]])",
        };

        std::vector<TString> expectedIndexes = {
            // "Index12",
            "Index12",
            "", // PK
            "Index212",
            "Index12",
            "Index12",
            "Index21",
            "Index212",
            "Index12",
            "Index212",
        };

        const std::string header = "PRAGMA ydb.OptDisableAutoIndexSelection = \"false\";\n";
        for (ui32 i = 0; i < queries.size(); ++i) {
            const std::string query = header + queries[i];
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            Cout << query << "\n";
            auto result2 = session2.ExecuteQuery(query,
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();
            const auto plan = TString{*result2.GetStats()->GetPlan()};
            PrintPlan(plan, /*analyzeMode=*/false);
            const auto ast = TString{*result2.GetStats()->GetAst()};
            Cout << "Plan AST:\n" << ast;

            const auto& expectedIndex = expectedIndexes[i];
            if (expectedIndex.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " must read the main table, ast:\n" << ast);
                UNIT_ASSERT_C(!plan.Contains("indexImplTable"), "query #" << i << " must read the main table, plan:\n" << plan);
            } else {
                const auto implTable = TString::Join(expectedIndex, "/indexImplTable");
                UNIT_ASSERT_C(ast.Contains(implTable), "query #" << i << " expected index " << expectedIndex << ", ast:\n" << ast);
                UNIT_ASSERT_C(plan.Contains(expectedIndex) && plan.Contains("indexImplTable"), "query #" << i << " expected index " << expectedIndex << ", plan:\n" << plan);
            }
        }
    }

    Y_UNIT_TEST(Indexes_oldRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        // appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
        // appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Table` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2),
                INDEX Index12 GLOBAL ON (SubKey1, SubKey2),
                INDEX Index21 GLOBAL ON (SubKey2, Value1),
                INDEX Index212 GLOBAL ON (SubKey2) COVER (Value2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Table` (Key, SubKey1, SubKey2, Value1, Value2) VALUES
                (0, 0, "0", "1", "1"),
                (0, 0, "1", "2", "2"),
                (0, 1, "0", "3", "3"),
                (0, 1, "1", "4", "4"),
                (1, 0, "0", "5", "5"),
                (1, 0, "1", "6", "6"),
                (1, 1, "0", "7", "7"),
                (1, 1, "1", "8", "8");
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                -- принудительно выбирается индекс с помощью конструкции VIEW, sort is free
                SELECT t.SubKey1
                FROM `/Root/Table` VIEW `Index12` as t
                WHERE t.SubKey2 = "0"
                order by t.SubKey1;
            )",
            R"(
                -- выбирается индекс Index12 (not PK), point prefix для него 1, thus re-sort is expected
                SELECT *
                FROM `/Root/Table`
                WHERE SubKey1 = 1 and SubKey2 > "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- не выбирается никакой индекс, так как PK основной таблицы имеет самый длинный point prefix, sort is free
                SELECT * 
                FROM Table `/Root/Table`
                WHERE Key = 0 and SubKey1 = 0 And SubKey2 = "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212 (not PK or Index21), since Index212 has order SubKey2 and Key, thus re-sort is expected
                SELECT * 
                FROM Table `/Root/Table`
                WHERE Key = 0 and SubKey2 = "1"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- должен использоваться Index12, sort is free
                SELECT * 
                FROM Table 
                WHERE Key >= 0 and SubKey1 = 0 And SubKey2 = "0"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index12, sort is free
                SELECT * 
                FROM Table 
                WHERE SubKey1 > 0
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index21 (not Index212), since Index21 is declared first, thus re-sort is expected
                SELECT * 
                FROM Table 
                WHERE SubKey2 = "1"
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212, still re-order is expected
                SELECT Value2 
                FROM Table 
                WHERE SubKey2 = "0"
                ORDER BY Value2;
            )",
            R"(
                -- используется Index12, thus re-sort is expected
                SELECT * 
                FROM Table 
                WHERE SubKey1 = 0
                ORDER BY Key, SubKey1, SubKey2;
            )",
            R"(
                -- используется Index212 (not Index21), since the sort is free due to key order of covering Index212
                SELECT Key, SubKey1, SubKey2
                FROM Table
                WHERE SubKey2 = "1"
                ORDER BY SubKey2, Key, SubKey1
                LIMIT 2;
            )",
        };

        std::vector<std::string> results = {
            R"([[[0]];[[0]];[[1]];[[1]]])",
            R"([[[0];[1];["1"];["4"];["4"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[[0];[0];["0"];["1"];["1"]]])",
            R"([[[0];[0];["1"];["2"];["2"]];[[0];[1];["1"];["4"];["4"]]])",
            R"([[[0];[0];["0"];["1"];["1"]];[[1];[0];["0"];["5"];["5"]]])",
            R"([[[0];[1];["0"];["3"];["3"]];[[0];[1];["1"];["4"];["4"]];[[1];[1];["0"];["7"];["7"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[[0];[0];["1"];["2"];["2"]];[[0];[1];["1"];["4"];["4"]];[[1];[0];["1"];["6"];["6"]];[[1];[1];["1"];["8"];["8"]]])",
            R"([[["1"]];[["3"]];[["5"]];[["7"]]])",
            R"([[[0];[0];["0"];["1"];["1"]];[[0];[0];["1"];["2"];["2"]];[[1];[0];["0"];["5"];["5"]];[[1];[0];["1"];["6"];["6"]]])",
            R"([[[0];[0];["1"]];[[0];[1];["1"]]])",
        };

        std::vector<TString> expectedIndexes = {
            "Index12",
            "Index12",
            "", // PK
            "Index212",
            "Index12",
            "Index12",
            "Index21",
            "Index212",
            "Index12",
            "Index212",
        };

        const std::string header = "PRAGMA ydb.OptDisableAutoIndexSelection = \"false\";\n";
        for (ui32 i = 0; i < queries.size(); ++i) {
            const std::string query = header + queries[i];
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            Cout << query << "\n";
            auto result2 = session2.ExecuteQuery(query,
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();
            const auto plan = TString{*result2.GetStats()->GetPlan()};
            PrintPlan(plan, /*analyzeMode=*/false);
            const auto ast = TString{*result2.GetStats()->GetAst()};
            Cout << "Plan AST:\n" << ast;

            const auto& expectedIndex = expectedIndexes[i];
            if (expectedIndex.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " must read the main table, ast:\n" << ast);
                UNIT_ASSERT_C(!plan.Contains("indexImplTable"), "query #" << i << " must read the main table, plan:\n" << plan);
            } else {
                const auto implTable = TString::Join(expectedIndex, "/indexImplTable");
                UNIT_ASSERT_C(ast.Contains(implTable), "query #" << i << " expected index " << expectedIndex << ", ast:\n" << ast);
                UNIT_ASSERT_C(plan.Contains(expectedIndex) && plan.Contains("indexImplTable"), "query #" << i << " expected index " << expectedIndex << ", plan:\n" << plan);
            }
        }
    }

    Y_UNIT_TEST(OlapPredicatePushdown) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        const std::vector<TString> results = {R"([[1;[2]]])", R"([[0;[1]];[1;[2]]])", R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]]])"};

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a, t1.b FROM `/Root/t1` as t1 WHERE t1.b == 2 order by t1.a;
            )",
            R"(
                SELECT a, b FROM `/Root/t1` WHERE b <= 2 order by a;
            )",
            R"(
                SELECT a, b FROM `/Root/t1` WHERE coalesce(b, 11) < 10 order by a;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();

            // Explain.
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Filter not pushed down. Query: " << query);

            // Execute.
            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(OlapProjection, Explain) {
        TestOlapProjectionPushdown(Explain);
    }

    Y_UNIT_TEST(OlapProjectionPreservesLiveSourceColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/docs` (id Int64 NOT NULL, doc JsonDocument, PRIMARY KEY(id))
            PARTITION BY HASH(id) WITH (STORE = COLUMN);
        )").GetValueSync().IsSuccess());
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto insert = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/docs` (id, doc) VALUES (1, JsonDocument('{"x":"value"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        const auto before = GetNewRBOCompileCounters(kikimr);
        const auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = 'true';
            SELECT doc, JSON_VALUE(doc, '$.x') AS x FROM `/Root/docs`;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto after = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(after.first, before.first + 1);
        UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
        const auto& rows = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.GetColumnsMeta()[0].Type.ToString(), "JsonDocument?");
        UNIT_ASSERT_VALUES_EQUAL(rows.GetColumnsMeta()[1].Type.ToString(), "Utf8?");
        TResultSetParser parser(rows);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(0).GetOptionalJsonDocument(), R"({"x":"value"})");
        UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(1).GetOptionalUtf8(), "value");
        UNIT_ASSERT(!parser.TryNextRow());
    }

}

} // namespace NKikimr::NKqp
