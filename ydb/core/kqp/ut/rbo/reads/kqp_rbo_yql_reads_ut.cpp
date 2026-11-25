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
#include <ydb/core/kqp/common/simple/user_request_context.h>
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

    // A single range known at compile time is passed to a row table read as literal key bounds, so it is not
    // computed in a separate transaction. Other ranges, and the ranges of a column table read, are computed.
    auto checkRanges = [&](const TString& ast, const std::string& query, bool literal) {
        if (literal && !columnTables) {
            UNIT_ASSERT_C(ast.Contains("KqlKeyInc") || ast.Contains("KqlKeyExc"),
                          "Ranges not pushed as a literal range for query: " << query << ", ast:\n" << ast);
            UNIT_ASSERT_C(!ast.Contains("RangeFinalize") && !ast.Contains("kqp_physical_tx_param_binding"),
                          "Ranges computed for query: " << query << ", ast:\n" << ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("RangeFinalize"), "Ranges not pushed for query: " << query << ", ast:\n" << ast);
        }
    };
    // IN lists are several ranges. The bound of IS NOT NULL and a literal bound cannot be compared at compile time,
    // so their intersection is not a literal range either.
    const std::vector<bool> literalRanges = {true, true, true, true, true, true, false, false, false};

    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        checkRanges(TString{*result.GetStats()->GetAst()}, query, literalRanges[i]);

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
        // The parameters have the key column type, so the bounds are the parameters themselves.
        checkRanges(TString{*result.GetStats()->GetAst()}, query, /*literal=*/true);

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

// The names of the operators in the plan stage that reads `table`, or nothing if no stage reads it.
TVector<TString> GetReadStageOperators(const NJson::TJsonValue& node, const TString& table) {
    if (node.IsArray()) {
        for (const auto& child : node.GetArraySafe()) {
            if (auto operators = GetReadStageOperators(child, table); !operators.empty()) {
                return operators;
            }
        }
        return {};
    }
    if (!node.IsMap()) {
        return {};
    }

    const auto& map = node.GetMapSafe();
    if (const auto operators = map.find("Operators"); operators != map.end() && operators->second.IsArray()) {
        TVector<TString> names;
        bool reads = false;
        for (const auto& op : operators->second.GetArraySafe()) {
            const auto& opMap = op.GetMapSafe();
            if (const auto name = opMap.find("Name"); name != opMap.end() && name->second.IsString()) {
                names.push_back(name->second.GetStringSafe());
            }
            if (const auto path = opMap.find("Path"); path != opMap.end() && path->second.IsString()) {
                reads |= path->second.GetStringSafe() == table;
            }
        }
        if (reads) {
            return names;
        }
    }
    for (const auto& [_, child] : map) {
        if (auto operators = GetReadStageOperators(child, table); !operators.empty()) {
            return operators;
        }
    }
    return {};
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(TopSortPushedToRowRead) {
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

        // ORDER BY a (PK) LIMIT 3: the read returns its rows in the key order ("Sorted"), and the
        // merge connection out of its stage keeps the order, so there is no sort.
        {
            auto ast = explainAst("SELECT a FROM `/Root/t1` ORDER BY a LIMIT 3;");
            UNIT_ASSERT_C(ast.Contains("'\"Sorted\""),
                "Expected the \"Sorted\" pushdown into the read settings, AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("WideTopSort"), "The sorted row read needs no sort, AST: " << ast);
            UNIT_ASSERT_C(ast.Contains("DqCnMerge"), "Expected a merge connection out of the read stage, AST: " << ast);
        }

        // ORDER BY a DESC (PK) LIMIT 3: as in the old optimizer, the read returns its rows in the reverse
        // key order ("Reverse"), so there is no sort either.
        {
            auto ast = explainAst("SELECT a FROM `/Root/t1` ORDER BY a DESC LIMIT 3;");
            UNIT_ASSERT_C(ast.Contains("'\"Sorted\"") && ast.Contains("'\"Reverse\""),
                "Expected a read in the reverse key order, AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("WideTopSort"), "The reverse row read needs no sort, AST: " << ast);
            UNIT_ASSERT_C(ast.Contains("DqCnMerge"), "Expected a merge connection out of the read stage, AST: " << ast);
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

    Y_UNIT_TEST(LiteralRanges) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                s String NOT NULL,
                v Int64,
                PRIMARY KEY (a, s)
            );

            CREATE TABLE `/Root/t2` (
                k Int64,
                v Int64,
                PRIMARY KEY (k)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, s, v] : TVector<std::tuple<i64, TString, i64>>{
                     {1, "a", 10}, {1, "b", 20}, {2, "a", 20}, {3, "c", 30}, {4, "d", 40}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").Int64(a)
                    .AddMember("s").String(s)
                    .AddMember("v").OptionalInt64(v)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [k, v] : TVector<std::pair<std::optional<i64>, i64>>{{std::nullopt, 100}, {1, 101}, {2, 102}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("k").OptionalInt64(k)
                    .AddMember("v").OptionalInt64(v)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t2", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // BulkUpsert does not support tables with sync indexes, so the index is built after loading.
        schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_v GLOBAL ON (v);").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        struct TCase {
            TString Query;
            // A literal range is passed to the read as is; otherwise the ranges are computed in a separate transaction.
            bool Literal;
            TString Result;
            std::optional<i64> Param;
            TString ReadTable;
        };

        const TVector<TCase> cases = {
            // The Int32 literal is converted to the Int64 key type at compile time.
            {"SELECT a, s FROM `/Root/t1` WHERE a = 1 AND s = \"b\" ORDER BY a, s;", true, R"([[1;"b"]])", {}, "/Root/t1"},
            // The Utf8 literal is converted to the String key type at compile time.
            {"SELECT a, s FROM `/Root/t1` WHERE a = 1 AND s = \"b\"u ORDER BY a, s;", true, R"([[1;"b"]])", {}, "/Root/t1"},
            {"SELECT a, s FROM `/Root/t1` WHERE a = 2 ORDER BY a, s;", true, R"([[2;"a"]])", {}, "/Root/t1"},
            {"SELECT a, s FROM `/Root/t1` WHERE a > 1 AND a <= 3 ORDER BY a, s;", true, R"([[2;"a"];[3;"c"]])", {}, "/Root/t1"},
            {"SELECT a, s FROM `/Root/t1` WHERE a >= 3 ORDER BY a, s;", true, R"([[3;"c"];[4;"d"]])", {}, "/Root/t1"},
            // The read is redirected to the covering index, whose key starts with v.
            {"SELECT a, s FROM `/Root/t1` WHERE v = 20 ORDER BY a, s;", true, R"([[1;"b"];[2;"a"]])", {}, "/Root/t1/idx_v/indexImplTable"},
            {"SELECT k, v FROM `/Root/t2` WHERE k IS NULL;", true, R"([[#;[100]]])", {}, "/Root/t2"},
            {"DECLARE $p AS Int64; SELECT a, s FROM `/Root/t1` WHERE a = $p ORDER BY a, s;", true, R"([[3;"c"]])", 3, "/Root/t1"},
            // The parameter has to be converted to the key type, which needs computing.
            {"DECLARE $p AS Int32; SELECT a, s FROM `/Root/t1` WHERE a = $p ORDER BY a, s;", false, R"([[3;"c"]])", 3, "/Root/t1"},
            // Several ranges are not a single literal range.
            {"SELECT a, s FROM `/Root/t1` WHERE a IN (1, 3) ORDER BY a, s;", false, R"([[1;"a"];[1;"b"];[3;"c"]])", {}, "/Root/t1"},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            NYdb::TParamsBuilder paramsBuilder;
            if (testCase.Param) {
                if (testCase.Query.Contains("AS Int32")) {
                    paramsBuilder.AddParam("$p").Int32(*testCase.Param).Build();
                } else {
                    paramsBuilder.AddParam("$p").Int64(*testCase.Param).Build();
                }
            }
            const auto params = paramsBuilder.Build();

            auto explained = querySession.ExecuteQuery(testCase.Query, NYdb::NQuery::TTxControl::NoTx(), params,
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explained.IsSuccess(), testCase.Query << ": " << explained.GetIssues().ToString());
            const TString ast{*explained.GetStats()->GetAst()};
            Cerr << "EXAMPLE-AST-BEGIN " << testCase.Query << "\n" << ast << "\nEXAMPLE-AST-END" << Endl;  // TEMPORARY

            UNIT_ASSERT_C(ast.Contains(TStringBuilder() << "'\"" << testCase.ReadTable << "\""),
                          testCase.Query << ": expected a read of " << testCase.ReadTable << ", ast:\n" << ast);
            size_t transactions = 0;
            for (size_t pos = ast.find("(KqpPhysicalTx "); pos != TString::npos; pos = ast.find("(KqpPhysicalTx ", pos + 1)) {
                ++transactions;
            }
            const auto sourceBegin = ast.find("(KqpRowsSourceSettings ");
            UNIT_ASSERT_C(sourceBegin != TString::npos, testCase.Query << ": expected a read source, ast:\n" << ast);
            const TString source = ast.substr(sourceBegin, ast.find('\n', sourceBegin) - sourceBegin);
            const bool literalBounds = ast.Contains("KqlKeyInc") || ast.Contains("KqlKeyExc");
            if (testCase.Literal) {
                // The read source takes the bounds as is: no ranges are computed and there is no separate transaction.
                UNIT_ASSERT_C(literalBounds, testCase.Query << ": expected a literal range, ast:\n" << ast);
                UNIT_ASSERT_C(!source.Contains("kqp_physical_tx_param_binding"), testCase.Query << ": expected literal bounds in " << source);
                UNIT_ASSERT_C(!ast.Contains("RangeFinalize") && !ast.Contains("'\"compute\""),
                              testCase.Query << ": expected no computed ranges, ast:\n" << ast);
                UNIT_ASSERT_VALUES_EQUAL_C(transactions, 1, testCase.Query << ", ast:\n" << ast);
            } else {
                // A compute transaction runs the ranges program and passes its result to the read as a parameter.
                UNIT_ASSERT_C(!literalBounds, testCase.Query << ": expected no literal range, ast:\n" << ast);
                UNIT_ASSERT_C(ast.Contains("RangeFinalize") && ast.Contains("DqCnValue") && ast.Contains("'\"compute\""),
                              testCase.Query << ": expected ranges computed by a compute transaction, ast:\n" << ast);
                UNIT_ASSERT_C(source.Contains("%kqp_physical_tx_param_binding_"), testCase.Query << ": expected the computed ranges in " << source);
                UNIT_ASSERT_VALUES_EQUAL_C(transactions, 2, testCase.Query << ", ast:\n" << ast);
            }

            auto result = querySession.ExecuteQuery(testCase.Query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), testCase.Query << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), testCase.Result, testCase.Query);
        }
    }

    // The bounds of a literal range can be parameters of the key column types. The read source takes the parameters
    // as is and the executer reads their values when the query runs, so one plan serves any values and needs no
    // separate transaction to compute the ranges.
    Y_UNIT_TEST(LiteralRangeWithParameters) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                s String NOT NULL,
                v Int64,
                PRIMARY KEY (a, s)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, s, v] : TVector<std::tuple<i64, TString, i64>>{
                     {1, "a", 10}, {1, "b", 20}, {2, "a", 20}, {3, "c", 30}, {4, "d", 40}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").Int64(a)
                    .AddMember("s").String(s)
                    .AddMember("v").OptionalInt64(v)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto checkLiteralRange = [&](const TString& query, const TVector<TString>& bounds) {
            auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explained.IsSuccess(), query << ": " << explained.GetIssues().ToString());
            const TString ast{*explained.GetStats()->GetAst()};

            // The physical AST binds parameters to generated variables. Resolve their
            // names instead of depending on the printer's variable numbering.
            THashMap<TString, TString> parameterAliases;
            TStringBuf rest = ast;
            for (TStringBuf line; rest.NextTok('\n', line);) {
                if (!line.StartsWith("(let $") || !line.EndsWith(")")) {
                    continue;
                }
                const size_t nameEnd = line.find(' ', 5);
                const TStringBuf value = line.substr(nameEnd + 1, line.size() - nameEnd - 2);
                if (value.StartsWith("$") && value.find(' ') == TStringBuf::npos) {
                    parameterAliases[TString(value)] = TString(line.substr(5, nameEnd - 5));
                }
            }
            for (const auto& bound : bounds) {
                TString expectedBound;
                TStringBuf remaining = bound;
                for (TStringBuf token; remaining.NextTok(' ', token);) {
                    if (!expectedBound.empty()) {
                        expectedBound += ' ';
                    }
                    const bool closesBound = token.EndsWith(")");
                    const TString parameter(closesBound ? token.substr(0, token.size() - 1) : token);
                    const auto alias = parameterAliases.find(parameter);
                    expectedBound += alias == parameterAliases.end() ? parameter : alias->second;
                    if (closesBound) {
                        expectedBound += ')';
                    }
                }
                UNIT_ASSERT_C(ast.Contains(expectedBound), query << ": expected the bound " << expectedBound << ", ast:\n" << ast);
            }
            UNIT_ASSERT_C(!ast.Contains("RangeFinalize") && !ast.Contains("kqp_physical_tx_param_binding") && !ast.Contains("'\"compute\""),
                          query << ": expected no computed ranges, ast:\n" << ast);
            size_t transactions = 0;
            for (size_t pos = ast.find("(KqpPhysicalTx "); pos != TString::npos; pos = ast.find("(KqpPhysicalTx ", pos + 1)) {
                ++transactions;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(transactions, 1, query << ", ast:\n" << ast);
        };

        // A point on the whole key.
        const TString pointQuery = R"(
            DECLARE $a AS Int64;
            DECLARE $s AS String;
            SELECT a, s, v FROM `/Root/t1` WHERE a = $a AND s = $s;
        )";
        checkLiteralRange(pointQuery, {"(KqlKeyInc $a $s)"});
        for (const auto& [a, s, expected] : TVector<std::tuple<i64, TString, TString>>{
                 {1, "b", R"([[1;"b";[20]]])"}, {3, "c", R"([[3;"c";[30]]])"}, {3, "x", "[]"}, {9, "a", "[]"}}) {
            const auto params = NYdb::TParamsBuilder().AddParam("$a").Int64(a).Build().AddParam("$s").String(s).Build().Build();
            auto result = querySession.ExecuteQuery(pointQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, "a = " << a << ", s = " << s);
        }

        // A range between two parameters. The bounds are only known when the query runs, so the range can be empty.
        const TString rangeQuery = R"(
            DECLARE $lo AS Int64;
            DECLARE $hi AS Int64;
            SELECT a, s FROM `/Root/t1` WHERE a >= $lo AND a < $hi ORDER BY a, s;
        )";
        checkLiteralRange(rangeQuery, {"(KqlKeyInc $lo)", "(KqlKeyExc $hi)"});
        for (const auto& [lo, hi, expected] : TVector<std::tuple<i64, i64, TString>>{
                 {1, 3, R"([[1;"a"];[1;"b"];[2;"a"]])"}, {3, 10, R"([[3;"c"];[4;"d"]])"}, {2, 2, "[]"}, {4, 1, "[]"}}) {
            const auto params = NYdb::TParamsBuilder().AddParam("$lo").Int64(lo).Build().AddParam("$hi").Int64(hi).Build().Build();
            auto result = querySession.ExecuteQuery(rangeQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, "lo = " << lo << ", hi = " << hi);
        }
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

    Y_UNIT_TEST(RowReadStageRunsFiltersAndMaps) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Several shards, so that the read has several tasks, and each of them filters its own rows.
        const auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Int64 NOT NULL,
                PRIMARY KEY (a, b)
            ) WITH (PARTITION_AT_KEYS = (1, 2));

            CREATE TABLE `/Root/dim` (
                a Int64 NOT NULL,
                d Int64 NOT NULL,
                PRIMARY KEY (a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c] : std::vector<std::tuple<i64, i64, i64>>{{0, 9, 1}, {1, 1, 7}, {1, 2, 5}, {2, 1, 3}, {2, 2, 4}}) {
            rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").Int64(b).AddMember("c").Int64(c).EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        NYdb::TValueBuilder dimRows;
        dimRows.BeginList();
        for (const auto& [a, d] : std::vector<std::pair<i64, i64>>{{1, 100}, {2, 200}}) {
            dimRows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("d").Int64(d).EndStruct();
        }
        dimRows.EndList();
        const auto dimResult = db.BulkUpsert("/Root/dim", dimRows.Build()).GetValueSync();
        UNIT_ASSERT_C(dimResult.IsSuccess(), dimResult.GetIssues().ToString());

        struct TCase {
            TString Query;
            TString Expected;
            // Whether the stage that reads `/Root/t` filters its rows, as in the old optimizer.
            bool Filtered;
            // Whether the ranges of the read are computed in a separate transaction.
            bool ComputedRanges;
        };

        const std::vector<TCase> cases = {
            {"select a, b from `/Root/t` where c > 3", "[[1;1];[1;2];[2;2]]", true, false},
            // A map runs in the read stage too.
            {"select a, b, c * 10 as d from `/Root/t` where c > 3", "[[1;1;70];[1;2;50];[2;2;40]]", true, false},
            {"select a + b as s from `/Root/t`", "[[9];[2];[3];[3];[4]]", false, false},
            // A filter on a non-key column over a literal key range and over computed ones.
            {"select a, b from `/Root/t` where a = 1 and c > 5", "[[1;1]]", true, false},
            {"select a, b from `/Root/t` where a in (0, 2) and c > 3", "[[2;2]]", true, true},
            // The filtered read is the input of an aggregate and of a join.
            {"select count(*) from `/Root/t` where c > 3", "[[3u]]", true, false},
            {"select t.a, t.b, d.d from `/Root/t` as t join `/Root/dim` as d on t.a = d.a where t.c > 3",
                "[[1;1;100];[1;2;100];[2;2;200]]", true, false},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; " + testCase.Query + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString plan{*explain.GetStats()->GetPlan()};
            NJson::TJsonValue planJson;
            UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
            const auto operators = GetReadStageOperators(planJson.GetMapSafe().at("Plan"), "/Root/t");
            UNIT_ASSERT_C(!operators.empty(), query << ": no stage reads /Root/t, plan: " << plan);
            const bool filtered = std::find(operators.begin(), operators.end(), "Filter") != operators.end();
            UNIT_ASSERT_VALUES_EQUAL_C(filtered, testCase.Filtered, query << ", plan: " << plan);
            const TString ast{*explain.GetStats()->GetAst()};
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("RangeFinalize"), testCase.ComputedRanges, query << ", ast: " << ast);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            CompareYsonUnordered(testCase.Expected, FormatResultSetYson(result.GetResultSet(0)), query);
        }

        // A system view is read by a row source as well.
        const TString sysViewQuery = R"(PRAGMA YqlSelect = 'force'; select count(*) from `/Root/.sys/nodes` where Host = "::1"u;)";
        auto sysViewResult = querySession.ExecuteQuery(sysViewQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(sysViewResult.IsSuccess(), sysViewResult.GetIssues().ToString());
        CompareYson("[[1u]]", FormatResultSetYson(sysViewResult.GetResultSet(0)));
    }

    Y_UNIT_TEST(SortByKeyOverFilteredRowRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Several shards, so that the merge of the read tasks decides the order of the result.
        const auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Int64 NOT NULL,
                PRIMARY KEY (a, b)
            ) WITH (PARTITION_AT_KEYS = (1, 2));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c] : std::vector<std::tuple<i64, i64, i64>>{{0, 9, 1}, {1, 1, 7}, {1, 2, 5}, {2, 1, 3}, {2, 2, 4}}) {
            rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").Int64(b).AddMember("c").Int64(c).EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        struct TCase {
            TString Query;
            TString Expected;
            // Whether the read returns its rows in the key order, so that the plan has no sort.
            bool SortedRead;
            // Whether the read takes the limit of the sort.
            bool ReadLimit;
            // Whether the read returns its rows in the reverse key order.
            bool Reverse = false;
        };

        const std::vector<TCase> cases = {
            // A filter drops rows, so the read takes no limit, and the limit of the result stops the read.
            {"select a, b from `/Root/t` where c > 3 order by a, b limit 2", "[[1;1];[1;2]]", true, false},
            {"select a, b from `/Root/t` where c > 3 order by a, b", "[[1;1];[1;2];[2;2]]", true, false},
            {"select a, b, c * 10 as d from `/Root/t` where c > 3 order by a, b limit 2", "[[1;1;70];[1;2;50]]", true, false},
            // Without a filter, each task of the read takes the first rows itself.
            {"select a, b from `/Root/t` order by a, b limit 2", "[[0;9];[1;1]]", true, true},
            // A map can rename the sort keys, and the sort keys can skip the point prefix of the key range.
            {"select a as x, b as y from `/Root/t` where c > 3 order by x, y limit 2", "[[1;1];[1;2]]", true, false},
            {"select a, b from `/Root/t` where a = 1 and c > 5 order by b limit 1", "[[1;1]]", true, false},
            // As in the old optimizer, a descending sort by the key is a read in the reverse key order.
            {"select a, b from `/Root/t` order by a desc, b desc limit 2", "[[2;2];[2;1]]", true, true, true},
            {"select a, b from `/Root/t` where c > 3 order by a desc, b desc limit 2", "[[2;2];[1;2]]", true, false, true},
            {"select a, b from `/Root/t` order by a desc, b desc", "[[2;2];[2;1];[1;2];[1;1];[0;9]]", true, false, true},
            {"select a, b from `/Root/t` where a = 1 order by b desc limit 1", "[[1;2]]", true, true, true},
            // The sort stays for keys out of the key order, for a computed key and for keys in both directions.
            {"select a, b from `/Root/t` where c > 3 order by c limit 2", "[[2;2];[1;2]]", false, false},
            {"select a + b as s from `/Root/t` where c > 3 order by s limit 2", "[[2];[3]]", false, false},
            {"select a, b from `/Root/t` order by a desc, b limit 2", "[[2;1];[2;2]]", false, false},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; " + testCase.Query + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString plan{*explain.GetStats()->GetPlan()};
            NJson::TJsonValue planJson;
            UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
            const auto sorts = CountPlanNodesByKv(planJson, "Name", "Sort") + CountPlanNodesByKv(planJson, "Name", "TopSort");
            UNIT_ASSERT_VALUES_EQUAL_C(sorts == 0, testCase.SortedRead, query << ", plan: " << plan);
            const TString ast{*explain.GetStats()->GetAst()};
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"Sorted\""), testCase.SortedRead, query << ", ast: " << ast);
            if (testCase.SortedRead) {
                UNIT_ASSERT_C(ast.Contains("DqCnMerge"), query << ": expected a merge connection out of the read stage, ast: " << ast);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"ItemsLimit\""), testCase.ReadLimit, query << ", ast: " << ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"Reverse\""), testCase.Reverse, query << ", ast: " << ast);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            CompareYson(testCase.Expected, FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    // NULL is smaller than any value, both in the key order of a datashard table and in the sorts of YQL. So a read in
    // the key order returns NULLs first, as an ascending sort does, and a read in the reverse order returns them last,
    // as a descending sort does.
    Y_UNIT_TEST(SortByNullableKeyOverRowRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Several shards, so that the merge of the read tasks decides the order of the result.
        const auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/n` (
                a Int64,
                b Int64,
                c Int64,
                PRIMARY KEY (a, b)
            ) WITH (PARTITION_AT_KEYS = (1));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c] : std::vector<std::tuple<std::optional<i64>, std::optional<i64>, i64>>{
                {std::nullopt, std::nullopt, 1}, {std::nullopt, 1, 2}, {0, std::nullopt, 3}, {0, 1, 4}, {1, 2, 5}, {2, std::nullopt, 6}}) {
            rows.AddListItem().BeginStruct()
                .AddMember("a").OptionalInt64(a)
                .AddMember("b").OptionalInt64(b)
                .AddMember("c").OptionalInt64(c)
            .EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/n", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        const std::vector<std::pair<TString, TString>> cases = {
            {"select a, b from `/Root/n` order by a desc, b desc", "[[[2];#];[[1];[2]];[[0];[1]];[[0];#];[#;[1]];[#;#]]"},
            {"select a, b from `/Root/n` order by a desc, b desc limit 3", "[[[2];#];[[1];[2]];[[0];[1]]]"},
            {"select a, b from `/Root/n` order by a, b", "[[#;#];[#;[1]];[[0];#];[[0];[1]];[[1];[2]];[[2];#]]"},
            {"select a, b from `/Root/n` order by a, b limit 3", "[[#;#];[#;[1]];[[0];#]]"},
            // The second key column after a point of the first one, which can be NULL as well.
            {"select b from `/Root/n` where a = 0 order by b desc", "[[[1]];[#]]"},
            {"select b from `/Root/n` where a is null order by b desc", "[[[1]];[#]]"},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& [sql, expected] : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; " + sql + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString ast{*explain.GetStats()->GetAst()};
            UNIT_ASSERT_C(ast.Contains("'\"Sorted\"") && !ast.Contains("TopSort") && !ast.Contains("WideSort") && !ast.Contains("(Sort "),
                query << ": expected a sorted read and no sort, ast: " << ast);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    // As in the old optimizer, a top-N sort over a row table read without ranges reads the table through an index
    // whose key order matches the sort and that covers the read, so the read stops at the limit.
    Y_UNIT_TEST(TopSortSelectsIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Several shards, so that the merge of the read tasks decides the order of the result.
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Int64,
                s String,
                PRIMARY KEY (a, b)
            ) WITH (PARTITION_AT_KEYS = (1, 2));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c, s] : std::vector<std::tuple<i64, i64, std::optional<i64>, std::string>>{
                {0, 9, 5, "e"}, {1, 1, std::nullopt, "d"}, {1, 2, 3, "c"}, {2, 1, 1, "b"}, {2, 2, 4, "a"}}) {
            rows.AddListItem().BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").Int64(b)
                .AddMember("c").OptionalInt64(c)
                .AddMember("s").OptionalString(s)
            .EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        // The indexes are built over the loaded rows.
        for (const auto* index : {"ALTER TABLE `/Root/t` ADD INDEX idx_c GLOBAL ON (c)",
                                  "ALTER TABLE `/Root/t` ADD INDEX idx_s GLOBAL ON (s) COVER (c)"}) {
            schemeResult = session.ExecuteSchemeQuery(index).GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), index << ": " << schemeResult.GetIssues().ToString());
        }

        struct TCase {
            TString Query;
            TString Expected;
            // The table the query reads, a path relative to /Root.
            TString Table;
            // Whether the read returns the rows in the order of the sort and takes its limit, so the plan has no sort.
            bool SortedRead;
            bool Reverse = false;
        };

        const std::vector<TCase> cases = {
            // NULLs come first in the key order of the index, as in an ascending sort.
            {"select a, b, c from `/Root/t` order by c limit 2", "[[1;1;#];[2;1;[1]]]", "t/idx_c/indexImplTable", true},
            {"select a, b, c from `/Root/t` order by c desc limit 2", "[[0;9;[5]];[2;2;[4]]]", "t/idx_c/indexImplTable", true, true},
            // The sort keys may continue with the key of the main table, which ends the key of the index.
            {"select c, a from `/Root/t` order by c, a limit 3", "[[#;1];[[1];2];[[3];1]]", "t/idx_c/indexImplTable", true},
            // A map can rename the sort keys.
            {"select a as x, c as y from `/Root/t` order by y limit 2", "[[1;#];[2;[1]]]", "t/idx_c/indexImplTable", true},
            // An index covers its own columns, those of the main table key and those it stores.
            {"select a, b, c from `/Root/t` order by s limit 2", "[[2;2;[4]];[2;1;[1]]]", "t/idx_s/indexImplTable", true},
            // The main table is read in the order of its key without an index.
            {"select a, b, c from `/Root/t` order by a, b limit 2", "[[0;9;[5]];[1;1;#]]", "t", true},
            // An index that does not cover the read is not chosen, it needs a lookup into the main table.
            {"select a, b, s from `/Root/t` order by c limit 2", "[[1;1;[\"d\"]];[2;1;[\"b\"]]]", "t", false},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; " + testCase.Query + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString ast{*explain.GetStats()->GetAst()};
            UNIT_ASSERT_C(ast.Contains("(KqpTable '\"/Root/" + testCase.Table + "\""), query << ": expected a read of " << testCase.Table << ", ast: " << ast);
            if (testCase.Table == "t") {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), query << ": expected no index read, ast: " << ast);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"Sorted\""), testCase.SortedRead, query << ", ast: " << ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("TopSort"), !testCase.SortedRead, query << ", ast: " << ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"ItemsLimit\""), testCase.SortedRead, query << ", ast: " << ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"Reverse\""), testCase.Reverse, query << ", ast: " << ast);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            CompareYson(testCase.Expected, FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ExistsDropsSubquerySort) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t` (
                a Int64 NOT NULL,
                c Int64 NOT NULL,
                PRIMARY KEY (a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, c] : std::vector<std::pair<i64, i64>>{{1, 3}, {2, 1}, {3, 2}}) {
            rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("c").Int64(c).EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        struct TCase {
            TString Predicate;
            TString Expected;
            // Whether the plan still sorts the rows of the subquery.
            bool Sorted;
        };

        // `c` is not a key, so only a sort can order the rows by it.
        const TString sorted = "select s.a from `/Root/t` as s order by s.c";
        const TString sortedEmpty = "select s.a from `/Root/t` as s where s.c > 5 order by s.c";
        const TString all = "[[1];[2];[3]]";
        const std::vector<TCase> cases = {
            // EXISTS only checks that a row exists, so any row is an answer.
            {"exists (" + sorted + ")", all, false},
            {"not exists (" + sorted + ")", "[]", false},
            {"exists (" + sortedEmpty + ")", "[]", false},
            // The same for an EXISTS that is not a conjunct of the filter.
            {"t.a = 1 or exists (" + sorted + ")", all, false},
            {"t.a = 1 or exists (" + sortedEmpty + ")", "[[1]]", false},
            // A limit written in the subquery depends on the order, whatever is above it.
            {"exists (" + sorted + " limit 1 offset 2)", all, true},
            {"exists (" + sorted + " limit 1 offset 3)", "[]", true},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; select t.a from `/Root/t` as t where " + testCase.Predicate + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString plan{*explain.GetStats()->GetPlan()};
            NJson::TJsonValue planJson;
            UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
            const bool planSorted = CountPlanNodesByKv(planJson, "Name", "Sort") + CountPlanNodesByKv(planJson, "Name", "TopSort") > 0;
            UNIT_ASSERT_VALUES_EQUAL_C(planSorted, testCase.Sorted, query << ", plan: " << plan);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            CompareYsonUnordered(testCase.Expected, FormatResultSetYson(result.GetResultSet(0)), query);
        }
    }

    // As in the old optimizer, a limit over a row table read goes to the read stage, so that each task stops at it.
    // An unsorted read takes a literal limit right over it as its own limit, and then reads one shard at a time.
    Y_UNIT_TEST(RowReadTakesLimit) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Several shards, so that the read has several of them to read one at a time.
        const auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Int64 NOT NULL,
                PRIMARY KEY (a, b)
            ) WITH (PARTITION_AT_KEYS = (1, 2));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c] : std::vector<std::tuple<i64, i64, i64>>{{0, 9, 1}, {1, 1, 7}, {1, 2, 5}, {2, 1, 3}, {2, 2, 4}}) {
            rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").Int64(b).AddMember("c").Int64(c).EndStruct();
        }
        rows.EndList();
        const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

        struct TCase {
            TString Query;
            // The row order is not defined, so only the number of rows is checked.
            size_t Rows;
            // The limit of the read, empty if the read has none: then the read stage takes the limit itself.
            TString ReadLimit;
        };

        const std::vector<TCase> cases = {
            {"select a, b from `/Root/t` limit 2", 2, "2"},
            // The read also returns the rows that the offset skips.
            {"select a, b from `/Root/t` limit 2 offset 1", 2, "3"},
            {"select a, b, c * 10 as d from `/Root/t` limit 2", 2, "2"},
            // Over a literal key range and over computed ones.
            {"select a, b from `/Root/t` where a = 1 limit 1", 1, "1"},
            {"select a, b from `/Root/t` where a in (0, 2) limit 1", 1, "1"},
            // A filter drops rows after the read, so only the read stage takes the limit.
            {"select a, b from `/Root/t` where c > 3 limit 2", 2, ""},
            // The limit of a subquery, below an aggregate.
            {"select count(*) from (select a from `/Root/t` limit 2)", 1, "2"},
        };

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const auto& testCase : cases) {
            const TString query = "PRAGMA YqlSelect = 'force'; " + testCase.Query + ";";
            auto explain = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), query << ": " << explain.GetIssues().ToString());
            const TString ast{*explain.GetStats()->GetAst()};
            if (testCase.ReadLimit.empty()) {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 0, query << ", ast: " << ast);
                const TString plan{*explain.GetStats()->GetPlan()};
                NJson::TJsonValue planJson;
                UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
                const auto operators = GetReadStageOperators(planJson.GetMapSafe().at("Plan"), "/Root/t");
                UNIT_ASSERT_C(std::find(operators.begin(), operators.end(), "Limit") != operators.end(),
                    query << ": expected a limit in the read stage, plan: " << plan);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 1, query << ", ast: " << ast);
                // The AST prints a literal with or without quotes.
                UNIT_ASSERT_C(ast.Contains(TStringBuilder() << "(Uint64 '\"" << testCase.ReadLimit << "\")")
                    || ast.Contains(TStringBuilder() << "(Uint64 '" << testCase.ReadLimit << ")"), query << ", ast: " << ast);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("'\"Sequential\""), !testCase.ReadLimit.empty(), query << ", ast: " << ast);

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSet(0).RowsCount(), testCase.Rows, query);
        }

        // A system view source ignores the limit of the read, so the read takes none.
        const TString sysViewQuery = "PRAGMA YqlSelect = 'force'; select Host from `/Root/.sys/nodes` limit 1;";
        auto sysViewExplain = querySession.ExecuteQuery(sysViewQuery, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(sysViewExplain.IsSuccess(), sysViewExplain.GetIssues().ToString());
        const TString sysViewAst{*sysViewExplain.GetStats()->GetAst()};
        UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(sysViewAst, "ItemsLimit"), 0, sysViewAst);
        auto sysViewResult = querySession.ExecuteQuery(sysViewQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(sysViewResult.IsSuccess(), sysViewResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(sysViewResult.GetResultSet(0).RowsCount(), 1);
    }

    // Probe: prints the final AST of row table queries for the old and the new optimizer, to compare them.
    Y_UNIT_TEST(RowTableAstOldVsNew) {
        struct TProbe {
            TString Query;
            // Rows are compared only when the query defines them, a LIMIT without ORDER BY does not.
            bool CompareRows = true;
        };
        const std::vector<TProbe> probes = {
            // Filters and ranges.
            {"select a, b from `/Root/t` where c > 3"},
            {"select a, b from `/Root/t` where a = 1 and c > 5"},
            {"select a, b from `/Root/t` where a in (0, 2) and c > 3"},
            {"select a, b from `/Root/t` where a > 1"},
            {"select a, b from `/Root/t` where a is not null"},
            {"select a, b from `/Root/t` where b = 1"},
            {"select c from `/Root/t` where a = 1 and b = 2"},
            {"select a, b from `/Root/t` where a = 1 or a = 3"},
            {"select c from `/Root/t` where (a = 1 and b = 2) or (a = 2 and b = 1)"},
            {"select a, b from `/Root/t` where a between 1 and 2 and b < 2"},
            {"select c from `/Root/t` where a = 1 and b in (1, 2)"},
            {"select a, b from `/Root/t` where a + 0 = 1"},
            {"select a, b from `/Root/t` where s like 'x%'"},
            {"select a, b from `/Root/t` where a = 1 and b > 1 and c is null"},
            // Sorts and limits.
            {"select a, b from `/Root/t` order by a, b limit 2"},
            {"select a, b from `/Root/t` order by a desc, b desc limit 2"},
            {"select a, b from `/Root/t` where a = 1 order by b limit 1"},
            {"select a, b from `/Root/t` where a = 1 order by b desc limit 1"},
            {"select a, b, c from `/Root/t` order by c limit 2"},
            {"select a, b from `/Root/t` limit 2", false},
            {"select a, b from `/Root/t` where c > 3 limit 2", false},
            {"select a, b from `/Root/t` order by a, b limit 2 offset 1"},
            {"select a, b from `/Root/t` order by a, b"},
            {"select a, b, c from `/Root/t` where a > 0 order by a, b limit 3"},
            {"select a, b from `/Root/t` where a > 0 order by a desc, b desc limit 2"},
            // Aggregates.
            {"select count(*) from `/Root/t`"},
            {"select a, count(*) from `/Root/t` group by a"},
            {"select a, sum(c) from `/Root/t` where a > 0 group by a"},
            {"select min(a), max(a) from `/Root/t`"},
            {"select distinct a from `/Root/t`"},
            {"select count(*) from `/Root/t` where a = 1"},
            // Joins, lookups and subqueries.
            {"select t.a, t.b, d.d from `/Root/t` as t join `/Root/dim` as d on t.c = d.a"},
            {"select t.a, t.b, d.d from `/Root/dim` as d join `/Root/t` as t on t.a = d.a"},
            {"select d.a, t.b from `/Root/dim` as d left join `/Root/t` as t on t.a = d.a"},
            {"select t.c, d.d from `/Root/dim` as d join `/Root/t` as t on t.a = d.a where t.b = 1"},
            {"select t.b, d.d from `/Root/dim` as d join `/Root/t` as t on t.a = d.a where t.c > 3"},
            {"select t.c from `/Root/dim` as d join `/Root/t` as t on t.a = d.a and t.b = d.d"},
            {"select t.a, t.b, d.d from `/Root/t` as t join `/Root/dim` as d on t.c = d.a where d.d > 100"},
            {"select a, b from `/Root/t` where a in (select a from `/Root/dim`)"},
            {"select t.a, t.b from `/Root/t` as t where exists (select 1 from `/Root/dim` as d where d.a = t.a)"},
            {"select t.a, t.b from `/Root/t` as t where not exists (select 1 from `/Root/dim` as d where d.a = t.a)"},
            {"select d.d, t.a, t.b from `/Root/dim` as d join `/Root/t` view idx_c as t on t.c = d.a"},
            // Secondary indexes.
            {"select a, b from `/Root/t` view idx_c where c = 4"},
            {"select a, b from `/Root/t` where c = 4"},
            {"select a, b, s from `/Root/t` view idx_c where c = 4"},
            {"select c from `/Root/t` view idx_c where c > 3 order by c limit 1"},
            {"select c from `/Root/t` view idx_s where s = 'x2'"},
            // Unions and nested selects.
            {"select a from `/Root/t` where a = 1 union all select a from `/Root/t` where a = 2"},
            {"select a from (select a, b from `/Root/t` where a > 0) where b = 1"},
            {"select a, b from `/Root/t` where a = (select max(a) from `/Root/dim`)"},
            // Full reads.
            {"select a, b, c, s from `/Root/t`"},
            {"select a, d from `/Root/dim`"},
        };

        struct TOutcome {
            TString Status;
            TString Ast;
            std::optional<TString> Rows;
        };
        std::vector<TOutcome> outcomes[2];
        for (const bool newRbo : {false, true}) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
            appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto schemeResult = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/t` (
                    a Int64,
                    b Int64,
                    c Int64,
                    s String,
                    PRIMARY KEY (a, b)
                ) WITH (PARTITION_AT_KEYS = (1, 2));

                CREATE TABLE `/Root/dim` (
                    a Int64,
                    d Int64,
                    PRIMARY KEY (a)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b, c, s] : std::vector<std::tuple<std::optional<i64>, std::optional<i64>, std::optional<i64>, std::string>>{
                    {std::nullopt, 1, 4, "x0"}, {0, std::nullopt, 9, "y"}, {0, 9, 1, "x1"}, {1, 1, 7, "x2"}, {1, 2, std::nullopt, "z"},
                    {2, 1, 3, "x3"}, {2, 2, 4, "x4"}, {3, 1, 5, "w"}, {3, 2, 6, "x5"}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt64(a)
                    .AddMember("b").OptionalInt64(b)
                    .AddMember("c").OptionalInt64(c)
                    .AddMember("s").OptionalString(s)
                .EndStruct();
            }
            rows.EndList();
            const auto rowsResult = db.BulkUpsert("/Root/t", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(rowsResult.IsSuccess(), rowsResult.GetIssues().ToString());

            NYdb::TValueBuilder dimRows;
            dimRows.BeginList();
            for (const auto& [a, d] : std::vector<std::pair<std::optional<i64>, i64>>{{std::nullopt, 0}, {1, 100}, {2, 200}, {4, 400}, {7, 700}}) {
                dimRows.AddListItem().BeginStruct().AddMember("a").OptionalInt64(a).AddMember("d").OptionalInt64(d).EndStruct();
            }
            dimRows.EndList();
            const auto dimResult = db.BulkUpsert("/Root/dim", dimRows.Build()).GetValueSync();
            UNIT_ASSERT_C(dimResult.IsSuccess(), dimResult.GetIssues().ToString());

            // The indexes are built over the loaded rows.
            for (const auto* index : {"ALTER TABLE `/Root/t` ADD INDEX idx_c GLOBAL ON (c)",
                                      "ALTER TABLE `/Root/t` ADD INDEX idx_s GLOBAL ON (s) COVER (c)"}) {
                schemeResult = session.ExecuteSchemeQuery(index).GetValueSync();
                UNIT_ASSERT_C(schemeResult.IsSuccess(), index << ": " << schemeResult.GetIssues().ToString());
            }

            auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            for (const auto& probe : probes) {
                auto result = querySession.ExecuteQuery(probe.Query + ";", NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)).ExtractValueSync();
                TOutcome outcome;
                if (!result.IsSuccess()) {
                    outcome.Status = "ERROR " + result.GetIssues().ToString();
                } else {
                    outcome.Status = "OK";
                    if (result.GetStats() && result.GetStats()->GetAst()) {
                        outcome.Ast = TString{*result.GetStats()->GetAst()};
                    }
                    if (!result.GetResultSets().empty()) {
                        outcome.Rows = FormatResultSetYson(result.GetResultSet(0));
                    }
                }
                outcomes[newRbo].push_back(std::move(outcome));
            }
        }

        for (size_t i = 0; i < probes.size(); ++i) {
            Cerr << "=== QUERY " << i << ": " << probes[i].Query << Endl;
            for (const bool newRbo : {false, true}) {
                const auto& outcome = outcomes[newRbo][i];
                Cerr << "=== " << (newRbo ? "NEW" : "OLD") << " STATUS " << i << ": " << outcome.Status << Endl;
                Cerr << "=== " << (newRbo ? "NEW" : "OLD") << " AST " << i << Endl << outcome.Ast << Endl;
            }
            Cerr << "=== END " << i << Endl;
        }
        for (size_t i = 0; i < probes.size(); ++i) {
            const auto& oldRows = outcomes[0][i].Rows;
            const auto& newRows = outcomes[1][i].Rows;
            if (probes[i].CompareRows && oldRows && newRows) {
                CompareYsonUnordered(*oldRows, *newRows, probes[i].Query);
            }
        }
    }

}

} // namespace NKikimr::NKqp
