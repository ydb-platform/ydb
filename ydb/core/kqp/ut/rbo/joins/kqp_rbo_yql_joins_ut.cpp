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

// Return the lowest plan node with an operator of the given name.
const NJson::TJsonValue* FindLowestPlanNodeByOperatorName(const NJson::TJsonValue& planNode, const TString& name) {
    if (!planNode.IsMap()) {
        return nullptr;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (const auto* node = FindLowestPlanNodeByOperatorName(child, name)) {
                return node;
            }
        }
    }

    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            const auto field = op.find("Name");
            if (field != op.end() && field->second.IsString() && field->second.GetStringSafe() == name) {
                return &planNode;
            }
        }
    }

    return nullptr;
}

void BasicHashJoinTest(bool useBlockHashJoin) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(useBlockHashJoin);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/t1` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);

        CREATE TABLE `/Root/t2` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);

        CREATE TABLE `/Root/t3` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);
    )").GetValueSync();

    NYdb::TValueBuilder rowsTablet1;
    rowsTablet1.BeginList();
    for (size_t i = 0; i < 4; ++i) {
        rowsTablet1.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet1.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTablet1.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rowsTablet2;
    rowsTablet2.BeginList();
    for (size_t i = 0; i < 3; ++i) {
        rowsTablet2.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet2.EndList();

    resultUpsert = db.BulkUpsert("/Root/t2", rowsTablet2.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rowsTablet3;
    rowsTablet3.BeginList();
    for (size_t i = 0; i < 5; ++i) {
        rowsTablet3.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet3.EndList();

    resultUpsert = db.BulkUpsert("/Root/t3", rowsTablet3.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    db = kikimr.GetTableClient();
    auto session2 = db.CreateSession().GetValueSync().GetSession();

    const std::string queryPrefix =
        R"(
            PRAGMA ydb.CostBasedOptimizationLevel='0';
            PRAGMA ydb.HashJoinMode='grace';
        )";

    std::vector<std::string> queries = {
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a order by t1.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a order by t1.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 where t1.a = t2.a and t2.a = t3.a order by t1.a, t2.a, t3.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 where t1.a = t2.a order by t1.a, t2.a, t3.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 order by t1.a, t2.a, t3.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t2.b > 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t2.b = 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t1.b = 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a inner join `/Root/t3` as t3 on t2.a = t3.a and t3.b = 2 order by t1.a, t2.a, t3.b;
        )",
        R"(
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a left join `/Root/t3` as t3 on t2.a = t3.a and t3.b = 2 order by t1.a, t2.a, t3.b;
        )",
    };

    std::vector<std::string> results = {
        R"([[0;0];[1;1];[2;2]])",
        R"([[0;[0]];[1;[1]];[2;[2]];[3;#]])",
        R"([[0;0;0];[1;1;1];[2;2;2]])",
        R"([[0;0;0];[0;0;1];[0;0;2];[0;0;3];[0;0;4];[1;1;0];[1;1;1];[1;1;2];[1;1;3];[1;1;4];[2;2;0];[2;2;1];[2;2;2];[2;2;3];[2;2;4]])",
        R"([[0;0;0];[0;0;1];[0;0;2];[0;0;3];[0;0;4];[0;1;0];[0;1;1];[0;1;2];[0;1;3];[0;1;4];[0;2;0];[0;2;1];[0;2;2];[0;2;3];[0;2;4];[1;0;0];[1;0;1];[1;0;2];[1;0;3];[1;0;4];[1;1;0];[1;1;1];[1;1;2];[1;1;3];[1;1;4];[1;2;0];[1;2;1];[1;2;2];[1;2;3];[1;2;4];[2;0;0];[2;0;1];[2;0;2];[2;0;3];[2;0;4];[2;1;0];[2;1;1];[2;1;2];[2;1;3];[2;1;4];[2;2;0];[2;2;1];[2;2;2];[2;2;3];[2;2;4];[3;0;0];[3;0;1];[3;0;2];[3;0;3];[3;0;4];[3;1;0];[3;1;1];[3;1;2];[3;1;3];[3;1;4];[3;2;0];[3;2;1];[3;2;2];[3;2;3];[3;2;4]])",
        R"([[0;#];[1;#];[2;[2]];[3;#]])",
        R"([[0;#];[1;[1]];[2;#];[3;#]])",
        R"([[1;1]])",
        R"([[1;1;1]])",
        R"([[0;[0];#];[1;[1];[1]];[2;[2];#];[3;#;#]])",
    };

    const std::string joinAlgo = useBlockHashJoin ? "BlockHashJoinCore" : "GraceJoinCore";
    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        const auto query = queryPrefix + "\n" + queries[i];

        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto ast = *result.GetStats()->GetAst();
        const auto isCrossJoin = query.find("AnsiImplicitCrossJoin") != std::string::npos;
        UNIT_ASSERT_C(isCrossJoin || ast.find(joinAlgo) != std::string::npos, TStringBuilder() << "Wrong join algo. Expected: " << joinAlgo);
        if (!isCrossJoin) {
            const auto plan = TString{*result.GetStats()->GetPlan()};
            const auto simplifiedPlan = GetSimplifiedPlan(plan);
            const TString explainJoinAlgo = useBlockHashJoin ? "BlockHash" : "Grace";
            const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinAlgo", explainJoinAlgo);
            UNIT_ASSERT_C(joinOp, plan);
            const TString explainJoinName = TStringBuilder() << "Join (" << explainJoinAlgo << ")";
            UNIT_ASSERT_C(GetStringField(*joinOp, "Name").Contains(explainJoinName), plan);
        }

        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    
    Y_UNIT_TEST(PushConstantConditionOnJoinKeyBothSides) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            select t1.a, t1.b
            from `/Root/t1` as t1
             join `/Root/t2` as t2 on t1.a = t2.a
             where t1.a == 1;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(!FindOperatorByStringFieldContaining(simplifiedPlan, "Name", "TableFullScan"), plan);
    }

    Y_UNIT_TEST_TWIN(PushFilterThroughAggregateOnLeftJoinInput, ColumnStore) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings(CreateExplainPlanTestAppConfig()).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto tableSession = db.CreateSession().GetValueSync().GetSession();
        const TString store = ColumnStore ? "WITH (STORE = COLUMN)" : "";
        auto schemeResult = tableSession.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                PRIMARY KEY (a)
            ) )" << store << R"(;

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                PRIMARY KEY (a)
            ) )" << store << R"(;
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder t1Rows;
        t1Rows.BeginList();
        for (i64 a = 1; a <= 12; ++a) {
            t1Rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").OptionalInt64(a % 4)
                .AddMember("c").OptionalInt64(a * 100)
                .EndStruct();
        }
        t1Rows.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", t1Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2Rows;
        t2Rows.BeginList();
        for (i64 a = 1; a <= 3; ++a) {
            t2Rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").OptionalInt64(a)
                .AddMember("c").OptionalInt64(a * 1000)
                .EndStruct();
        }
        t2Rows.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", t2Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto session = CreateQuerySession(kikimr);

        struct TTestCase {
            TString Query;
            TString PushedPredicate;
            TString RemainingPredicate;
            TString Result;
        };

        const TVector<TTestCase> testCases = {
            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT s.b, s.cnt, s.total, t2.c
                FROM (
                    SELECT b, COUNT(*) AS cnt, SUM(c) AS total
                    FROM `/Root/t1`
                    GROUP BY b
                ) AS s
                LEFT JOIN `/Root/t2` AS t2 ON s.b = t2.a
                WHERE s.b = 2;
            )", "b == 2", "", R"([[[2];3u;[1800];[2000]]])"},
            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT s.b, s.total, t2.c
                FROM (
                    SELECT b, SUM(c) AS total
                    FROM `/Root/t1`
                    GROUP BY b
                ) AS s
                LEFT JOIN `/Root/t2` AS t2 ON s.b = t2.a
                WHERE s.b = 2 AND s.total > 500;
            )", "b == 2", "> 500", R"([[[2];[1800];[2000]]])"},
            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT s.b, t2.c
                FROM (
                    SELECT DISTINCT b
                    FROM `/Root/t1`
                ) AS s
                LEFT JOIN `/Root/t2` AS t2 ON s.b = t2.a
                WHERE s.b = 2;
            )", "b == 2", "", R"([[[2];[2000]]])"},
            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT s.b, s.total, t2.c
                FROM (
                    SELECT b, SUM(c) AS total
                    FROM `/Root/t1`
                    GROUP BY b
                ) AS s
                LEFT JOIN `/Root/t2` AS t2 ON s.b = t2.a
                WHERE s.total = 1800;
            )", "", "== 1800", R"([[[2];[1800];[2000]]])"},
        };

        for (const auto& testCase : testCases) {
            auto plan = ExecuteExplain(session, testCase.Query);
            const auto simplifiedPlan = GetSimplifiedPlan(plan);
            const auto* aggregate = FindLowestPlanNodeByOperatorName(simplifiedPlan, "Aggregate");
            UNIT_ASSERT_C(aggregate, plan);
            if (!testCase.PushedPredicate.empty()) {
                UNIT_ASSERT_C(FindOperatorByStringFieldContaining(*aggregate, "Predicate", testCase.PushedPredicate),
                    testCase.Query << plan);
                UNIT_ASSERT_C(!FindOperatorByStringFieldContaining(simplifiedPlan, "Predicate", testCase.PushedPredicate, aggregate),
                    testCase.Query << plan);
            }
            if (!testCase.RemainingPredicate.empty()) {
                UNIT_ASSERT_C(!FindOperatorByStringFieldContaining(*aggregate, "Predicate", testCase.RemainingPredicate),
                    testCase.Query << plan);
                UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedPlan, "Predicate", testCase.RemainingPredicate, aggregate),
                    testCase.Query << plan);
            }

            auto result = session.ExecuteQuery(testCase.Query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, testCase.Query << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), testCase.Result, testCase.Query);
        }

        const TString scalarQuery = R"(
            PRAGMA YqlSelect = 'force';
            DECLARE $p AS Int64;
            SELECT s.cnt
            FROM (
                SELECT COUNT(*) AS cnt
                FROM `/Root/t1`
            ) AS s
            WHERE $p > 100;
        )";
        auto params = TParamsBuilder().AddParam("$p").Int64(0).Build().Build();
        auto result = session.ExecuteQuery(scalarQuery, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([])");
    }

    Y_UNIT_TEST(EliminateUnusedLeftJoin) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            select t1.a, t1.b
            from `/Root/t1` as t1
            left join `/Root/t2` as t2 on t1.a = t2.a;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(!FindOperatorByStringFieldContaining(simplifiedPlan, "Name", "Join"), plan);
    }

    Y_UNIT_TEST(CommonConjunctExtractionFeedsJoinKey) {
        TExplainPlanTestContext testContext;
        auto plan = ExecuteExplain(testContext.GetSession(), R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT count(*)
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE
                (t1.a = t2.a AND t1.b = 1)
                OR
                (t1.a = t2.a AND t2.b = 2);
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner");
        UNIT_ASSERT_C(joinOp, plan);
        const auto condition = GetStringField(*joinOp, "Condition");
        UNIT_ASSERT_C(condition == "t1.a = t2.a" || condition == "t2.a = t1.a", plan);
    }

    Y_UNIT_TEST(ComputedEqualityFeedsJoinKey) {
        TExplainPlanTestContext testContext;
        auto plan = ExecuteExplain(testContext.GetSession(), R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT count(*)
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a + 1 = t2.a;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner"), plan);
        UNIT_ASSERT_C(!FindOperatorByStringField(simplifiedPlan, "JoinKind", "Cross"), plan);
    }

    Y_UNIT_TEST_TWIN(BasicHashJoin, UseBlockHashJoin) {
        BasicHashJoinTest(UseBlockHashJoin);
    }

    Y_UNIT_TEST_QUAD(BlockHashJoinCrossWithJoinFilters, UseBlockHashJoin, UseBlockHashJoinForCross) {
        constexpr bool UseBlockCrossJoin = UseBlockHashJoin && UseBlockHashJoinForCross;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(UseBlockHashJoin);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoinForCross(UseBlockHashJoinForCross);
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        UNIT_ASSERT_C(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                PRIMARY KEY (a)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                PRIMARY KEY (a)
            ) WITH (STORE = COLUMN);
        )").GetValueSync().IsSuccess(), "create tables");

        NYdb::TValueBuilder leftRows;
        leftRows.BeginList();
        for (size_t i = 0; i < 2; ++i) {
            leftRows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        leftRows.EndList();
        UNIT_ASSERT(db.BulkUpsert("/Root/t1", leftRows.Build()).GetValueSync().IsSuccess());

        NYdb::TValueBuilder rightRows;
        rightRows.BeginList();
        for (size_t i = 0; i < 3; ++i) {
            rightRows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(10 + i)
                .EndStruct();
        }
        rightRows.EndList();
        UNIT_ASSERT(db.BulkUpsert("/Root/t2", rightRows.Build()).GetValueSync().IsSuccess());

        auto queryClient = kikimr.GetQueryClient();
        auto run = [&](const TString& query, const TString& expectedYson, ui32 expectedJoinFilters = 0) {
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            const TString fullQuery = TString(R"(
                PRAGMA ydb.CostBasedOptimizationLevel='0';
                PRAGMA ydb.HashJoinMode='grace';
            )") + query;

            auto explain = session.ExecuteQuery(
                fullQuery, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
            const auto ast = TString{*explain.GetStats()->GetAst()};
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("BlockHashJoinCore"), UseBlockCrossJoin, ast);

            const auto plan = TString{*explain.GetStats()->GetPlan()};
            const auto simplifiedPlan = GetSimplifiedPlan(plan);
            const auto* crossJoin = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Cross");
            UNIT_ASSERT_C(crossJoin, plan);
            const auto filters = crossJoin->GetMapSafe().find("Filters");
            if (UseBlockCrossJoin && expectedJoinFilters) {
                UNIT_ASSERT_C(filters != crossJoin->GetMapSafe().end() && filters->second.IsArray(), plan);
                UNIT_ASSERT_VALUES_EQUAL_C(filters->second.GetArraySafe().size(), expectedJoinFilters, plan);
            } else {
                UNIT_ASSERT_C(filters == crossJoin->GetMapSafe().end(), plan);
            }

            auto result = session.ExecuteQuery(
                fullQuery, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), expectedYson);
        };

        // 2 x 3 cartesian product
        run(R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a FROM `/Root/t1` AS t1, `/Root/t2` AS t2 ORDER BY t1.a, t2.a;
        )", R"([[0;0];[0;1];[0;2];[1;0];[1;1];[1;2]])");

        // Common filter.
        run(R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t2.b > t1.b + 9
            ORDER BY t1.a, t2.a;
        )", R"([[0;1];[0;2];[1;2]])", 1);

        // Left and right pushed, common is a join filter.
        run(R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.b > 0 AND t2.b > 10 AND t2.b > t1.b + 9
            ORDER BY t1.a, t2.a;
        )", R"([[0;1];[0;2];[1;2]])", 1);
    }

    Y_UNIT_TEST(InlineJoinFiltersAfterCBOChangesJoinTree) {
        for (const bool inlineJoinFiltersAfterCBO : {false, true}) {
            TExplainPlanTestContext testContext(inlineJoinFiltersAfterCBO);
            const auto plan = ExecuteExplain(testContext.GetSession(), R"(
                PRAGMA YqlSelect = 'force';
                PRAGMA ydb.CostBasedOptimizationLevel = '4';
                PRAGMA ydb.OptimizerHints = 'JoinOrder(l (m r))';

                SELECT count(*)
                FROM `/Root/t1` AS l
                INNER JOIN `/Root/t2` AS m
                    ON l.a = m.a AND l.b < m.c
                INNER JOIN `/Root/t1` AS r
                    ON m.a = r.a;
            )");

            const auto joinOrder = GetJoinOrder(plan).GetStringRobust();
            // Early inlining lets CBO apply the hinted order to all three
            // inputs. Late inlining keeps the first join as a boundary.
            const TString expectedJoinOrder = inlineJoinFiltersAfterCBO
                ? R"([["t1","t2"],"t1"])"
                : R"([["t1","t2"],"t1"])";
            UNIT_ASSERT_VALUES_EQUAL_C(joinOrder, expectedJoinOrder, plan);
        }
    }

    Y_UNIT_TEST(JoinFiltersBasic) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64	NOT NULL,
	            b Int64,
                primary key(a)
            ) WITH (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
	            b Int64,
                primary key(a)
            ) WITH (Store = Column);

            CREATE TABLE `/Root/t3` (
                a Int64	NOT NULL,
	            b Int64,
                primary key(a)
            ) WITH (Store = Column);
        )").GetValueSync();

        NYdb::TValueBuilder rowsTablet1;
        rowsTablet1.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTablet1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rowsTablet1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTablet1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTablet2;
        rowsTablet2.BeginList();
        for (size_t i = 0; i < 3; ++i) {
            rowsTablet2.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rowsTablet2.EndList();

        resultUpsert = db.BulkUpsert("/Root/t2", rowsTablet2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTablet3;
        rowsTablet3.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            rowsTablet3.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rowsTablet3.EndList();

        resultUpsert = db.BulkUpsert("/Root/t3", rowsTablet3.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t1.b >= t2.b  order by t1.a, t2.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a or t1.b = t2.b order by t1.a, t2.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t1.b >= t2.b order by t1.a, t2.a;
            )",
            // Filter pushed through join.
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdown = "false";
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t1.b > 1 order by t1.a, t2.a;
            )",
            // Filter pushed through join.
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdown = "false";
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t2.b > 2 order by t1.a, t2.a;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;[1];0;[1]];[1;[2];1;[2]];[2;[3];2;[3]]])",
            R"([[0;[1];0;[1]];[1;[2];1;[2]];[2;[3];2;[3]]])",
            R"([[0;[1];[0];[1]];[1;[2];[1];[2]];[2;[3];[2];[3]];[3;[4];#;#]])",
            R"([[1;[2];1;[2]];[2;[3];2;[3]]])",
            R"([[2;[3];2;[3]]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            Cout << "Processing query: " << i << "\n";
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(JoinFiltersOnClauseOuterJoins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto tableSession = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows1;
        rows1.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rows1.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(i + 1).EndStruct();
        }
        rows1.EndList();
        auto resultUpsert = db.BulkUpsert("/Root/t1", rows1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rows2;
        rows2.BeginList();
        for (size_t i = 0; i < 3; ++i) {
            rows2.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(i + 1).EndStruct();
        }
        rows2.EndList();
        resultUpsert = db.BulkUpsert("/Root/t2", rows2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        std::vector<std::pair<std::string, std::string>> cases = {
            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` AS t1
                LEFT JOIN `/Root/t2` AS t2 ON t1.a = t2.a AND t1.b > 2 ORDER BY t1.a, t2.a;
            )", R"([[0;[1];#;#];[1;[2];#;#];[2;[3];[2];[3]];[3;[4];#;#]])"},

            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b, t2.a, t2.b FROM `/Root/t1` AS t1
                LEFT JOIN `/Root/t2` AS t2 ON t1.a = t2.a AND t2.b > 2 ORDER BY t1.a, t2.a;
            )", R"([[0;[1];#;#];[1;[2];#;#];[2;[3];[2];[3]];[3;[4];#;#]])"},

            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b FROM `/Root/t1` AS t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` AS t2 WHERE t2.a = t1.a AND t1.b > 2) ORDER BY t1.a;
            )", R"([[2;[3]]])"},

            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b FROM `/Root/t1` AS t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` AS t2 WHERE t2.a = t1.a AND t2.b > 2) ORDER BY t1.a;
            )", R"([[2;[3]]])"},

            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b FROM `/Root/t1` AS t1
                WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` AS t2 WHERE t2.a = t1.a AND t1.b > 2) ORDER BY t1.a;
            )", R"([[0;[1]];[1;[2]];[3;[4]]])"},

            {R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t1.b FROM `/Root/t1` AS t1
                WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` AS t2 WHERE t2.a = t1.a AND t2.b > 2) ORDER BY t1.a;
            )", R"([[0;[1]];[1;[2]];[3;[4]]])"},
        };

        for (ui32 i = 0; i < cases.size(); ++i) {
            const auto& [query, expected] = cases[i];
            auto result = session.ExecuteQuery(TString(query), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "case " << i << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, "case " << i);
        }
    }

    Y_UNIT_TEST(LeftJoinInequalityOnClause) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoinForCross(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto tableSession = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows1;
        rows1.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rows1.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(i + 1).EndStruct();
        }
        rows1.EndList();
        auto resultUpsert = db.BulkUpsert("/Root/t1", rows1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rows2;
        rows2.BeginList();
        for (size_t i = 0; i < 3; ++i) {
            rows2.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(i + 1).EndStruct();
        }
        rows2.EndList();
        resultUpsert = db.BulkUpsert("/Root/t2", rows2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            SELECT t1.a, t1.b, t2.a, t2.b
            FROM `/Root/t1` AS t1
            LEFT JOIN `/Root/t2` AS t2 ON t1.a > t2.b
            ORDER BY t1.a, t2.a;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        auto explain = session.ExecuteQuery(
            query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());

        const auto plan = TString{*explain.GetStats()->GetPlan()};
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* crossJoin = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Cross");
        UNIT_ASSERT_C(crossJoin, plan);
        const auto filters = crossJoin->GetMapSafe().find("Filters");
        UNIT_ASSERT_C(filters != crossJoin->GetMapSafe().end() && filters->second.IsArray(), plan);
        UNIT_ASSERT_VALUES_EQUAL_C(filters->second.GetArraySafe().size(), 1, plan);

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResultSetYson(result.GetResultSet(0)),
            R"([[0;[1];#;#];[1;[2];#;#];[2;[3];[0];[1]];[3;[4];[0];[1]];[3;[4];[1];[2]]])"
        );
    }

    Y_UNIT_TEST(JoinFiltersAdvanced) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                PRIMARY KEY (id)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/t2` (
                id Int64 NOT NULL,
                value String NOT NULL,
                PRIMARY KEY (id)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder leftRows;
        leftRows.BeginList();
        for (i64 id : {1, 2, 3}) {
            leftRows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(id)
                .EndStruct();
        }
        leftRows.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", leftRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder rightRows;
        rightRows.BeginList();
        for (const auto& [id, value] : TVector<std::pair<i64, TString>>{{1, "axy"}, {2, "abc"}}) {
            rightRows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("value").String(value)
                .EndStruct();
        }
        rightRows.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", rightRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.HashJoinMode = 'grace';

            SELECT l.id, r.id
            FROM `/Root/t1` AS l
            LEFT JOIN `/Root/t2` AS r
                ON l.id = r.id AND r.value NOT LIKE '%x%y%'
            ORDER BY l.id;
        )";

        const auto explainResult = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
        UNIT_ASSERT_C(TString(explainResult.GetAst()).Contains("BlockHashJoinCore"), explainResult.GetAst());

        const auto queryResult = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(queryResult.GetResultSet(0)), R"([[1;#];[2;[2]];[3;#]])");
    }

    Y_UNIT_TEST(DontEliminateLeftJoinWhenNoPK) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), payload = f.Id(), b = f.Id(), rightPayload = f.Id();
        auto right = f.Read({b, rightPayload});
        right->Props.Metadata.emplace();
        auto join = MakeIntrusive<TOpJoin>(f.Read({a, payload}), std::move(right), f.Pos, "Left", TPairedIUs{{a, b}});
        auto* original = join.get();
        auto root = f.Root(std::move(join), {{a, "a"}, {payload, "payload"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(!TEliminateLeftJoinRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_C(root->GetInput().Get() == original, root->PlanToString(f.ExprCtx));
    }

    Y_UNIT_TEST(MapJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::tuple<std::string, ui32>> tables{{"/Root/t1", 6}, {"/Root/t2", 4}};
        for (const auto& [table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        const std::string queryPrefix = 
            R"(
                PRAGMA ydb.HashJoinMode='map';
                PRAGMA ydb.CostBasedOptimizationLevel='0';
            )";

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a, t2.c FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.c order by t1.a, t2.c;
            )",
            R"(
                SELECT t1.a, t2.c FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.c order by t1.a, t2.c;
            )",
            R"(
                SELECT t1.a FROM `/Root/t1` as t1 where t1.a in (select t2.c from `/Root/t2` as t2) order by t1.a;
            )",
        };

        const std::vector<std::string> results = {
            R"([[1;[1]];[2;[2]];[3;[3]];[4;[4]]])",
            R"([[0;#];[1;[1]];[2;[2]];[3;[3]];[4;[4]];[5;#]])",
            R"([[1];[2];[3];[4]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            const auto query = queryPrefix + "\n" + queries[i];
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("BlockHashJoin") != std::string::npos, TStringBuilder() << "Wrong join algo. Expected: " << "BlockHashJoin");

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(JoinOptionalKeys) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::tuple<std::string, ui32>> tables{{"/Root/t1", 6}, {"/Root/t2", 4}};
        for (const auto& [table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.c FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.c order by t1.a, t2.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.c FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.c = t2.a order by t1.c, t2.a;
            )",
        };

        std::vector<std::string> results = {
            R"([[1;[1]];[2;[2]];[3;[3]];[4;[4]]])",
            R"([[0;[2]];[1;[3]];[2;[4]]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(LeftJoins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t3` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t4` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 10}, {"/Root/t2", 8}, {"/Root/t3", 6}, {"/Root/t4", 4}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a order by t1.a, t2.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a left join `/Root/t3` as t3 on t2.a = t3.a order by t1.a, t2.a, t3.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a, t3.a, t4.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a left join `/Root/t3` as t3 on t2.a = t3.a
                                                                    left join `/Root/t4` as t4 on t3.a = t4.a and t4.c = t2.c and t1.c = t4.c order by t1.a, t2.a, t3.a, t4.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, count(t2.a) FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a group by t1.a order by t1.a;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;[0]];[1;[1]];[2;[2]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;#];[9;#]])",
            R"([[0;[0];[0]];[1;[1];[1]];[2;[2];[2]];[3;[3];[3]];[4;[4];[4]];[5;[5];[5]];[6;[6];#];[7;[7];#];[8;#;#];[9;#;#]])",
            R"([[0;[0];[0];[0]];[1;[1];[1];[1]];[2;[2];[2];[2]];[3;[3];[3];[3]];[4;[4];[4];#];[5;[5];[5];#];[6;[6];#;#];[7;[7];#;#];[8;#;#;#];[9;#;#;#]])",
            R"([[0;1u];[1;1u];[2;1u];[3;1u];[4;1u];[5;1u];[6;1u];[7;1u];[8;0u];[9;0u]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(RightJoins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 10}, {"/Root/t2", 8}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                SELECT t1.a, t2.a FROM `/Root/t2` as t2 right join `/Root/t1` as t1 on t1.a = t2.a order by t1.a, t2.a;
            )",
            /*
            ONLY | SEMI still unsupported in YQL Select
            R"(
                SELECT t2.a FROM `/Root/t2` as t2 right semi join `/Root/t1` as t1 on t1.a = t2.a order by t2.a;
            )",
            */ 
        };

        std::vector<std::string> results = {
            R"([[0;[0]];[1;[1]];[2;[2]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;#];[9;#]])",
            R"([[0;[0]];[1;[1]];[2;[2]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;#];[9;#]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(FullOuterJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 10}, {"/Root/t2", 8}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                SELECT t1.a, t2.a FROM 
                (SELECT * FROM `/Root/t1` as t1
                WHERE t1.a < 3) as t1 full outer join 
                (SELECT * FROM `/Root/t2` as t2
                WHERE t2.a > 2) as t2 on t1.a = t2.a order by t1.a, t2.a;
            )",
        };

        std::vector<std::string> results = {
            R"([[#;[3]];[#;[4]];[#;[5]];[#;[6]];[#;[7]];[[0];#];[[1];#];[[2];#]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(FilterPushdownThroughJoin) {
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
	            b Int64,
                c Int64,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                d Utf8,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        const std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` as t1 where t1.b > 1 and t1.a in (select t2.a from `/Root/t2` as t2);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` as t1 where t1.b > 1 and t1.a not in (select t2.a from `/Root/t2` as t2);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b == 1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where 1 == t2.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b != 1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b < 1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.d like '%abcd%';
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.d not like '%abcd%';
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where not(t2.b == 10);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.d like 'abcd%';
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.b FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.d like '%abcd';
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        const std::unordered_set<ui32> notRewriteLeftInnerQueries{0, 1};
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            Y_ENSURE(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Filter not pushed down.");
            Y_ENSURE(notRewriteLeftInnerQueries.contains(i) || (ast.find("Inner") != std::string::npos), TStringBuilder() << "Expected inner join");

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        const std::vector<std::string> notPushedQueries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b is null;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b == 10 or t2.b is null;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b is not null or t2.b is null;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where coalesce(t2.b, 0) == 10;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b == Just(10);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a where t2.b == Nothing(OptionalType(Int64));
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < notPushedQueries.size(); ++i) {
            const auto& query = notPushedQueries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            Y_ENSURE(ast.find("KqpOlapFilter") == std::string::npos, TStringBuilder() << "Filter pushed down.");
            Y_ENSURE(ast.find("Left") != std::string::npos, TStringBuilder() << "Expected left join");

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

}

} // namespace NKikimr::NKqp
