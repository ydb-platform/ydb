#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/aggregation.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <util/system/env.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <algorithm>
#include <ctime>
#include <regex>
#include <fstream>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NStat;

std::pair<ui32, ui32> GetNewRBOCompileCounters(TKikimrRunner& kikimr) {
    auto counters = TKqpCounters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    return {counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Success")->Val(),
            counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Failed")->Val()};
}

double TimeQuery(NKikimr::NKqp::TKikimrRunner& kikimr, TString query, int nIterations) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;
}

double TimeQuery(TString schema, TString query, int nIterations) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    session.ExecuteSchemeQuery(schema).GetValueSync();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;
}

TString GetStringField(const NJson::TJsonValue& node, const TString& fieldName) {
    const auto& map = node.GetMapSafe();
    const auto field = map.find(fieldName);
    UNIT_ASSERT_C(field != map.end() && field->second.IsString(), fieldName);
    return field->second.GetStringSafe();
}

bool GetBoolField(const NJson::TJsonValue& node, const TString& fieldName) {
    const auto& map = node.GetMapSafe();
    const auto field = map.find(fieldName);
    UNIT_ASSERT_C(field != map.end() && field->second.IsBoolean(), fieldName);
    return field->second.GetBoolean();
}

bool StringArrayFieldContains(const NJson::TJsonValue& node, const TString& fieldName, const TString& value) {
    const auto& map = node.GetMapSafe();
    const auto field = map.find(fieldName);
    UNIT_ASSERT_C(field != map.end() && field->second.IsArray(), fieldName);
    for (const auto& item : field->second.GetArraySafe()) {
        if (item.IsString() && item.GetStringSafe() == value) {
            return true;
        }
    }
    return false;
}

const NJson::TJsonValue* FindOperatorByStringField(const NJson::TJsonValue& planNode, const TString& fieldName, const TString& fieldValue) {
    if (!planNode.IsMap()) {
        return nullptr;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            const auto field = op.find(fieldName);
            if (field != op.end() && field->second.IsString() && field->second.GetStringSafe() == fieldValue) {
                return &opNode;
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (const auto* op = FindOperatorByStringField(child, fieldName, fieldValue)) {
                return op;
            }
        }
    }

    return nullptr;
}

const NJson::TJsonValue* FindOperatorByStringFieldContaining(const NJson::TJsonValue& planNode, const TString& fieldName, const TString& fieldValue) {
    if (!planNode.IsMap()) {
        return nullptr;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            const auto field = op.find(fieldName);
            if (field != op.end() && field->second.IsString() && field->second.GetStringSafe().Contains(fieldValue)) {
                return &opNode;
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (const auto* op = FindOperatorByStringFieldContaining(child, fieldName, fieldValue)) {
                return op;
            }
        }
    }

    return nullptr;
}

const NJson::TJsonValue* FindOperatorByNamePrefix(const NJson::TJsonValue& planNode, const TString& namePrefix) {
    if (!planNode.IsMap()) {
        return nullptr;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            const auto name = op.find("Name");
            if (name != op.end() && name->second.IsString() && name->second.GetStringSafe().StartsWith(namePrefix)) {
                return &opNode;
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (const auto* op = FindOperatorByNamePrefix(child, namePrefix)) {
                return op;
            }
        }
    }

    return nullptr;
}

const NJson::TJsonValue* FindConnectionNode(const NJson::TJsonValue& node, const TString& connectionName) {
    if (node.IsMap()) {
        const auto& map = node.GetMapSafe();
        const auto planNodeType = map.find("PlanNodeType");
        const auto nodeType = map.find("Node Type");
        if (planNodeType != map.end() && nodeType != map.end()
            && planNodeType->second.IsString() && nodeType->second.IsString()
            && planNodeType->second.GetStringSafe() == "Connection"
            && nodeType->second.GetStringSafe().StartsWith(connectionName))
        {
            return &node;
        }

        for (const auto& item : map) {
            if (const auto* connection = FindConnectionNode(item.second, connectionName)) {
                return connection;
            }
        }
    } else if (node.IsArray()) {
        for (const auto& value : node.GetArraySafe()) {
            if (const auto* connection = FindConnectionNode(value, connectionName)) {
                return connection;
            }
        }
    }

    return nullptr;
}

void PrintPlan(const TString& plan, bool analyzeMode) {
    NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
        NYdb::NConsoleClient::EDataFormat::PrettyTable,
        analyzeMode, Cout, /*maxWidth=*/0
    );
    queryPlanPrinter.Print(plan);
}

TString ExecuteExplain(NYdb::NQuery::TSession& session, const TString& query) {
    auto result = session.ExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
    ).ExtractValueSync();

    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    auto plan = TString{*result.GetStats()->GetPlan()};
    PrintPlan(plan, /*analyzeMode=*/false);
    return plan;
}

TString ExecuteExplainAnalyze(NYdb::NQuery::TSession& session, const TString& query) {
    auto result = session.ExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)
    ).ExtractValueSync();

    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    auto plan = TString{*result.GetStats()->GetPlan()};
    PrintPlan(plan, /*analyzeMode=*/true);
    return plan;
}

NJson::TJsonValue GetSimplifiedPlan(const TString& plan) {
    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);

    const auto& planMap = planJson.GetMapSafe();
    const auto simplifiedPlan = planMap.find("SimplifiedPlan");
    UNIT_ASSERT_C(simplifiedPlan != planMap.end(), plan);
    return simplifiedPlan->second;
}

TIntrusivePtr<TOpRead> MakeTestRead(const TVector<TInfoUnit>& outputIUs, TPositionHandle pos) {
    TVector<TString> columns;
    columns.reserve(outputIUs.size());
    for (const auto& iu : outputIUs) {
        columns.push_back(iu.GetColumnName());
    }

    return MakeIntrusive<TOpRead>(
        "",
        columns,
        outputIUs,
        NYql::EStorageType::RowStorage,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        std::nullopt,
        ESortDir::None,
        TPhysicalOpProps{},
        pos
    );
}

TMapElement MakeTestRename(const TString& to, const TString& from, TPositionHandle pos, NYql::TExprContext& exprCtx, TPlanProps& planProps) {
    return TMapElement(TInfoUnit(to), TInfoUnit(from), pos, &exprCtx, &planProps);
}

TMapElement MakeTestAppend(const TString& to, const TString& from, TPositionHandle pos, NYql::TExprContext& exprCtx, TPlanProps& planProps) {
    return TMapElement(TInfoUnit(to), MakeColumnAccess(TInfoUnit(from), pos, &exprCtx, &planProps), false);
}

TMapElement MakeTestConstantAppend(const TString& to, TPositionHandle pos, NYql::TExprContext& exprCtx) {
    return TMapElement(TInfoUnit(to), MakeConstant("Int32", "1", pos, &exprCtx), false);
}

void ComputeLogicalTestProps(TOpRoot& root) {
    root.ComputeParents();
    ComputePlanLiveness(root);
    ComputePlanNameConstraints(root);
    ComputePlanAliases(root);
}

struct TMapRuleTestContext {
    TMapRuleTestContext()
        : FuncRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<NYql::TKikimrConfiguration>())
        , QueryCtx(MakeIntrusive<NYql::TKikimrQueryContext>(FuncRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<NYql::TKikimrTablesData>())
        , UserRequestContext(MakeIntrusive<TUserRequestContext>())
        , KqpCtx("ut", Config, QueryCtx, Tables, UserRequestContext)
        , RboCtx(KqpCtx, ExprCtx, TypeCtx, TypeAnnTransformer, *FuncRegistry)
    {
    }

    NYql::TExprContext ExprCtx;
    NYql::TTypeAnnotationContext TypeCtx;
    NYql::TNullTransformer TypeAnnTransformer;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry;
    TIntrusivePtr<NYql::TKikimrConfiguration> Config;
    TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NOpt::TKqpOptimizeContext KqpCtx;
    TRBOContext RboCtx;
};

void AddMapAliasRulesForTest(TVector<std::unique_ptr<IRule>>& rules, bool pushAppendsUnderFilter) {
    rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
    rules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>());
    rules.emplace_back(std::make_unique<TRenameToAppendRule>());
    rules.emplace_back(std::make_unique<TPushAppendRule>(pushAppendsUnderFilter));
    rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
    rules.emplace_back(std::make_unique<TPushRenameRule>(pushAppendsUnderFilter));
}

TVector<std::unique_ptr<IRule>> MakeMapAliasCleanupRulesForTest() {
    TVector<std::unique_ptr<IRule>> rules;
    AddMapAliasRulesForTest(rules, /*pushAppendsUnderFilter*/ true);
    return rules;
}

TVector<std::unique_ptr<IRule>> MakeLogicalMapRulesForTest() {
    TVector<std::unique_ptr<IRule>> rules;
    AddMapAliasRulesForTest(rules, /*pushAppendsUnderFilter*/ false);
    rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
    return rules;
}

}

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT 1 as a, 2 as b;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void TestFilter(bool columnTables) {
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
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
	            name String,
                b Int64,
                primary key(id)
            )
        )";

        if (columnTables) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .AddMember("b").Int64(i)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
             R"(
                PRAGMA YqlSelect = 'force';
                SELECT id as id2 FROM `/Root/foo` WHERE name != '3_name' order by id;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT id as id2 FROM `/Root/foo` WHERE name = '3_name' order by id;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT id, name FROM `/Root/foo` WHERE name = '3_name' order by id;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT id, b FROM `/Root/foo` WHERE b not in [1, 2] order by b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT id, b FROM `/Root/foo` WHERE b in [1, 2] order by b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT * FROM `/Root/foo` WHERE name = '3_name' order by id;
            )",
        };

        std::vector<std::string> results = {
            R"([[0];[1];[2];[4];[5];[6];[7];[8];[9]])",
            R"([[3]])",
            R"([[3;["3_name"]]])",
            R"([[0;[0]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;[8]];[9;[9]]])",
            R"([[1;[1]];[2;[2]]])",
            R"([[3;["3_name"];[3]]])",
        };

        auto tableClient = kikimr.GetTableClient();
        auto session2 = tableClient.GetSession().GetValueSync().GetSession();

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST_TWIN(Filter, ColumnStore) {
        TestFilter(ColumnStore);
    }

    NKikimrConfig::TAppConfig CreateExplainPlanTestAppConfig() {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        return appConfig;
    }

    void CreateExplainPlanTestTables(TKikimrRunner& kikimr) {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64	NOT NULL,
                b Int64,
                c Int64,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b Int64,
                c Int64,
                primary key(a)
            ) WITH (STORE = column);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    NYdb::NQuery::TSession CreateQuerySession(TKikimrRunner& kikimr) {
        auto db = kikimr.GetQueryClient();
        auto res = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(res);
        return res.GetSession();
    }

    class TExplainPlanTestContext {
    public:
        TExplainPlanTestContext()
            : AppConfig(CreateExplainPlanTestAppConfig())
            , Kikimr(NKqp::TKikimrSettings(AppConfig).SetWithSampleTables(false))
            , Session(CreateSession())
        {
        }

        NYdb::NQuery::TSession& GetSession() {
            return Session;
        }

    private:
        NYdb::NQuery::TSession CreateSession() {
            CreateExplainPlanTestTables(Kikimr);
            return CreateQuerySession(Kikimr);
        }

    private:
        NKikimrConfig::TAppConfig AppConfig;
        TKikimrRunner Kikimr;
        NYdb::NQuery::TSession Session;
    };

    Y_UNIT_TEST(ExplainAnalyze) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplainAnalyze(session, R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.OptimizerHints = 'JoinType(t1 t2 Shuffle)';
            select count(*)
            from `/Root/t1` as t1
            inner join `/Root/t2` as t2 on t1.a = t2.b;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner");
        UNIT_ASSERT_C(joinOp, plan);

        UNIT_ASSERT_C(!GetStringField(*joinOp, "JoinAlgo").empty(), plan);
        const auto condition = GetStringField(*joinOp, "Condition");
        UNIT_ASSERT_C(condition.Contains("t1.a") && condition.Contains("t2.b") && condition.Contains(" = "), plan);
    }

    Y_UNIT_TEST(ExplainJoin) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.OptimizerHints = 'JoinType(t1 t2 Shuffle)';
            select count(*)
            from `/Root/t1` as t1
            inner join `/Root/t2` as t2 on t1.a = t2.b;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner");
        UNIT_ASSERT_C(joinOp, plan);

        UNIT_ASSERT_C(!GetStringField(*joinOp, "JoinAlgo").empty(), plan);
        const auto condition = GetStringField(*joinOp, "Condition");
        UNIT_ASSERT_C(condition.Contains("t1.a") && condition.Contains("t2.b") && condition.Contains(" = "), plan);
    }

    Y_UNIT_TEST(ExplainTopSort) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto sortPlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.a, t1.b
            from `/Root/t1` as t1
            order by t1.a desc, t1.b asc
            limit 5;
        )");
        const auto simplifiedSortPlan = GetSimplifiedPlan(sortPlan);
        const auto* topSortOp = FindOperatorByStringField(simplifiedSortPlan, "Name", "TopSort");
        UNIT_ASSERT_C(topSortOp, sortPlan);
        const auto topSortBy = GetStringField(*topSortOp, "TopSortBy");
        UNIT_ASSERT_C(topSortBy.Contains("a desc nulls first"), sortPlan);
        UNIT_ASSERT_C(topSortBy.Contains("b asc nulls first"), sortPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*topSortOp, "Limit"), "5", sortPlan);

        const auto* mergeConnection = FindConnectionNode(simplifiedSortPlan, "Merge");
        UNIT_ASSERT_C(mergeConnection, sortPlan);
        UNIT_ASSERT_C(GetStringField(*mergeConnection, "Node Type").StartsWith("Merge"), sortPlan);
        const auto mergeSortBy = GetStringField(*mergeConnection, "SortBy");
        UNIT_ASSERT_C(mergeSortBy.Contains("a desc nulls first"), sortPlan);
        UNIT_ASSERT_C(mergeSortBy.Contains("b asc nulls first"), sortPlan);
    }

    Y_UNIT_TEST(ExplainReadPushdown) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto pushedReadPlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.a, t1.b
            from `/Root/t1` as t1
            order by t1.a desc
            limit 5;
        )");
        const auto simplifiedPushedReadPlan = GetSimplifiedPlan(pushedReadPlan);
        const auto* readOp = FindOperatorByStringField(simplifiedPushedReadPlan, "Table", "t1");
        UNIT_ASSERT_C(readOp, pushedReadPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readOp, "Storage"), "Column", pushedReadPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readOp, "SortDirection"), "desc", pushedReadPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readOp, "Limit"), "5", pushedReadPlan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "a"), pushedReadPlan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "b"), pushedReadPlan);
    }

    Y_UNIT_TEST(ExplainAggregate) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto aggregatePlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.b, sum(t1.a) as total_price, count(t1.a) as cnt
            from `/Root/t1` as t1
            group by t1.b;
        )");
        const auto simplifiedAggregatePlan = GetSimplifiedPlan(aggregatePlan);
        const auto* aggregateOp = FindOperatorByStringFieldContaining(simplifiedAggregatePlan, "Aggregation", ": count(");
        UNIT_ASSERT_C(aggregateOp, aggregatePlan);
        const auto aggregation = GetStringField(*aggregateOp, "Aggregation");
        UNIT_ASSERT_C(aggregation.Contains(": sum("), aggregatePlan);
        UNIT_ASSERT_C(aggregation.Contains(": count("), aggregatePlan);
    }

    Y_UNIT_TEST(ExplainUnionAll) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto unionPlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.a from `/Root/t1` as t1
            union all
            select t2.a from `/Root/t2` as t2;
        )");
        const auto simplifiedUnionPlan = GetSimplifiedPlan(unionPlan);
        const auto* unionOp = FindOperatorByStringField(simplifiedUnionPlan, "Name", "UnionAll");
        UNIT_ASSERT_C(unionOp, unionPlan);
        UNIT_ASSERT_C(!GetBoolField(*unionOp, "Ordered"), unionPlan);
    }

    Y_UNIT_TEST(ExplainScalarSubquery) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto scalarSubplanPlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.a
            from `/Root/t1` as t1
            where t1.a = (select max(t2.a) from `/Root/t2` as t2);
        )");
        const auto simplifiedScalarSubplanPlan = GetSimplifiedPlan(scalarSubplanPlan);
        const auto* orderedUnionOp = FindOperatorByStringField(simplifiedScalarSubplanPlan, "Name", "UnionAll");
        UNIT_ASSERT_C(orderedUnionOp, scalarSubplanPlan);
        UNIT_ASSERT_C(GetBoolField(*orderedUnionOp, "Ordered"), scalarSubplanPlan);
    }

    Y_UNIT_TEST(ExplainStageConnections) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto connectionPlan = ExecuteExplainAnalyze(session, R"(
            PRAGMA YqlSelect = 'force';
            select count(*)
            from `/Root/t1` as t1
            inner join `/Root/t2` as t2 on t1.a = t2.b;
        )");

        const auto simplifiedConnectionPlan = GetSimplifiedPlan(connectionPlan);
        UNIT_ASSERT_C(FindConnectionNode(simplifiedConnectionPlan, "UnionAll"), connectionPlan);
        UNIT_ASSERT_C(FindConnectionNode(simplifiedConnectionPlan, "Broadcast"), connectionPlan);
        UNIT_ASSERT_C(!FindConnectionNode(simplifiedConnectionPlan, "Map"), connectionPlan);
    }

    Y_UNIT_TEST(Explain) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            TString t = R"(
                CREATE TABLE `/Root/t1` (
                    a Int64	NOT NULL,
                    b Int64,
                    c Int64,
                    primary key(a)
                ) WITH (STORE = column);

                CREATE TABLE `/Root/t2` (
                    a Int64	NOT NULL,
                    b Int64,
                    c Int64,
                    primary key(a)
                ) WITH (STORE = column);
            )";

            Y_ENSURE(session.ExecuteSchemeQuery(t).GetValueSync().IsSuccess());
        }

        {
            auto db = kikimr.GetQueryClient();
            auto res = db.GetSession().GetValueSync();
            NStatusHelpers::ThrowOnError(res);
            auto session = res.GetSession();

            auto result =
                session.ExecuteQuery(
                    R"(
                        PRAGMA YqlSelect = 'force';
                        select count(*)
                        from `/Root/t1` as t1
                        inner join `/Root/t2` as t2 on t1.a = t2.b;
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

    NKikimrConfig::TAppConfig CreateExpressionPrintingTestAppConfig() {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        return appConfig;
    }

    void CreateExpressionPrintingTestTables(TKikimrRunner& kikimr) {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
                b Int64,
                primary key(id)
            );

            CREATE TABLE `/Root/bar` (
                id Int64 NOT NULL,
                c Int64,
                primary key(id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    class TExpressionPrintingTestContext {
    public:
        TExpressionPrintingTestContext()
            : AppConfig(CreateExpressionPrintingTestAppConfig())
            , Kikimr(NKqp::TKikimrSettings(AppConfig).SetWithSampleTables(false))
            , Session(CreateSession())
        {
        }

        NYdb::NQuery::TSession& GetSession() {
            return Session;
        }

    private:
        NYdb::NQuery::TSession CreateSession() {
            CreateExpressionPrintingTestTables(Kikimr);
            return CreateQuerySession(Kikimr);
        }

    private:
        NKikimrConfig::TAppConfig AppConfig;
        TKikimrRunner Kikimr;
        NYdb::NQuery::TSession Session;
    };

    Y_UNIT_TEST(ExplainExpressionPrintingSimpleQuery) {
        TExpressionPrintingTestContext testContext;
        auto plan = ExecuteExplain(testContext.GetSession(), R"(
            PRAGMA YqlSelect = 'force';
            SELECT id + 1 AS next_id
            FROM `/Root/foo`
            WHERE b > 10
            LIMIT 3;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* mapOp = FindOperatorByNamePrefix(simplifiedPlan, "Map [");
        const auto* filterOp = FindOperatorByNamePrefix(simplifiedPlan, "Filter");
        const auto* limitOp = FindOperatorByNamePrefix(simplifiedPlan, "Limit");
        UNIT_ASSERT_C(mapOp, plan);
        UNIT_ASSERT_C(filterOp, plan);
        UNIT_ASSERT_C(limitOp, plan);

        const auto mapName = GetStringField(*mapOp, "Name");
        UNIT_ASSERT_C((mapName.Contains("next_id:") || mapName.Contains("next_id :=")) && mapName.Contains("id + 1"), plan);
        const auto predicate = GetStringField(*filterOp, "Predicate");
        UNIT_ASSERT_C(predicate.Contains("b > 10"), plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*limitOp, "Limit"), "3", plan);
    }

    Y_UNIT_TEST(ExplainExpressionPrintingJoinPredicate) {
        TExpressionPrintingTestContext testContext;
        auto plan = ExecuteExplain(testContext.GetSession(), R"(
            PRAGMA YqlSelect = 'force';
            SELECT count(*)
            FROM `/Root/foo` AS t1
            INNER JOIN `/Root/bar` AS t2
                ON t1.id = t2.id AND t1.b < t2.c;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner");
        UNIT_ASSERT_C(joinOp, plan);
        const auto condition = GetStringField(*joinOp, "Condition");
        UNIT_ASSERT_C(condition.Contains("id") && condition.Contains(" = "), plan);

        const auto* residualFilterOp = FindOperatorByNamePrefix(simplifiedPlan, "Filter");
        UNIT_ASSERT_C(residualFilterOp, plan);
        const auto residualPredicate = GetStringField(*residualFilterOp, "Predicate");
        UNIT_ASSERT_C(residualPredicate.Contains("b") && residualPredicate.Contains(" < ") && residualPredicate.Contains("c"), plan);
    }

    Y_UNIT_TEST(ExplainExpressionPrintingJoinFilters) {
        NYql::TExprContext exprCtx;
        TPlanProps planProps;
        const auto pos = NYql::TPositionHandle();
        const auto filter = MakeBinaryPredicate(
            "<",
            MakeColumnAccess(TInfoUnit("t1.b"), pos, &exprCtx, &planProps),
            MakeColumnAccess(TInfoUnit("t2.c"), pos, &exprCtx, &planProps)
        );
        TOpJoin join(
            MakeIntrusive<TOpEmptySource>(pos),
            MakeIntrusive<TOpEmptySource>(pos),
            pos,
            "Inner",
            {{TInfoUnit("t1.id"), TInfoUnit("t2.id")}},
            {filter}
        );

        const auto joinJson = join.ToJson(0);
        const auto condition = GetStringField(joinJson, "Condition");
        UNIT_ASSERT_C(condition.Contains("t1.id = t2.id"), condition);
        const auto& joinOpMap = joinJson.GetMapSafe();
        const auto filtersIt = joinOpMap.find("Filters");
        UNIT_ASSERT_C(filtersIt != joinOpMap.end() && filtersIt->second.IsArray(), joinJson.GetStringRobust());
        const auto& filters = filtersIt->second.GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL_C(filters.size(), 1, joinJson.GetStringRobust());
        UNIT_ASSERT_C(filters[0].IsString(), joinJson.GetStringRobust());
        const auto joinFilter = filters[0].GetStringSafe();
        UNIT_ASSERT_C(joinFilter.Contains("t1.b < t2.c"), joinFilter);
    }

    Y_UNIT_TEST(NameConstraintsPropagateThroughUnary) {
        NYql::TExprContext exprCtx;
        TPlanProps planProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(leftRead, pos, MakeColumnAccess(TInfoUnit("a"), pos, &exprCtx, &planProps));
        auto rightRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto join = MakeIntrusive<TOpJoin>(filter, rightRead, pos, "Inner", TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        TOpRoot root(join, pos, {"a"});

        ComputeLogicalTestProps(root);

        UNIT_ASSERT(root.PlanProps.NameConstraints.GetForbiddenOut(join.get(), 0).contains(TInfoUnit("a")));
        UNIT_ASSERT(root.PlanProps.NameConstraints.GetForbiddenOut(filter.get(), 0).contains(TInfoUnit("a")));
    }

    Y_UNIT_TEST(NameConstraintsMapRenameHidesForbiddenSource) {
        NYql::TExprContext exprCtx;
        TPlanProps planProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto leftMap = MakeIntrusive<TOpMap>(leftRead, pos, TVector<TMapElement>{MakeTestRename("b", "a", pos, exprCtx, planProps)});
        auto rightRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto join = MakeIntrusive<TOpJoin>(leftMap, rightRead, pos, "Inner", TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        TOpRoot root(join, pos, {"a"});

        ComputeLogicalTestProps(root);

        UNIT_ASSERT(root.PlanProps.NameConstraints.GetForbiddenOut(join.get(), 0).contains(TInfoUnit("a")));
        UNIT_ASSERT(!root.PlanProps.NameConstraints.GetForbiddenOut(leftMap.get(), 0).contains(TInfoUnit("a")));
    }

    Y_UNIT_TEST(NameConstraintsMapAppendDoesNotHideForbiddenSource) {
        NYql::TExprContext exprCtx;
        TPlanProps planProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto leftMap = MakeIntrusive<TOpMap>(leftRead, pos, TVector<TMapElement>{MakeTestAppend("b", "a", pos, exprCtx, planProps)});
        auto rightRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto join = MakeIntrusive<TOpJoin>(leftMap, rightRead, pos, "Inner", TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        TOpRoot root(join, pos, {"a"});

        ComputeLogicalTestProps(root);

        UNIT_ASSERT(root.PlanProps.NameConstraints.GetForbiddenOut(join.get(), 0).contains(TInfoUnit("a")));
        UNIT_ASSERT(root.PlanProps.NameConstraints.GetForbiddenOut(leftMap.get(), 0).contains(TInfoUnit("a")));
    }

    Y_UNIT_TEST(ReplaceAliasSubqueryDoesNotDuplicateVisibleColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/test` (
                D Utf8 NOT NULL,
                S Utf8 NOT NULL,
                V Utf8,
                PRIMARY KEY(D, S)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';

            SELECT t1.S AS result
            FROM (
                SELECT D, S, V
                FROM `/Root/test`
            ) AS t1
            WHERE t1.D = 'd'
            ORDER BY result;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CorrelatedScalarAggregateReuseDoesNotDuplicateVisibleColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/sales` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                v Double,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $totals = (
                SELECT
                    k AS group_key,
                    Sum(v) AS total
                FROM `/Root/sales`
                GROUP BY k
            );

            SELECT
                a.group_key
            FROM $totals AS a
            WHERE a.total > (
                SELECT
                    Avg(total)
                FROM $totals AS b
                WHERE a.group_key == b.group_key
            )
            ORDER BY a.group_key;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DistinctAllTypeMatchesLogicalOutputColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/dups` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                v Int64 NOT NULL,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(1)
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(100)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(2)
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(100)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(3)
            .AddMember("k").Int64(20)
            .AddMember("v").Int64(200)
            .EndStruct();
        rows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/dups", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';

            SELECT DISTINCT k AS k, v AS v
            FROM `/Root/dups`
            ORDER BY k, v;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings())
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[10;100];[20;200]])");
    }

    bool HasParam(const std::string& ast, const std::string& param) {
        auto txPos = ast.find("KqpPhysicalTx");
        if (txPos == std::string::npos) {
            return false;
        }

        return ast.find(param, txPos) != std::string::npos;
    }

     void TestParams(bool columnTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
	            name String,
                b Int64,
                primary key(id)
            )
        )";

        if (columnTables) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .AddMember("b").Int64(i)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                declare $param as String;
                SELECT id as id2 FROM `/Root/foo` WHERE name != $param order by id;
            )",
            R"(
                declare $param1 as String;
                SELECT id as id2 FROM `/Root/foo` WHERE name == $param1 order by id;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        std::vector<std::pair<std::string, std::string>> params = {{"$param", "0_name"}, {"$param1", "1_name"}};
        std::vector<std::string> results = {
              R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])",
              R"([[1]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(HasParam(ast, params[i].first), "Params not specified in tx param bindings");

            // clang-format off
            auto qParams = TParamsBuilder()
                .AddParam(params[i].first)
                    .String(params[i].second)
                .Build()
            .Build();
            // clang-format on
            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), qParams, NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST_TWIN(Params, ColumnStore) {
        TestParams(ColumnStore);
    }

    void TestMultiConsumer(bool columnTables) {
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

        std::vector<std::string> results = {
            R"([[1;1]])",
        };

        std::vector<std::string> queries = {
            R"(
                $subselect = (select a, b from `/Root/t1`);
                SELECT t1.a, t1.b FROM $subselect as t1 join $subselect as t2 on t1.a = t2.a WHERE t1.a == 1 and t1.b = 1 order by t1.a;
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

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(MultiConsumer, ColumnStore) {
        TestMultiConsumer(ColumnStore);
    }

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
            if (columnTables) {
                UNIT_ASSERT_C(ast.find("RangeFinalize") != TString::npos, "Ranges not pushed");
            }

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

            if (columnTables) {
                UNIT_ASSERT_C(ast.find("RangeFinalize") != TString::npos, "Ranges not pushed");
            }

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

    void TestConstantFolding(bool columnTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
	            name String,
                primary key(id)
            )
        )";

        if (columnTables) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        auto tableClient = kikimr.GetTableClient();
        auto session2 = tableClient.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT id as id2 FROM `/Root/foo` WHERE id = 15 - 14 and 18 - 17 = 1;
            )"
        };

        std::vector<std::string> results = {
            R"([[1]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(ConstantFolding, ColumnStore) {
        TestConstantFolding(ColumnStore);
    }

    void TestAggregation(bool columnStore) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString withColumnstore = R"(WITH (Store = COLUMN);)";
        TString t1 = R"(CREATE TABLE `/Root/t1` (
                a Int64	NOT NULL,
	            b Int64,
                c Int64,
                primary key(a)
            ))";
        TString t2 = R"(CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
	            b Int64,
                c Int64,
                d Decimal(14, 3),
                e Decimal(12, 2) NOT NULL,
                primary key(a)
            ))";
        if (columnStore) {
            t1 += withColumnstore;
            t2 += withColumnstore;
        } else {
            t1 += ";";
            t2 += ";";
        }

        Y_ENSURE(session.ExecuteSchemeQuery(t1).GetValueSync().IsSuccess());
        Y_ENSURE(session.ExecuteSchemeQuery(t2).GetValueSync().IsSuccess());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queriesOnEmptyColumns = {
            R"(
                select count(*) from `/Root/t1` as t1;
            )",
            // non optional, optional coumn
            R"(
                select count(t1.a), count(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select sum(t1.a), sum(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select min(t1.a), min(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select avg(t1.a), avg(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                SELECT stddev_samp(t1.a), stddev_samp(t1.b) from `/Root/t1` as t1;
            )",
        };
        std::vector<std::string> resultsEmptyColumns = {
            R"([[0u]])",
            R"([[0u;0u]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])"
        };

        for (ui32 i = 0; i < queriesOnEmptyColumns.size(); ++i) {
            const auto& query = queriesOnEmptyColumns[i];
            // Cout << query << Endl;
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), resultsEmptyColumns[i]);
        }

        NYdb::TValueBuilder rowsTableT1;
        rowsTableT1.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            rowsTableT1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i & 1 ? 1 : 2)
                .AddMember("c").Int64(2)
                .EndStruct();
        }
        rowsTableT1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableT2;
        rowsTableT2.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            rowsTableT2.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i & 1 ? 1 : 2)
                .AddMember("c").Int64(2)
                .AddMember("d").Decimal(TDecimalValue(ToString(i + 0.1), 14, 3))
                .AddMember("e").Decimal(TDecimalValue(ToString(i + 0.2), 12, 2))
                .EndStruct();
        }
        rowsTableT2.EndList();

        resultUpsert = db.BulkUpsert("/Root/t2", rowsTableT2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                select t2.b, sum(t2.d), sum(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
            )",
            R"(
                select t2.b, min(t2.d), max(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
            )",
            R"(
                select t2.b, count(t2.d), count(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
            )",
            R"(
                select t2.b, avg(t2.d), avg(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
            )",
            R"(
                select t1.b, sum(t1.c) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select t1.b, sum(t1.c) from `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a group by t1.b order by t1.b;
            )",
            R"(
                select t1.b, min(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select t1.b, max(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select t1.b, count(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select max(t1.b) as maxb, min(t1.a) from `/Root/t1` as t1 order by maxb;
            )",
            R"(
                select sum(t1.a) as suma from `/Root/t1` as t1 group by t1.b, t1.c order by suma;
            )",
            R"(
                select sum(t1.c), t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select max(t1.a) as maxa, min(t1.a), min(t1.b) as min_b from `/Root/t1` as t1 order by maxa;
            )",
            R"(
                select sum(t1.a + 1 + t1.c) as sumExpr0, sum(t1.c + 2) as sumExpr1 from `/Root/t1` as t1 group by t1.b order by sumExpr0;
            )",
            R"(
                select sum(distinct t1.b) as sum, t1.a from `/Root/t1` as t1 group by t1.a order by sum, t1.a;
            )",
            R"(
                select sum(t1.a) + 1, t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select count(distinct t1.a), t1.b from `/Root/t1` as t1 group by t1.b, t1.c order by t1.b;
            )",
            R"(
                select avg(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select avg(t1.a) as avgA, avg(t1.c) as avgC from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.b) as sumb from `/Root/t1` as t1 group by t1.b order by sumb;
            )",
            R"(
                select count(*), sum(t1.a) as result from `/Root/t1` as t1 order by result;
            )",
            R"(
                select count(*) as result from `/Root/t1` as t1 order by result;
            )",
            R"(
                select t1.b, count(*) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select count(*) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                select
                       sum(case when t1.b > 0
                            then 1
                            else 0 end) as count1,
                       sum(case when t1.b < 0
                            then 1
                            else 0 end) count2 from `/Root/t1` as t1 group by t1.b order by count1, count2;
            )",
            R"(
                 select max(t1.a), min(t1.a) from `/Root/t1` as t1;
            )",
            R"(
                 select max(t1.a), min(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                 select max(t1.a), min(t1.a) from `/Root/t1` as t1 group by t1.a order by t1.a;
            )",
            R"(
                 select max(t1.a) from `/Root/t1` as t1 group by t1.b, t1.a order by t1.a, t1.b;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select count(*) from `/Root/t1` as t1 group by t1.b + 1 order by t1.b + 1;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select sum(t1.c) as sum0, sum(t1.a + 3) as sum1 from `/Root/t1` as t1 group by t1.b + 1 order by sum0;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select sum(t1.c + 2) as sum0 from `/Root/t1` as t1 group by t1.b + t1.a order by sum0;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select
                       sum(case when t1.a > 0
                            then 1
                            else 0 end) +
                       sum(case when t1.a < 0
                            then 1
                            else 0 end) + 1, sum(t1.a) as r, t1.b + 2 as group_key from `/Root/t1` as t1 group by t1.b + 2 order by r;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select sum(t1.c) as sum0, t1.b + 1, t1.c + 2 from `/Root/t1` as t1 group by t1.b + 1, t1.c + 2 order by sum0;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select sum(t1.c) as sum0, t1.b, t1.c from `/Root/t1` as t1 group by t1.b, t1.c order by sum0;
            )",
            R"(
                pragma YqlSelect = "force";
                select distinct sum(t1.c) as sum_c, sum(t1.a) as sum_b, t1.b from `/Root/t1` as t1 group by t1.b order by sum_c;
            )",
            R"(
                pragma YqlSelect = "force";
                select distinct min(t1.a) as min_a, max(t1.a) as max_a, t1.b from `/Root/t1` as t1 group by t1.b order by min_a;
            )",
            R"(
                pragma YqlSelect = "force";
                select distinct (t1.a + t1.b) as res from `/Root/t1` as t1 order by res;
            )",
            R"(
                pragma YqlSelect = "force";
                select distinct (t1.b + 1) as res from `/Root/t1` as t1 order by res;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select distinct t1.a, t1.b from `/Root/t1` as t1 order by t1.a, t1.b;
            )",
            /*
            R"(
                select distinct t1.b from `/Root/t1` as t1 group by t1.a, t1.b;
            )",
            */
        };

        std::vector<std::string> results = {
                                            R"([[[1];["4.2"];"4.4"];[[2];["6.3"];"6.6"]])",
                                            R"([[[1];["1.1"];"3.2"];[[2];["0.1"];"4.2"]])",
                                            R"([[[1];2u;2u];[[2];3u;3u]])",
                                            R"([[[1];["2.1"];"2.2"];[[2];["2.1"];"2.2"]])",
                                            R"([[[1];[4]];[[2];[6]]])",
                                            R"([[[1];[4]];[[2];[6]]])",
                                            R"([[[1];1];[[2];0]])",
                                            R"([[[1];3];[[2];4]])",
                                            R"([[[1];2u];[[2];3u]])",
                                            R"([[[2];[0]]])",
                                            R"([[4];[6]])",
                                            R"([[[4];[1]];[[6];[2]]])",
                                            R"([[[4];[0];[1]]])",
                                            R"([[[10];[8]];[[15];[12]]])",
                                            R"([[[1];1];[[1];3];[[2];0];[[2];2];[[2];4]])",
                                            R"([[5;[1]];[7;[2]]])",
                                            R"([[2u;[1]];[3u;[2]]])",
                                            R"([[[1.6]]])",
                                            R"([[2.;[2.]];[2.;[2.]]])",
                                            R"([[[2]];[[6]]])",
                                            R"([[5u;[10]]])",
                                            R"([[5u]])",
                                            R"([[[1];2u];[[2];3u]])",
                                            R"([[2u];[3u]])",
                                            R"([[2;0];[3;0]])",
                                            R"([[[4];[0]]])",
                                            R"([[3;1];[4;0]])",
                                            R"([[0;0];[1;1];[2;2];[3;3];[4;4]])",
                                            R"([[0];[1];[2];[3];[4]])",
                                            R"([[2u];[3u]])",
                                            R"([[[4];10];[[6];15]])",
                                            R"([[[4]];[[8]];[[8]]])",
                                            R"([[3;4;[3]];[3;6;[4]]])",
                                            R"([[[4];[2];[4]];[[6];[3];[4]]])",
                                            R"([[[4];[1];[2]];[[6];[2];[2]]])",
                                            R"([[[4];4;[1]];[[6];6;[2]]])",
                                            R"([[0;4;[2]];[1;3;[1]]])",
                                            R"([[[2]];[[4]];[[6]]])",
                                            R"([[[2]];[[3]]])",
                                            R"([[0;[2]];[1;[1]];[2;[2]];[3;[1]];[4;[2]]])",
                                        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            //Cout << query << Endl;
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }

        NYdb::TValueBuilder rowsTableT1More;
        rowsTableT1More.BeginList();
        for (size_t i = 5; i < 20; ++i) {
            rowsTableT1More.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i & 1 ? 1 : 2)
                .AddMember("c").Int64(2)
                .EndStruct();
        }
        rowsTableT1More.EndList();

        auto resultUpsertMore = db.BulkUpsert("/Root/t1", rowsTableT1More.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsertMore.IsSuccess(), resultUpsertMore.GetIssues().ToString());

        queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT stddev_samp(t1.a) as res0, stddev_samp(t1.b) as res1 from `/Root/t1` as t1 order by res0, res1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT stddev_samp(t1.a) as res, t1.b from `/Root/t1` as t1 group by t1.b order by res;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT stddev_samp(t1.b) as res, t1.c from `/Root/t1` as t1 group by t1.c order by res;
            )",
        };

        results = {
            R"([[[5.916079783];[0.512989176]]])",
            R"([[6.055300708;[1]];[6.055300708;[2]]])",
            R"([[[0.512989176];[2]]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            //Cout << query << Endl;
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
       }
    }

    Y_UNIT_TEST_TWIN(Aggregation, ColumnStore) {
        TestAggregation(ColumnStore);
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

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(BasicHashJoin, UseBlockHashJoin) {
        BasicHashJoinTest(UseBlockHashJoin);
    }

    Y_UNIT_TEST(JoinFilters) {
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
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t1.b >= t2.b  order by t1.a, t2.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a or t1.b = t2.b order by t1.a, t2.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t1.b >= t2.b order by t1.a, t2.a;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;0];[1;1];[2;2]])",
            R"([[0;0];[1;1];[2;2]])",
            R"([[0;[0]];[1;[1]];[2;[2]];[3;#]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
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

    void Replace(std::string& s, const std::string& from, const std::string& to) {
        size_t pos = 0;
        while ((pos = s.find(from, pos)) != std::string::npos) {
            s.replace(pos, from.size(), to);
            pos += to.size();
        }
    }

    TString GetFullPath(const TString& prefix, const TString& filePath) {
        TString fullPath = SRC_(prefix + filePath);

        std::ifstream file(fullPath);

        if (!file.is_open()) {
            throw std::runtime_error("can't open + " + fullPath + " " + std::filesystem::current_path());
        }

        std::stringstream buffer;
        buffer << file.rdbuf();

        return buffer.str();
    }

    void CreateTablesFromPath(NYdb::NTable::TSession session, const TString& pathPrefix, const TString& schemaPath, bool useColumnStore) {
        std::string query = GetFullPath(pathPrefix, schemaPath);
        if (useColumnStore) {
            std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
            query = std::regex_replace(query, pattern, "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
        }

        auto res = session.ExecuteSchemeQuery(TString(query)).GetValueSync();
        res.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(res.IsSuccess());
    }

    void CreateTablesFromPath(NYdb::NTable::TSession session, const TString& schemaPath, bool useColumnStore) {
        CreateTablesFromPath(session, "../join/data/", schemaPath, useColumnStore);
    }

    void RunTPCHBenchmark(bool columnStore, std::vector<ui32> queries, bool newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(session, "schema/tpch.sql", columnStore);

        if (!queries.size()) {
            for (ui32 i = 1; i <= 22; ++i) {
                queries.push_back(i);
            }
        }

        std::string consts = NResource::Find(TStringBuilder() << "consts.yql");
        std::string tablePrefix = "/Root/";
        for (const auto qId : queries) {
            Cout << "Q " << qId << Endl;
            std::string q = NResource::Find(TStringBuilder() << "resfs/file/tpch/queries/yql/q" << qId << ".sql");
            Replace(q, "{path}", tablePrefix);
            Replace(q, "{% include 'header.sql.jinja' %}", R"(PRAGMA YqlSelect = 'force';)");
            std::regex pattern(R"(\{\{\s*([a-zA-Z0-9_]+)\s*\}\})");
            q = std::regex_replace(q, pattern, "`" + tablePrefix + "$1`");
            q = consts + "\n" + q;
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExplainDataQuery(q).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TPCH_YDB_PERF) {
       RunTPCHBenchmark(/*columnstore*/ true, {1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19}, /*new rbo*/ true);
       //RunTPCHBenchmark(/*columnstore*/ true, {1, 6, 14, 19}, /*new rbo*/ false);
    }

    void PrintStatus(std::unordered_map<ui32, bool>& queries, std::vector<TString>&& errors) {
        for (const auto &[id, result] : queries) {
            const TString status = result ? "SUCCESS" : "FAIL";
            Cout << "Q#" << id << " " << status << ";" << Endl;
            if (!result) {
                Cout << errors[id - 1] << Endl;
            }
        }
    }

    enum EBenchType { TPCH = 0, TPCDS };
    static constexpr std::array<const char*, 2> BenchmarkSchemaPathPrefix{R"(data/)", R"(data/)"};
    static constexpr std::array<const char*, 2> BenchmarkSchemaPath{R"(schema/tpch.sql)", R"(schema/tpcds.sql)"};
    static constexpr std::array<const char*, 2> BenchmarkQueryPath{R"(data/yql-tpch/q)", R"(data/yql-tpcds/q)"};
    static constexpr std::array<ui32, 2> BenchmarkQueryCount{22, 99};

    bool PlanHasJoin(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
            for (const auto& opNode : operators->second.GetArraySafe()) {
                const auto& op = opNode.GetMapSafe();
                if (auto opName = op.find("Name"); opName != op.end() && opName->second.GetStringSafe().Contains("Join")) {
                    return true;
                }
            }
        }

        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                if (PlanHasJoin(child)) {
                    return true;
                }
            }
        }

        return false;
    }

    void AssertNewRBOCboOptimizedAllTrees(const EBenchType type, const ui32 queryId, const TString& plan) {
        const TString benchmarkName = type == EBenchType::TPCH ? "TPCH" : "TPCDS";
        const TString context = TStringBuilder()
            << benchmarkName << " query " << queryId
            << "\nPlan:\n" << plan;

        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);
        const auto& planRootMap = planRoot.GetMapSafe();
        auto simplifiedPlanIt = planRootMap.find("SimplifiedPlan");
        UNIT_ASSERT_C(simplifiedPlanIt != planRootMap.end(), "Missing SimplifiedPlan. " << context);

        const auto& simplifiedPlan = simplifiedPlanIt->second;
        const auto& simplifiedPlanMap = simplifiedPlan.GetMapSafe();
        auto optimizerStatsIt = simplifiedPlanMap.find("OptimizerStats");
        UNIT_ASSERT_C(optimizerStatsIt != simplifiedPlanMap.end(), "Missing OptimizerStats. " << context);

        const auto& optimizerStats = optimizerStatsIt->second;
        const auto& optimizerStatsMap = optimizerStats.GetMapSafe();
        const auto getStat = [&](const TString& name) {
            auto it = optimizerStatsMap.find(name);
            UNIT_ASSERT_C(it != optimizerStatsMap.end(), "Missing optimizer stat " << name << ". " << context);
            return it->second.GetUIntegerSafe();
        };

        const ui64 total = getStat("CBOTreesTotal");
        const ui64 optimized = getStat("CBOTreesOptimized");
        const TString statsContext = TStringBuilder()
            << "Stats: " << optimizerStats.GetStringRobust()
            << "\n" << context;

        if (PlanHasJoin(simplifiedPlan)) {
            UNIT_ASSERT_C(total > 0, TStringBuilder() << "Expected CBO to see at least one tree. " << statsContext);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(optimized, total, statsContext);
    }

    void RunTPC_YqlBenchmark(const EBenchType type, const bool columnStore, std::set<ui32>&& queriesStatus, std::set<ui32>&& skipList, const bool newRbo,
                             const bool printStatus = false, const bool compareResults = false, const bool checkNewRBOCbo = false,
                             std::set<ui32>&& queriesWithoutCboCheck = {}) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(session, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);

        std::unordered_map<ui32, bool> queriesCurrentStatus;
        std::vector<bool> queriesExpectedStatus;
        std::vector<TString> errors;
        for (ui32 qId = 1, e = BenchmarkQueryCount[type]; qId <= e; ++qId) {
            if (skipList.contains(qId)) {
                queriesCurrentStatus.insert({qId, false});
                queriesExpectedStatus.push_back(false);
                errors.emplace_back("Skipped.");
                continue;
            }

            const auto expectedStatus = queriesStatus.empty() ? true : queriesStatus.contains(qId);
            queriesExpectedStatus.push_back(expectedStatus);
            TString q = GetFullPath(BenchmarkQueryPath[type], ToString(qId) + ".yql");
            const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
            const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
            const TString round = R"($round = ($x,$y) -> {return $x;};)";

            q = toDecimal + "\n" + toDecimalMax + "\n" + round + "\n" + q;

            Cerr << "Executing benchmark query " << qId << "\n";

            auto queryClient = kikimr.GetQueryClient();
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                              .ExtractValueSync();
            queriesCurrentStatus.insert({qId, result.IsSuccess()});
            errors.emplace_back(result.GetIssues().ToString());
            if (checkNewRBOCbo && result.IsSuccess() && !queriesWithoutCboCheck.contains(qId)) {
                UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan for query: " << qId);
                AssertNewRBOCboOptimizedAllTrees(type, qId, TString{*result.GetStats()->GetPlan()});
            }

        }

        if (printStatus) {
            PrintStatus(queriesCurrentStatus, std::move(errors));
        }

        if (compareResults) {
            for (ui32 i = 0; i < queriesExpectedStatus.size(); ++i) {
                auto status = queriesExpectedStatus[i];
                if (status) {
                    UNIT_ASSERT_C(queriesCurrentStatus[i + 1], "Expected success for query: " + ToString(i + 1));
                }
            }
        }
    }

    std::set<ui32> MakeTPC_YqlSingleQuerySkipList(const EBenchType type, const ui32 queryId) {
        std::set<ui32> skipList;
        for (ui32 qId = 1, e = BenchmarkQueryCount[type]; qId <= e; ++qId) {
            if (qId != queryId) {
                skipList.insert(qId);
            }
        }
        return skipList;
    }

    void RunTPCH_YqlSingleQueryTest(const ui32 queryId, const bool expectedSuccess = true) {
        std::set<ui32> expectedSuccessQueries;
        if (expectedSuccess) {
            expectedSuccessQueries.insert(queryId);
        } else {
            // An empty queriesStatus set means all non-skipped queries are expected to succeed.
            expectedSuccessQueries.insert(BenchmarkQueryCount[EBenchType::TPCH] + 1);
        }

        RunTPC_YqlBenchmark(EBenchType::TPCH, /*columnstore=*/true, std::move(expectedSuccessQueries),
                            MakeTPC_YqlSingleQuerySkipList(EBenchType::TPCH, queryId),
                            /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true);
    }

    void RunTPC_YqlTest(const EBenchType type, ui32 queryId, const bool columnStore, const bool newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        auto kikimrSettings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);

        kikimrSettings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::EPriority::PRI_TRACE);
        kikimrSettings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;

        TKikimrRunner kikimr(kikimrSettings);
        
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(session, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);

        {
            TString q = GetFullPath(BenchmarkQueryPath[type], ToString(queryId) + ".yql");
            const TString toDecimal =  R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
            const TString toDecimalMax =  R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
            const TString round = R"($round = ($x,$y) -> {return $x;};)";

            q = round + "\n" + toDecimal + "\n" + toDecimalMax + "\n" + q;

            auto queryClient = kikimr.GetQueryClient();
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                                .ExtractValueSync();
            Y_ENSURE(result.IsSuccess());
        }
    }

    NKikimrKqp::TKqpSetting MakeTPCHStatsSetting() {
        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(GetFullPath("../join/data/", "stats/tpch1000s.json"));
        return statsSetting;
    }

    TString LoadTPCHYqlQuery(ui32 queryId) {
        const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        return toDecimal + "\n" + toDecimalMax + "\n"
            + GetFullPath(BenchmarkQueryPath[EBenchType::TPCH], ToString(queryId) + ".yql");
    }

    Y_UNIT_TEST(CorrelatedScalarSubqueryCBO4KeepsOuterJoinKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/lineitem": {"n_rows": 4, "byte_size": 128},
            "/Root/part": {"n_rows": 2, "byte_size": 64}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/lineitem` (
                id Int64 NOT NULL,
                l_partkey Int64 NOT NULL,
                l_quantity Double,
                l_extendedprice Int64,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/part` (
                p_partkey Int64 NOT NULL,
                p_brand String,
                p_container String,
                PRIMARY KEY(p_partkey)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder lineitemRows;
        lineitemRows.BeginList();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(1)
            .AddMember("l_partkey").Int64(1)
            .AddMember("l_quantity").Double(10.0)
            .AddMember("l_extendedprice").Int64(100)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(2)
            .AddMember("l_partkey").Int64(1)
            .AddMember("l_quantity").Double(20.0)
            .AddMember("l_extendedprice").Int64(200)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(3)
            .AddMember("l_partkey").Int64(2)
            .AddMember("l_quantity").Double(100.0)
            .AddMember("l_extendedprice").Int64(1000)
            .EndStruct();
        lineitemRows.AddListItem().BeginStruct()
            .AddMember("id").Int64(4)
            .AddMember("l_partkey").Int64(2)
            .AddMember("l_quantity").Double(200.0)
            .AddMember("l_extendedprice").Int64(2000)
            .EndStruct();
        lineitemRows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/lineitem", lineitemRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder partRows;
        partRows.BeginList();
        for (const auto partKey : {1, 2}) {
            partRows.AddListItem().BeginStruct()
                .AddMember("p_partkey").Int64(partKey)
                .AddMember("p_brand").String("Brand#23")
                .AddMember("p_container").String("MED BOX")
                .EndStruct();
        }
        partRows.EndList();

        upsertResult = tableClient.BulkUpsert("/Root/part", partRows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT
                SUM(l_extendedprice) AS total
            FROM
                `/Root/lineitem` AS lineitem,
                `/Root/part` AS part
            WHERE
                p_partkey == l_partkey
                AND p_brand == 'Brand#23'
                AND p_container == 'MED BOX'
                AND l_quantity < (
                    SELECT
                        AVG(l_quantity)
                    FROM
                        `/Root/lineitem` AS lineitem
                    WHERE
                        l_partkey == p_partkey
                );
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explainResult = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();
        UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());

        const auto plan = TString{*explainResult.GetStats()->GetPlan()};
        UNIT_ASSERT_C(!plan.Contains("CrossJoin"), plan);

        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[[1100]]])");
    }

    Y_UNIT_TEST(TPCH_YQL_CBO4_ColumnLineageMapping) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({MakeTPCHStatsSetting()});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], /*useColumnStore*/ true);

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        for (const ui32 queryId : {2, 3, 10}) {
            auto result = querySession.ExecuteQuery(LoadTPCHYqlQuery(queryId),
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), "Expected TPCH q" << queryId << " explain to succeed: " << result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(MapAliasCleanupComplexQuery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], /*useColumnStore*/ true);

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $zero_i32 = cast(0 as Int32);
            $zero_i64 = cast(0 as Int64);
            $one_i64 = cast(1 as Int64);
            $two_i64 = cast(2 as Int64);

            $c0 = (
                SELECT
                    c.c_custkey AS c_k,
                    c.c_nationkey AS c_nkey,
                    c.c_custkey AS c_amount,
                    c.c_custkey + $one_i64 AS c_metric
                FROM `/Root/customer` AS c
                WHERE c.c_custkey > $zero_i64
                ORDER BY c.c_custkey
                LIMIT 1000
            );

            $s0 = (
                SELECT
                    s.s_suppkey AS s_k,
                    s.s_nationkey AS s_nkey,
                    s.s_suppkey AS s_amount,
                    s.s_suppkey + $two_i64 AS s_metric
                FROM `/Root/supplier` AS s
                WHERE EXISTS (
                    SELECT *
                    FROM `/Root/nation` AS n
                    WHERE n.n_nationkey == s.s_nationkey
                        AND n.n_name IS NOT NULL
                )
            );

            $inner_j = (
                SELECT
                    c_k AS k,
                    s_k AS supplier_key,
                    c_nkey AS nkey,
                    c_amount + s_amount AS amount,
                    c_metric + s_metric AS metric
                FROM $c0, $s0
                WHERE c_nkey == s_nkey
            );

            $left_j = (
                SELECT
                    k,
                    nkey,
                    amount,
                    metric + cast(coalesce(ps_availqty, $zero_i32) as Int64) AS metric2
                FROM $inner_j
                LEFT JOIN `/Root/partsupp` AS partsupp
                    ON supplier_key == partsupp.ps_suppkey
            );

            $agg = (
                SELECT
                    nkey AS k,
                    MAX(metric2 + amount) AS sort_metric
                FROM $left_j
                GROUP BY nkey
            );

            $unioned = (
                SELECT k, sort_metric
                FROM $agg

                UNION ALL

                SELECT
                    cast(n.n_nationkey as Int32?) AS k,
                    cast(n.n_nationkey as Int64) AS sort_metric
                FROM `/Root/nation` AS n
            );

            SELECT
                k AS result_key,
                k + $one_i64 AS result_amount
            FROM $unioned
            ORDER BY sort_metric DESC
            LIMIT 10;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner"), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinKind", "Left"), plan);
        UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedPlan, "Aggregation", ": max("), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "UnionAll"), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "TopSort") || FindOperatorByStringField(simplifiedPlan, "Name", "Sort"), plan);
        UNIT_ASSERT_C(plan.Contains("Join"), plan);
        UNIT_ASSERT_C(!plan.Contains("__kqp_rbo_ignore_arg_"), plan);
    }

    Y_UNIT_TEST(MapAliasCleanupSemanticRenameAndDeadSortKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], /*useColumnStore*/ true);

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $cte = (
                SELECT a1.id2 AS id2, a1.join_id AS join_id
                FROM (
                    SELECT c.c_custkey AS id2, c.c_nationkey AS join_id
                    FROM `/Root/customer` AS c
                ) AS a1
            );

            $joined = (
                SELECT X1.id2 AS left_id, X2.id2 AS right_id, X1.id2 + X2.id2 AS sort_key
                FROM
                   (
                       SELECT cte.id2 AS id2
                       FROM `/Root/supplier` AS supplier, $cte AS cte
                       WHERE supplier.s_nationkey == cte.join_id
                   ) AS X1,
                   (
                       SELECT cte.id2 AS id2
                       FROM `/Root/nation` AS nation, $cte AS cte
                       WHERE nation.n_nationkey == cte.join_id
                   ) AS X2
            );

            SELECT left_id, right_id
            FROM $joined
            ORDER BY sort_key
            LIMIT 10;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "TopSort") || FindOperatorByStringField(simplifiedPlan, "Name", "Sort"), plan);
        UNIT_ASSERT_C(plan.Contains("Join"), plan);
    }

    Y_UNIT_TEST(FocusedMapAliasRules) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto identityAndDeadMap = MakeIntrusive<TOpMap>(leftRead, pos, TVector<TMapElement>{
            MakeTestRename("a", "a", pos, testContext.ExprCtx, expressionProps),
            MakeTestAppend("dead_payload", "payload", pos, testContext.ExprCtx, expressionProps),
        });
        auto semanticRenameMap = MakeIntrusive<TOpMap>(identityAndDeadMap, pos, TVector<TMapElement>{
            MakeTestRename("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });

        auto rightRead = MakeTestRead({TInfoUnit("a"), TInfoUnit("right_payload")}, pos);
        auto join = MakeIntrusive<TOpJoin>(
            semanticRenameMap,
            rightRead,
            pos,
            "Inner",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("l_a"), TInfoUnit("a")}}
        );

        auto bothSidesExpression = MakeBinaryPredicate(
            "+",
            MakeColumnAccess(TInfoUnit("l_a"), pos, &testContext.ExprCtx, &expressionProps),
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto topMap = MakeIntrusive<TOpMap>(join, pos, TVector<TMapElement>{
            MakeTestRename("alias_payload", "payload", pos, testContext.ExprCtx, expressionProps),
            MakeTestAppend("left_calc", "l_a", pos, testContext.ExprCtx, expressionProps),
            TMapElement(TInfoUnit("both_calc"), bothSidesExpression, false),
        });

        auto filter = MakeIntrusive<TOpFilter>(
            topMap,
            pos,
            MakeColumnAccess(TInfoUnit("l_a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        TOpRoot root(filter, pos, {"alias_payload", "left_calc", "both_calc"});

        TRuleBasedStage mapAliasCleanup("Focused map alias cleanup", MakeMapAliasCleanupRulesForTest());
        ComputeLogicalTestProps(root);
        mapAliasCleanup.RunStage(root, testContext.RboCtx);

        TRuleBasedStage logicalMapRules("Focused logical map rules", MakeLogicalMapRulesForTest());
        ComputeLogicalTestProps(root);
        logicalMapRules.RunStage(root, testContext.RboCtx);
    }

    Y_UNIT_TEST(RemoveIdentityMapRemovesEmptyMap) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto emptyMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{});
        TOpRoot root(emptyMap, pos, {"a", "payload"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
        TRuleBasedStage removeIdentity("Focused remove identity map", std::move(rules));
        ComputeLogicalTestProps(root);
        removeIdentity.RunStage(root, testContext.RboCtx);
        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
    }

    Y_UNIT_TEST(RemoveIdentityMapUnderUnionAll) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("__kqp_agg_result_agg_col_0"), TInfoUnit("column0")}, pos);
        auto identityMap = MakeIntrusive<TOpMap>(leftRead, pos, TVector<TMapElement>{
            MakeTestRename("column0", "column0", pos, testContext.ExprCtx, expressionProps),
        });
        auto rightRead = MakeTestRead({TInfoUnit("column0")}, pos);
        auto unionAll = MakeIntrusive<TOpUnionAll>(identityMap, rightRead, pos);
        TOpRoot root(unionAll, pos, {"column0"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
        TRuleBasedStage removeIdentity("Focused remove identity map", std::move(rules));
        ComputeLogicalTestProps(root);
        removeIdentity.RunStage(root, testContext.RboCtx);

        auto rewrittenUnion = CastOperator<TOpUnionAll>(root.GetInput());
        UNIT_ASSERT_C(rewrittenUnion->GetLeftInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT(rewrittenUnion->GetLeftInput() == leftRead);
    }

    Y_UNIT_TEST(RemoveIdentityMapDoesNotCareAboutOutputOrder) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto identityMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestRename("a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(identityMap, pos, {"a", "payload"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRemoveIdenityMapRule>());
        TRuleBasedStage removeIdentity("Focused remove identity map", std::move(rules));
        ComputeLogicalTestProps(root);
        removeIdentity.RunStage(root, testContext.RboCtx);
        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT(root.GetInput() == read);
    }

    Y_UNIT_TEST(PruneDeadReadColumnsRule) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("dead_payload")}, pos);
        TOpRoot root(read, pos, {"a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>());
        TRuleBasedStage pruneRead("Focused read pruning", std::move(rules));
        ComputeLogicalTestProps(root);
        pruneRead.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(read->Columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read->OutputIUs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read->Columns.front(), "a");
        UNIT_ASSERT(read->OutputIUs.front() == TInfoUnit("a"));
    }

    Y_UNIT_TEST(NarrowByLivenessPrunesReadColumnsAfterDeadAggregateTraits) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value"), TInfoUnit("dead_value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
                TOpAggregationTraits(TInfoUnit("dead_value"), "sum", TInfoUnit("dead_sum")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        TOpRoot root(aggregate, pos, {"key", "sum_value"});

        ComputeLogicalTestProps(root);
        TLogicalOutputPruningStage outputPruning;
        outputPruning.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(aggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(aggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("sum_value"));

        const auto readOutput = read->GetOutputIUs();
        UNIT_ASSERT_VALUES_EQUAL(readOutput.size(), 2);
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("key")) != readOutput.end());
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("value")) != readOutput.end());
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("dead_value")) == readOutput.end());
    }

    Y_UNIT_TEST(PruneDeadAggregateTraitsEnablesReadColumnPruning) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value"), TInfoUnit("dead_value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
                TOpAggregationTraits(TInfoUnit("dead_value"), "sum", TInfoUnit("dead_sum")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        TOpRoot root(aggregate, pos, {"key", "sum_value"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
        rules.emplace_back(std::make_unique<TPruneDeadReadColumnsRule>());
        TRuleBasedStage pruneAggregateAndRead("Focused aggregate/read pruning", std::move(rules));
        ComputeLogicalTestProps(root);
        pruneAggregateAndRead.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(aggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(aggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("sum_value"));

        const auto readOutput = read->GetOutputIUs();
        UNIT_ASSERT_VALUES_EQUAL(readOutput.size(), 2);
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("key")) != readOutput.end());
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("value")) != readOutput.end());
        UNIT_ASSERT(std::find(readOutput.begin(), readOutput.end(), TInfoUnit("dead_value")) == readOutput.end());
    }

    Y_UNIT_TEST(PushRenamePushesAggregateResultRename) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        auto renameMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestRename("total", "sum_value", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(renameMap, pos, {"key", "total"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList.front().OriginalColName == TInfoUnit("value"));
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("total"));
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->KeyColumns.front() == TInfoUnit("key"));

        const auto aggregateOutput = rewrittenAggregate->GetOutputIUs();
        UNIT_ASSERT(std::find(aggregateOutput.begin(), aggregateOutput.end(), TInfoUnit("total")) != aggregateOutput.end());
        UNIT_ASSERT(std::find(aggregateOutput.begin(), aggregateOutput.end(), TInfoUnit("sum_value")) == aggregateOutput.end());
    }

    Y_UNIT_TEST(PushRenamePushesAggregateKeyAppendAliasThroughSort) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        auto aliasMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestAppend("alias_key", "key", pos, testContext.ExprCtx, expressionProps),
        });
        auto sort = MakeIntrusive<TOpSort>(
            aliasMap,
            pos,
            TVector<TSortElement>{TSortElement(TInfoUnit("alias_key"), true, true)},
            std::nullopt
        );
        TOpRoot root(sort, pos, {"alias_key", "sum_value"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Sort, root.PlanToString(testContext.ExprCtx));
        auto rewrittenSort = CastOperator<TOpSort>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenSort->SortElements.size(), 1);
        UNIT_ASSERT(rewrittenSort->SortElements.front().SortColumn == TInfoUnit("alias_key"));

        UNIT_ASSERT_C(rewrittenSort->GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(rewrittenSort->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList.front().OriginalColName == TInfoUnit("value"));
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("sum_value"));
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->KeyColumns.front() == TInfoUnit("alias_key"));

        auto rewrittenRead = CastOperator<TOpRead>(rewrittenAggregate->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenRead->Columns.front(), "key");
        UNIT_ASSERT(rewrittenRead->OutputIUs.front() == TInfoUnit("alias_key"));

        const auto aggregateOutput = rewrittenAggregate->GetOutputIUs();
        UNIT_ASSERT(std::find(aggregateOutput.begin(), aggregateOutput.end(), TInfoUnit("alias_key")) != aggregateOutput.end());
        UNIT_ASSERT(std::find(aggregateOutput.begin(), aggregateOutput.end(), TInfoUnit("key")) == aggregateOutput.end());
    }

    Y_UNIT_TEST(PushRenamePushesAggregateKeySemanticRename) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        auto renameMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestRename("alias_key", "key", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(renameMap, pos, {"alias_key", "sum_value"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->KeyColumns.front() == TInfoUnit("alias_key"));

        auto rewrittenRead = CastOperator<TOpRead>(rewrittenAggregate->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenRead->Columns.front(), "key");
        UNIT_ASSERT(rewrittenRead->OutputIUs.front() == TInfoUnit("alias_key"));
    }

    Y_UNIT_TEST(PushAppendPushesAggregateKeyAliasBelowAggregate) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        auto aliasMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestAppend("alias_key", "key", pos, testContext.ExprCtx, expressionProps),
            MakeTestConstantAppend("sale_type", pos, testContext.ExprCtx),
        });
        TOpRoot root(aliasMap, pos, {"alias_key", "sum_value", "sale_type"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>());
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto residualMap = CastOperator<TOpMap>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL_C(residualMap->MapElements.size(), 1, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT(residualMap->MapElements.front().GetElementName() == TInfoUnit("sale_type"));

        UNIT_ASSERT_C(residualMap->GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(residualMap->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->KeyColumns.front() == TInfoUnit("alias_key"));

        UNIT_ASSERT_C(rewrittenAggregate->GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto pushedMap = CastOperator<TOpMap>(rewrittenAggregate->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(pushedMap->MapElements.size(), 1);
        UNIT_ASSERT(pushedMap->MapElements.front().GetElementName() == TInfoUnit("alias_key"));
        UNIT_ASSERT(pushedMap->MapElements.front().GetColumnAccess() == TInfoUnit("key"));
    }

    Y_UNIT_TEST(PushAppendPushesDistinctAllKeyAliasAndResultBelowAggregate) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("other_key")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("key"), "distinct", TInfoUnit("key")),
                TOpAggregationTraits(TInfoUnit("other_key"), "distinct", TInfoUnit("other_key")),
            },
            TVector<TInfoUnit>{TInfoUnit("key"), TInfoUnit("other_key")},
            EOpPhase::Undefined,
            true,
            pos
        );
        auto aliasMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestAppend("alias_key", "key", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"alias_key", "other_key"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>());
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), 2);
        UNIT_ASSERT(rewrittenAggregate->KeyColumns[0] == TInfoUnit("alias_key"));
        UNIT_ASSERT(rewrittenAggregate->KeyColumns[1] == TInfoUnit("other_key"));

        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->AggregationTraitsList.size(), 2);
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList[0].OriginalColName == TInfoUnit("alias_key"));
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList[0].ResultColName == TInfoUnit("alias_key"));
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList[1].OriginalColName == TInfoUnit("other_key"));
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList[1].ResultColName == TInfoUnit("other_key"));

        UNIT_ASSERT_C(rewrittenAggregate->GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto pushedMap = CastOperator<TOpMap>(rewrittenAggregate->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(pushedMap->MapElements.size(), 1);
        UNIT_ASSERT(pushedMap->MapElements.front().GetElementName() == TInfoUnit("alias_key"));
        UNIT_ASSERT(pushedMap->MapElements.front().GetColumnAccess() == TInfoUnit("key"));
    }

    Y_UNIT_TEST(PushAppendDoesNotPushAggregateKeyAliasWhenOriginalKeyIsLive) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("key"), TInfoUnit("value")}, pos);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value")),
            },
            TVector<TInfoUnit>{TInfoUnit("key")},
            EOpPhase::Final,
            false,
            pos
        );
        auto aliasMap = MakeIntrusive<TOpMap>(aggregate, pos, TVector<TMapElement>{
            MakeTestAppend("alias_key", "key", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"key", "alias_key", "sum_value"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>());
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput() == aliasMap, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT_C(aliasMap->GetInput() == aggregate, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(aggregate->KeyColumns.size(), 1);
        UNIT_ASSERT(aggregate->KeyColumns.front() == TInfoUnit("key"));
    }

    Y_UNIT_TEST(MapAliasCleanupOverAggregate) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        const TVector<TInfoUnit> qualifiedKeys = {
            TInfoUnit("customer.c_customer_id"),
            TInfoUnit("customer.c_first_name"),
            TInfoUnit("customer.c_last_name"),
            TInfoUnit("date_dim.d_year"),
        };
        const TVector<TString> aliases = {
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "dyear",
        };

        TVector<TInfoUnit> readIUs = qualifiedKeys;
        readIUs.push_back(TInfoUnit("__kqp_agg_input_agg_expr_2"));
        auto read = MakeTestRead(readIUs, pos);

        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(TInfoUnit("__kqp_agg_input_agg_expr_2"), "sum", TInfoUnit("year_total")),
            },
            qualifiedKeys,
            EOpPhase::Final,
            false,
            pos
        );

        TVector<TMapElement> mapElements;
        for (ui32 i = 0; i < qualifiedKeys.size(); ++i) {
            mapElements.push_back(MakeTestAppend(aliases[i], qualifiedKeys[i].GetFullName(), pos, testContext.ExprCtx, expressionProps));
        }
        mapElements.push_back(TMapElement(TInfoUnit("sale_type"), MakeConstant("String", "c", pos, &testContext.ExprCtx), false));
        mapElements.push_back(MakeTestAppend("__kqp_agg_result_agg_col_1", "year_total", pos, testContext.ExprCtx, expressionProps));

        auto aliasMap = MakeIntrusive<TOpMap>(aggregate, pos, std::move(mapElements));
        TOpRoot root(aliasMap, pos, {
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "dyear",
            "sale_type",
            "__kqp_agg_result_agg_col_1",
        });

        TRuleBasedStage mapAliasCleanup("Focused map alias cleanup", MakeMapAliasCleanupRulesForTest());
        ComputeLogicalTestProps(root);
        mapAliasCleanup.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto residualMap = CastOperator<TOpMap>(root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL_C(residualMap->MapElements.size(), 1, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT(residualMap->MapElements.front().GetElementName() == TInfoUnit("sale_type"));

        UNIT_ASSERT_C(residualMap->GetInput()->Kind == EOperator::Aggregate, root.PlanToString(testContext.ExprCtx));
        auto rewrittenAggregate = CastOperator<TOpAggregate>(residualMap->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->KeyColumns.size(), aliases.size());
        for (ui32 i = 0; i < aliases.size(); ++i) {
            UNIT_ASSERT_C(rewrittenAggregate->KeyColumns[i] == TInfoUnit(aliases[i]), root.PlanToString(testContext.ExprCtx));
        }
        UNIT_ASSERT_VALUES_EQUAL(rewrittenAggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(rewrittenAggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("__kqp_agg_result_agg_col_1"));
    }

    Y_UNIT_TEST(PushRenameUpdatesPendingSubplanTuple) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        const auto subplanIU = TInfoUnit("_rbo_arg_1", true);
        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(subplanIU, pos, &testContext.ExprCtx, &expressionProps)
        );
        auto aliasMap = MakeIntrusive<TOpMap>(filter, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"l_a"});

        auto subplanRead = MakeTestRead({TInfoUnit("rhs")}, pos);
        TSubplanEntry subplanEntry;
        subplanEntry.Plan = subplanRead;
        subplanEntry.Tuple = {TInfoUnit("a")};
        subplanEntry.Type = ESubplanType::IN_SUBPLAN;
        subplanEntry.IU = subplanIU;
        root.PlanProps.Subplans.Add(subplanIU, subplanEntry);
        filter->FilterExpr.PlanProps = &root.PlanProps;

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        const auto& rewrittenEntry = root.PlanProps.Subplans.PlanMap.at(subplanIU);
        UNIT_ASSERT_VALUES_EQUAL(rewrittenEntry.Tuple.size(), 1);
        UNIT_ASSERT(rewrittenEntry.Tuple.front() == TInfoUnit("l_a"));

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
        auto rewrittenFilter = CastOperator<TOpFilter>(root.GetInput());
        auto rewrittenRead = CastOperator<TOpRead>(rewrittenFilter->GetInput());
        UNIT_ASSERT(rewrittenRead->OutputIUs.front() == TInfoUnit("l_a"));
    }

    Y_UNIT_TEST(PushRenameUpdatesPendingSubplanDependencies) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        const auto subplanIU = TInfoUnit("_rbo_arg_1", true);
        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto outerFilter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(subplanIU, pos, &testContext.ExprCtx, &expressionProps)
        );
        auto aliasMap = MakeIntrusive<TOpMap>(outerFilter, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"l_a"});

        auto subplanRead = MakeTestRead({TInfoUnit("rhs")}, pos);
        auto addDeps = MakeIntrusive<TOpAddDependencies>(
            subplanRead,
            pos,
            TVector<TInfoUnit>{TInfoUnit("a")},
            TVector<const TTypeAnnotationNode*>{nullptr}
        );
        auto subplanFilter = MakeIntrusive<TOpFilter>(
            addDeps,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );

        TSubplanEntry subplanEntry;
        subplanEntry.Plan = subplanFilter;
        subplanEntry.Type = ESubplanType::EXISTS;
        subplanEntry.IU = subplanIU;
        subplanEntry.DependentIUs = {TInfoUnit("a")};
        root.PlanProps.Subplans.Add(subplanIU, subplanEntry);
        outerFilter->FilterExpr.PlanProps = &root.PlanProps;
        subplanFilter->FilterExpr.PlanProps = &root.PlanProps;

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        const auto& rewrittenEntry = root.PlanProps.Subplans.PlanMap.at(subplanIU);
        UNIT_ASSERT_VALUES_EQUAL(rewrittenEntry.DependentIUs.size(), 1);
        UNIT_ASSERT(rewrittenEntry.DependentIUs.front() == TInfoUnit("l_a"));
        UNIT_ASSERT_VALUES_EQUAL(addDeps->Dependencies.size(), 1);
        UNIT_ASSERT(addDeps->Dependencies.front() == TInfoUnit("l_a"));

        const auto subplanFilterInputs = subplanFilter->FilterExpr.GetInputIUs(false, true);
        UNIT_ASSERT(std::find(subplanFilterInputs.begin(), subplanFilterInputs.end(), TInfoUnit("l_a")) != subplanFilterInputs.end());
        UNIT_ASSERT(std::find(subplanFilterInputs.begin(), subplanFilterInputs.end(), TInfoUnit("a")) == subplanFilterInputs.end());
    }

    Y_UNIT_TEST(RewritePreferredAliasUpdatesPendingSubplanTuple) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        const auto subplanIU = TInfoUnit("_rbo_arg_1", true);
        auto read = MakeTestRead({TInfoUnit("b"), TInfoUnit("payload")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto filter = MakeIntrusive<TOpFilter>(
            aliasMap,
            pos,
            MakeColumnAccess(subplanIU, pos, &testContext.ExprCtx, &expressionProps)
        );
        TOpRoot root(filter, pos, {"a"});

        auto subplanRead = MakeTestRead({TInfoUnit("rhs")}, pos);
        TSubplanEntry subplanEntry;
        subplanEntry.Plan = subplanRead;
        subplanEntry.Tuple = {TInfoUnit("b")};
        subplanEntry.Type = ESubplanType::IN_SUBPLAN;
        subplanEntry.IU = subplanIU;
        root.PlanProps.Subplans.Add(subplanIU, subplanEntry);
        filter->FilterExpr.PlanProps = &root.PlanProps;

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        const auto& rewrittenEntry = root.PlanProps.Subplans.PlanMap.at(subplanIU);
        UNIT_ASSERT_VALUES_EQUAL(rewrittenEntry.Tuple.size(), 1);
        UNIT_ASSERT(rewrittenEntry.Tuple.front() == TInfoUnit("a"));
    }

    Y_UNIT_TEST(PushRenameTreatsDeadAppendAliasAsRename) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto aliasMap = MakeIntrusive<TOpMap>(filter, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"l_a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
        auto rewrittenFilter = CastOperator<TOpFilter>(root.GetInput());
        auto rewrittenRead = CastOperator<TOpRead>(rewrittenFilter->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenRead->Columns.front(), "a");
        UNIT_ASSERT(rewrittenRead->OutputIUs.front() == TInfoUnit("l_a"));

        const auto filterInputs = rewrittenFilter->FilterExpr.GetInputIUs(false, true);
        UNIT_ASSERT(std::find(filterInputs.begin(), filterInputs.end(), TInfoUnit("l_a")) != filterInputs.end());
        UNIT_ASSERT(std::find(filterInputs.begin(), filterInputs.end(), TInfoUnit("a")) == filterInputs.end());
    }

    Y_UNIT_TEST(PushRenameTreatsDeadAppendAliasAsRenameThroughJoin) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto rightRead = MakeTestRead({TInfoUnit("b"), TInfoUnit("right_payload")}, pos);
        auto join = MakeIntrusive<TOpJoin>(
            leftRead,
            rightRead,
            pos,
            "Inner",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a"), TInfoUnit("b")}}
        );
        auto aliasMap = MakeIntrusive<TOpMap>(join, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"l_a", "right_payload"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage pushRename("Focused push rename", std::move(rules));
        ComputeLogicalTestProps(root);
        pushRename.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Join, root.PlanToString(testContext.ExprCtx));
        auto rewrittenJoin = CastOperator<TOpJoin>(root.GetInput());
        auto rewrittenLeftRead = CastOperator<TOpRead>(rewrittenJoin->GetLeftInput());
        auto rewrittenRightRead = CastOperator<TOpRead>(rewrittenJoin->GetRightInput());

        UNIT_ASSERT_VALUES_EQUAL(rewrittenLeftRead->Columns.front(), "a");
        UNIT_ASSERT(rewrittenLeftRead->OutputIUs.front() == TInfoUnit("l_a"));
        UNIT_ASSERT(rewrittenRightRead->OutputIUs.front() == TInfoUnit("b"));

        UNIT_ASSERT_VALUES_EQUAL(rewrittenJoin->JoinKeys.size(), 1);
        UNIT_ASSERT(rewrittenJoin->JoinKeys.front().first == TInfoUnit("l_a"));
        UNIT_ASSERT(rewrittenJoin->JoinKeys.front().second == TInfoUnit("b"));

        const auto joinOutput = rewrittenJoin->GetOutputIUs();
        UNIT_ASSERT(std::find(joinOutput.begin(), joinOutput.end(), TInfoUnit("l_a")) != joinOutput.end());
        UNIT_ASSERT(std::find(joinOutput.begin(), joinOutput.end(), TInfoUnit("a")) == joinOutput.end());
    }

    Y_UNIT_TEST(RewriteFilterToPreferredAliasEnablesRenamePush) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("b"), TInfoUnit("payload")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto filter = MakeIntrusive<TOpFilter>(
            aliasMap,
            pos,
            MakeColumnAccess(TInfoUnit("b"), pos, &testContext.ExprCtx, &expressionProps)
        );
        TOpRoot root(filter, pos, {"a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        rules.emplace_back(std::make_unique<TPushRenameRule>());
        TRuleBasedStage aliasRules("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        aliasRules.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
        auto rewrittenFilter = CastOperator<TOpFilter>(root.GetInput());
        UNIT_ASSERT_C(rewrittenFilter->GetInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
        auto rewrittenRead = CastOperator<TOpRead>(rewrittenFilter->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(rewrittenRead->Columns.front(), "b");
        UNIT_ASSERT(rewrittenRead->OutputIUs.front() == TInfoUnit("a"));

        const auto filterInputs = rewrittenFilter->FilterExpr.GetInputIUs(false, true);
        UNIT_ASSERT(std::find(filterInputs.begin(), filterInputs.end(), TInfoUnit("a")) != filterInputs.end());
        UNIT_ASSERT(std::find(filterInputs.begin(), filterInputs.end(), TInfoUnit("b")) == filterInputs.end());
    }

    Y_UNIT_TEST(RewritePreferredAliasUpdatesMapExpression) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("b"), TInfoUnit("payload")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto topMap = MakeIntrusive<TOpMap>(aliasMap, pos, TVector<TMapElement>{
            MakeTestAppend("out", "b", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(topMap, pos, {"a", "out"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(topMap->MapElements.size(), 1);
        UNIT_ASSERT(topMap->MapElements.front().IsColumnAccess());
        UNIT_ASSERT(topMap->MapElements.front().GetColumnAccess() == TInfoUnit("a"));
    }

    Y_UNIT_TEST(RewritePreferredAliasDoesNotPreferGeneratedIgnoreName) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();
        const auto ignore = TInfoUnit("__kqp_rbo_ignore_arg_0");

        auto read = MakeTestRead({TInfoUnit("b")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
            MakeTestAppend(ignore.GetFullName(), "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto topMap = MakeIntrusive<TOpMap>(aliasMap, pos, TVector<TMapElement>{
            MakeTestAppend("out", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(topMap, pos, {ignore.GetFullName(), "out"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(topMap->MapElements.size(), 1);
        UNIT_ASSERT(topMap->MapElements.front().IsColumnAccess());
        UNIT_ASSERT(topMap->MapElements.front().GetColumnAccess() == TInfoUnit("b"));
    }

    Y_UNIT_TEST(RewritePreferredAliasUpdatesJoinKeysAndFilters) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("b"), TInfoUnit("payload")}, pos);
        auto leftAliasMap = MakeIntrusive<TOpMap>(leftRead, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto rightRead = MakeTestRead({TInfoUnit("r")}, pos);
        auto joinFilter = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(TInfoUnit("b"), pos, &testContext.ExprCtx, &expressionProps),
            MakeColumnAccess(TInfoUnit("r"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto join = MakeIntrusive<TOpJoin>(
            leftAliasMap,
            rightRead,
            pos,
            "Inner",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("b"), TInfoUnit("r")}},
            TVector<TExpression>{joinFilter}
        );
        TOpRoot root(join, pos, {"a", "r"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(join->JoinKeys.size(), 1);
        UNIT_ASSERT(join->JoinKeys.front().first == TInfoUnit("a"));
        UNIT_ASSERT(join->JoinKeys.front().second == TInfoUnit("r"));

        UNIT_ASSERT_VALUES_EQUAL(join->JoinFilters.size(), 1);
        const auto joinFilterInputs = join->JoinFilters.front().GetInputIUs(false, true);
        UNIT_ASSERT(std::find(joinFilterInputs.begin(), joinFilterInputs.end(), TInfoUnit("a")) != joinFilterInputs.end());
        UNIT_ASSERT(std::find(joinFilterInputs.begin(), joinFilterInputs.end(), TInfoUnit("b")) == joinFilterInputs.end());
    }

    Y_UNIT_TEST(RewritePreferredAliasUpdatesAggregateInputs) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("b"), TInfoUnit("value")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
            MakeTestAppend("v", "value", pos, testContext.ExprCtx, expressionProps),
        });
        auto aggregate = MakeIntrusive<TOpAggregate>(
            aliasMap,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(TInfoUnit("value"), "sum", TInfoUnit("sum_value"))},
            TVector<TInfoUnit>{TInfoUnit("b"), TInfoUnit("v")},
            EOpPhase::Final,
            false,
            pos
        );
        TOpRoot root(aggregate, pos, {"a", "v", "sum_value"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(aggregate->KeyColumns.size(), 2);
        UNIT_ASSERT(aggregate->KeyColumns[0] == TInfoUnit("a"));
        UNIT_ASSERT(aggregate->KeyColumns[1] == TInfoUnit("v"));
        UNIT_ASSERT_VALUES_EQUAL(aggregate->AggregationTraitsList.size(), 1);
        UNIT_ASSERT(aggregate->AggregationTraitsList.front().OriginalColName == TInfoUnit("v"));
        UNIT_ASSERT(aggregate->AggregationTraitsList.front().ResultColName == TInfoUnit("sum_value"));
    }

    Y_UNIT_TEST(RewritePreferredAliasUpdatesSortAndLimitInputs) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("b"), TInfoUnit("payload")}, pos);
        auto aliasMap = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            MakeTestAppend("a", "b", pos, testContext.ExprCtx, expressionProps),
        });
        auto limit = MakeIntrusive<TOpLimit>(
            aliasMap,
            pos,
            MakeColumnAccess(TInfoUnit("b"), pos, &testContext.ExprCtx, &expressionProps),
            EOpPhase::Final
        );
        auto sort = MakeIntrusive<TOpSort>(
            limit,
            pos,
            TVector<TSortElement>{TSortElement(TInfoUnit("b"), true, true)},
            MakeColumnAccess(TInfoUnit("b"), pos, &testContext.ExprCtx, &expressionProps)
        );
        TOpRoot root(sort, pos, {"a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteExpressionsToPreferredAliasesRule>());
        TRuleBasedStage rewriteAliases("Focused alias rewrite", std::move(rules));
        ComputeLogicalTestProps(root);
        rewriteAliases.RunStage(root, testContext.RboCtx);

        const auto limitInputs = limit->LimitCond.GetInputIUs(false, true);
        UNIT_ASSERT(std::find(limitInputs.begin(), limitInputs.end(), TInfoUnit("a")) != limitInputs.end());
        UNIT_ASSERT(std::find(limitInputs.begin(), limitInputs.end(), TInfoUnit("b")) == limitInputs.end());

        UNIT_ASSERT_VALUES_EQUAL(sort->SortElements.size(), 1);
        UNIT_ASSERT(sort->SortElements.front().SortColumn == TInfoUnit("a"));
        UNIT_ASSERT(sort->LimitCond);
        const auto sortLimitInputs = sort->LimitCond->GetInputIUs(false, true);
        UNIT_ASSERT(std::find(sortLimitInputs.begin(), sortLimitInputs.end(), TInfoUnit("a")) != sortLimitInputs.end());
        UNIT_ASSERT(std::find(sortLimitInputs.begin(), sortLimitInputs.end(), TInfoUnit("b")) == sortLimitInputs.end());
    }

    Y_UNIT_TEST(PushAppendAliasCrossesTransparentUnaryChain) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a"), TInfoUnit("payload")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto addDeps = MakeIntrusive<TOpAddDependencies>(
            filter,
            pos,
            TVector<TInfoUnit>{TInfoUnit("dep")},
            TVector<const TTypeAnnotationNode*>{nullptr}
        );
        auto limit = MakeIntrusive<TOpLimit>(
            addDeps,
            pos,
            MakeConstant("Uint64", "10", pos, &testContext.ExprCtx),
            EOpPhase::Final
        );
        auto sort = MakeIntrusive<TOpSort>(
            limit,
            pos,
            TVector<TSortElement>{TSortElement(TInfoUnit("payload"), true, true)}
        );
        auto aliasMap = MakeIntrusive<TOpMap>(sort, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"a", "payload", "dep", "l_a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>());
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Sort, root.PlanToString(testContext.ExprCtx));
        auto rewrittenSort = CastOperator<TOpSort>(root.GetInput());
        UNIT_ASSERT_C(rewrittenSort->GetInput()->Kind == EOperator::Limit, root.PlanToString(testContext.ExprCtx));
        auto rewrittenLimit = CastOperator<TOpLimit>(rewrittenSort->GetInput());
        UNIT_ASSERT_C(rewrittenLimit->GetInput()->Kind == EOperator::AddDependencies, root.PlanToString(testContext.ExprCtx));
        auto rewrittenDeps = CastOperator<TOpAddDependencies>(rewrittenLimit->GetInput());
        UNIT_ASSERT_C(rewrittenDeps->GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
        auto rewrittenFilter = CastOperator<TOpFilter>(rewrittenDeps->GetInput());
        UNIT_ASSERT_C(rewrittenFilter->GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto pushedMap = CastOperator<TOpMap>(rewrittenFilter->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(pushedMap->MapElements.size(), 1);
        UNIT_ASSERT(pushedMap->MapElements.front().GetElementName() == TInfoUnit("l_a"));
        UNIT_ASSERT(pushedMap->MapElements.front().IsColumnAccess());
        UNIT_ASSERT_C(pushedMap->GetInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
    }

    Y_UNIT_TEST(PushAppendAliasDoesNotCrossFilterWhenDisabled) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto aliasMap = MakeIntrusive<TOpMap>(filter, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"a", "l_a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>(/*pushUnderFilter*/ false));
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto topMap = CastOperator<TOpMap>(root.GetInput());
        UNIT_ASSERT_C(topMap->GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
    }

    Y_UNIT_TEST(PushAppendAliasCrossesFullJoinSide) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto rightRead = MakeTestRead({TInfoUnit("b")}, pos);
        auto join = MakeIntrusive<TOpJoin>(
            leftRead,
            rightRead,
            pos,
            "Full",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a"), TInfoUnit("b")}}
        );
        auto aliasMap = MakeIntrusive<TOpMap>(join, pos, TVector<TMapElement>{
            MakeTestAppend("l_a", "a", pos, testContext.ExprCtx, expressionProps),
        });
        TOpRoot root(aliasMap, pos, {"a", "b", "l_a"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendRule>());
        TRuleBasedStage pushAppend("Focused push append", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Join, root.PlanToString(testContext.ExprCtx));
        auto rewrittenJoin = CastOperator<TOpJoin>(root.GetInput());
        UNIT_ASSERT_C(rewrittenJoin->GetLeftInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT_C(rewrittenJoin->GetRightInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));

        auto leftMap = CastOperator<TOpMap>(rewrittenJoin->GetLeftInput());
        UNIT_ASSERT_VALUES_EQUAL(leftMap->MapElements.size(), 1);
        UNIT_ASSERT(leftMap->MapElements.front().GetElementName() == TInfoUnit("l_a"));
        UNIT_ASSERT(leftMap->MapElements.front().IsColumnAccess());

        const auto joinOutput = rewrittenJoin->GetOutputIUs();
        UNIT_ASSERT(std::find(joinOutput.begin(), joinOutput.end(), TInfoUnit("a")) != joinOutput.end());
        UNIT_ASSERT(std::find(joinOutput.begin(), joinOutput.end(), TInfoUnit("l_a")) != joinOutput.end());
    }

    Y_UNIT_TEST(PushAppendExpressionCrossesFilterWhenEnabled) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto appendMap = MakeIntrusive<TOpMap>(filter, pos, TVector<TMapElement>{
            MakeTestConstantAppend("one", pos, testContext.ExprCtx),
        });
        TOpRoot root(appendMap, pos, {"a", "one"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendExpressionRule>(/*pushUnderFilter*/ true));
        TRuleBasedStage pushAppend("Focused push append expressions", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
        auto rewrittenFilter = CastOperator<TOpFilter>(root.GetInput());
        UNIT_ASSERT_C(rewrittenFilter->GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto pushedMap = CastOperator<TOpMap>(rewrittenFilter->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(pushedMap->MapElements.size(), 1);
        UNIT_ASSERT(pushedMap->MapElements.front().GetElementName() == TInfoUnit("one"));
    }

    Y_UNIT_TEST(PushAppendExpressionDoesNotCrossFilterWhenDisabled) {
        TMapRuleTestContext testContext;
        TPlanProps expressionProps;
        const auto pos = NYql::TPositionHandle();

        auto read = MakeTestRead({TInfoUnit("a")}, pos);
        auto filter = MakeIntrusive<TOpFilter>(
            read,
            pos,
            MakeColumnAccess(TInfoUnit("a"), pos, &testContext.ExprCtx, &expressionProps)
        );
        auto appendMap = MakeIntrusive<TOpMap>(filter, pos, TVector<TMapElement>{
            MakeTestConstantAppend("one", pos, testContext.ExprCtx),
        });
        TOpRoot root(appendMap, pos, {"a", "one"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendExpressionRule>(/*pushUnderFilter*/ false));
        TRuleBasedStage pushAppend("Focused push append expressions", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto topMap = CastOperator<TOpMap>(root.GetInput());
        UNIT_ASSERT_C(topMap->GetInput()->Kind == EOperator::Filter, root.PlanToString(testContext.ExprCtx));
    }

    Y_UNIT_TEST(PushAppendExpressionConstantChoosesPreservedJoinSide) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto rightRead = MakeTestRead({TInfoUnit("b")}, pos);
        auto join = MakeIntrusive<TOpJoin>(
            leftRead,
            rightRead,
            pos,
            "Right",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a"), TInfoUnit("b")}}
        );
        auto appendMap = MakeIntrusive<TOpMap>(join, pos, TVector<TMapElement>{
            MakeTestConstantAppend("one", pos, testContext.ExprCtx),
        });
        TOpRoot root(appendMap, pos, {"a", "b", "one"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendExpressionRule>());
        TRuleBasedStage pushAppend("Focused push append expressions", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Join, root.PlanToString(testContext.ExprCtx));
        auto rewrittenJoin = CastOperator<TOpJoin>(root.GetInput());
        UNIT_ASSERT_C(rewrittenJoin->GetLeftInput()->Kind == EOperator::Source, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT_C(rewrittenJoin->GetRightInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));

        auto rightMap = CastOperator<TOpMap>(rewrittenJoin->GetRightInput());
        UNIT_ASSERT_VALUES_EQUAL(rightMap->MapElements.size(), 1);
        UNIT_ASSERT(rightMap->MapElements.front().GetElementName() == TInfoUnit("one"));
    }

    Y_UNIT_TEST(PushAppendExpressionConstantStaysAboveFullJoin) {
        TMapRuleTestContext testContext;
        const auto pos = NYql::TPositionHandle();

        auto leftRead = MakeTestRead({TInfoUnit("a")}, pos);
        auto rightRead = MakeTestRead({TInfoUnit("b")}, pos);
        auto join = MakeIntrusive<TOpJoin>(
            leftRead,
            rightRead,
            pos,
            "Full",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a"), TInfoUnit("b")}}
        );
        auto appendMap = MakeIntrusive<TOpMap>(join, pos, TVector<TMapElement>{
            MakeTestConstantAppend("one", pos, testContext.ExprCtx),
        });
        TOpRoot root(appendMap, pos, {"a", "b", "one"});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushAppendExpressionRule>());
        TRuleBasedStage pushAppend("Focused push append expressions", std::move(rules));
        ComputeLogicalTestProps(root);
        pushAppend.RunStage(root, testContext.RboCtx);

        UNIT_ASSERT_C(root.GetInput()->Kind == EOperator::Map, root.PlanToString(testContext.ExprCtx));
        auto topMap = CastOperator<TOpMap>(root.GetInput());
        UNIT_ASSERT_C(topMap->GetInput()->Kind == EOperator::Join, root.PlanToString(testContext.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(topMap->MapElements.size(), 1);
        UNIT_ASSERT(topMap->MapElements.front().GetElementName() == TInfoUnit("one"));
    }

    Y_UNIT_TEST(TPCH_YQL) {
        // RunTPCHYqlBenchmark(/*columnstore*/ true, {}, {}, /*new rbo*/ false);
        // Q11 is intentionally omitted: it is not accepted by the current New RBO benchmark path.
        RunTPC_YqlBenchmark(EBenchType::TPCH, /*columnstore=*/true, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
                            {}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true);
    }

    Y_UNIT_TEST(TPCDS_YQL) {
        // RunTPC_YqlBenchmark(EBenchType::TPCDS, /*columnstore*/ true, {}, {}, /*new rbo*/ false);
        RunTPC_YqlBenchmark(EBenchType::TPCDS, /*columnstore=*/true, {1, 2, 3, 4, 5, 6, 7, 10, 11, 13, 15, 18, 19, 21, 22, 24, 25, 26, 29, 30, 31, 32, 33, 34, 35, 37, 40, 42, 43, 46, 48,
                                                                      50, 52, 54, 55, 56, 58, 59, 60, 61, 62, 64, 65, 66, 68, 69, 71, 72, 73, 74, 76, 77, 78, 79, 81, 82, 83,
                                                                      84, 85, 88, 90, 91, 92, 93, 96, 99},
                           /*rbo never finish*/{}, /*new rbo=*/true, /*printStatus=*/true, /*compareResults=*/true, /*checkNewRBOCbo=*/true,
                           // Still explain these queries, but do not require the CBO stats invariant until the known gaps are fixed.
                           /*queriesWithoutCboCheck=*/{15, 31, 58, 64, 72, 78, 85});
    }

    Y_UNIT_TEST(TPCDS_YQL_Q77_Scratch) {
        RunTPC_YqlBenchmark(EBenchType::TPCDS, /*columnstore=*/true, {77}, MakeTPC_YqlSingleQuerySkipList(EBenchType::TPCDS, 77),
                            /*new rbo=*/true, /*printStatus=*/true, /*compareResults=*/true, /*checkNewRBOCbo=*/false);
    }

    void InsertIntoSchema0(NYdb::NTable::TTableClient& db, std::string tableName, ui32 numRows) {
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < numRows; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").String(std::to_string(i) + "_b")
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rows.EndList();
        auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
    }

    void InsertIntoSchema1(NYdb::NTable::TTableClient& db, std::string tableName, ui32 numRows) {
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < numRows; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .AddMember("c").Int64(i + 2)
                .AddMember("d").Int64(i + 3)
                .AddMember("e").Int64(i + 4)
                .EndStruct();
        }
        rows.EndList();
        auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExpressionSubquery) {
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
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            ) with (Store = Column);
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableFoo;
        rowsTableFoo.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableFoo.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableFoo.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rowsTableFoo.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableBar;
        rowsTableBar.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableBar.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("lastname").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableBar.EndList();

        resultUpsert = db.BulkUpsert("/Root/bar", rowsTableBar.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id = (SELECT max(foo.id) FROM `/Root/foo` as foo);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id IN (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND bar.id NOT IN (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id != 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id != 0);
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == 0 AND NOT EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == 6);
            )",
        };

        // TODO: The order of result is not defined, we need order by to add more interesting tests.
        std::vector<std::string> results = {
            R"([[3]])",
            R"([[0]])",
            R"([[0]])",
            R"([[0]])",
            R"([[0]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(CorrelatedSubquery) {
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
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                id2 Int64 NOT NULL,
                name	String,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                id2 Int64 NOT NULL,
                lastname	String,
                primary key(id)
            ) with (Store = Column);
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableFoo;
        rowsTableFoo.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableFoo.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("id2").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableFoo.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rowsTableFoo.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableBar;
        rowsTableBar.BeginList();
        for (size_t i = 0; i < 4; ++i) {
            rowsTableBar.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("id2").Int64(i)
                .AddMember("lastname").String(std::to_string(i) + "_name")
                .EndStruct();
        }
        rowsTableBar.EndList();

        resultUpsert = db.BulkUpsert("/Root/bar", rowsTableBar.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.id == (SELECT max(foo.id) FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.name == lastname AND foo.id==1);
            )",
             R"(
                SELECT bar.id FROM `/Root/bar` as bar where EXISTS (SELECT foo.id FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.name == lastname AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id == bar.id AND foo.id2 >= bar.id2 AND foo.id==1);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where bar.lastname NOT IN (SELECT foo.name FROM `/Root/foo` as foo WHERE foo.id > bar.id ) order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo)) OR bar.id == (SELECT max(foo.id) FROM `/Root/foo` as foo);
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id)) OR bar.id == 1;
            )",
        };

        // TODO: The order of result is not defined, we need order by to add more interesting tests.
        std::vector<std::string> results = {
            R"([[1]])",
            R"([[1]])",
            R"([[1]])",
            R"([[1]])",
            R"([[0];[1];[2];[3]])",
            R"([[3]])",
            R"([[1]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(OrderBy) {
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
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                SELECT a FROM `/Root/t1`
                ORDER BY a DESC;
            )",
            R"(
                SELECT a, c FROM `/Root/t1`
                ORDER BY a DESC, c ASC;
            )",
            R"(
                SELECT a FROM `/Root/t1`
                UNION ALL
                SELECT a FROM `/Root/t2`
                ORDER BY a DESC;
            )"
        };

        std::vector<std::string> results = {
            R"([[3];[2];[1];[0]])",
            R"([[3;[4]];[2;[3]];[1;[2]];[0;[1]]])",
            R"([[3];[2];[2];[1];[1];[0];[0]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(MapJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
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
            UNIT_ASSERT_C(ast.find("MapJoinCore") != std::string::npos, TStringBuilder() << "Wrong join algo. Expected: " << "MapJoinCore");

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

    Y_UNIT_TEST(Having) {
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
                b Int64,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        NYdb::TValueBuilder rowsTableT1;
        rowsTableT1.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rowsTableT1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i & 1 ? 1 : 2)
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.c) > 0 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.c) < 10 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.a) >= 1 and sum(t1.c) <= 10 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.a FROM `/Root/t1` as t1 group by t1.a having sum(t1.c) > 1 and sum(t1.c) < 3 order by t1.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) >= 1 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a) + 2 >= 2 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 3) + 2 >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) + sum(t1.a + 2) >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a + 1) + 11, t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) + sum(t1.a + 2) >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) as a_sum FROM `/Root/t1` as t1 having sum(t1.a) >= 5 order by a_sum;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) FROM `/Root/t1` as t1 having sum(t1.b) >= 5 order by sum(t1.a)
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) FROM `/Root/t1` as t1 group by t1.c having sum(t1.b) >= 5 order by t1.c;
            )",
        };

        std::vector<std::string> results = {
            R"([[[30];[1]];[[25];[2]]])",
            R"([])",
            R"([])",
            R"([[[2];1]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[13;[2]];[14;[3]];[15;[4]];[16;[5]];[17;[6]];[18;[7]];[19;[8]];[20;[9]];[21;[10]]])",
            R"([[[45]]])",
            R"([[[45]]])",
            R"([])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(ColumnStatistics, ColumnStore) {
        auto enableNewRbo = [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableTableServiceConfig()->SetEnableNewRBO(true);
            // Fallback is enabled, because analyze uses UDAF which are not supported in NEW RBO.
            settings.AppConfig->MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
            settings.AppConfig->MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            settings.AppConfig->MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            settings.AppConfig->MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        };

        TTestEnv env(1, 1, true, enableNewRbo);
        CreateDatabase(env, "Database");
        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/Database/t1` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            )
        )";

        if (ColumnStore) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto result = session.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rowsTable;
        rowsTable.BeginList();
        for (size_t i = 0, e = (1 << 4); i < e; ++i) {
            rowsTable.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rowsTable.EndList();

        auto resultUpsert = client.BulkUpsert("/Root/Database/t1", rowsTable.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "t1")).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                select t1.a, t1.b from `/Root/Database/t1` as t1 where t1.a > 10;
            )",
        };

        auto session2 = client.GetSession().GetValueSync().GetSession();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    void TestQueryClient(bool columnTables) {
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
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
	            name String,
                b Int64,
                primary key(id)
            )
        )";

        if (columnTables) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .AddMember("b").Int64(i)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                SELECT id, b FROM `/Root/foo` WHERE b not in [1, 2] order by b;
            )",
            R"(
                SELECT * FROM `/Root/foo` WHERE name = '3_name' order by id;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;[0]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;[8]];[9;[9]]])",
            R"([[3;["3_name"];[3]]])",
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

            // Execute.
            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

     Y_UNIT_TEST_TWIN(QueryClient, ColumnStore) {
        TestQueryClient(ColumnStore);
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

    Y_UNIT_TEST_TWIN(OlapProjection, Explain) {
        TestOlapProjectionPushdown(Explain);
    }

    ui32 CountNumberOfCallables(const std::string& ast, const std::string_view callable) {
        ui32 count = 0;
        auto pos = ast.find(callable);
        while (pos != std::string::npos) {
            pos = ast.find(callable, pos + 1);
            ++count;
        }
        return count;
    }

    void TestLimit(bool columnTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/foo` (
                id Int64 NOT NULL,
	            name String,
                b Int64,
                primary key(id)
            )
        )";

        if (true || columnTables) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("name").String(std::to_string(i) + "_name")
                .AddMember("b").Int64(i)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                SELECT id FROM `/Root/foo` order by id limit 1 + 2;
            )",
            R"(
                SELECT id FROM `/Root/foo` order by id limit 5;
            )",
            R"(
                SELECT id FROM `/Root/foo` order by id limit 5 offset 1;
            )",
        };

        std::vector<std::string> results = {
            R"([[0];[1];[2]])",
            R"([[0];[1];[2];[3];[4]])",
            R"([[1];[2];[3];[4];[5]])"
        };

        auto queryClient = kikimr.GetQueryClient();

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqCnMerge"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(Limit, ColumnStore) {
        TestLimit(ColumnStore);
    }

    Y_UNIT_TEST(PropagateLimitThroughStages) {
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
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t2.a from `/Root/t1` as t1 join `/Root/t2` as t2 on t1.a = t2.a where t1.b = 10 limit 1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            // Any from Take -> WideTakeBlocks is also ok.
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 2);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Push limit to CS.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 where t1.b = 10 limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            // Pushed to cs.
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "ItemsLimit"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 where t1.b = 10 order by t1.a limit 1 + 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(PropagateTopSortThroughStages) {
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
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t2.a from `/Root/t1` as t1 join `/Root/t2` as t2 on t1.a = t2.a where t1.b = 10 order by t1.a limit 1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            // TopSort -> Take(TopSort())
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Just propagate through stages, cannot push to cs, because t1.b is not a key.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc, t1.b desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b asc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc limit 1 + 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Push to CS.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a desc limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "ItemsLimit"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Sorted"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a desc;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "WideSortBlocks"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqCnMerge"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(PropagateAggregateThroughStages) {
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
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        const std::vector<std::string> queries = {
            R"(
                select avg(t1.b) from `/Root/t1` as t1 group by t1.a;
            )",
            R"(
                select avg(t1.a) from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select avg(t1.a), avg(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select sum(t1.a), max(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.a), min(t1.b), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select count(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.a), max(t1.a) from `/Root/t1` as t1;
            )",
            R"(
                select sum(t1.a), min(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select count(t1.a) from `/Root/t1` as t1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqPhyHashCombine"), 2);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    void CreateSimpleTable(TKikimrRunner &kikimr) {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b Int64,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void TestFallbackToYql(bool fallbackToYqlEnabled, const std::vector<std::string>& queries,
                           const std::vector<std::pair<ui32, ui32>>& expectedCompileCounters, const std::vector<bool>& expectedResult) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(fallbackToYqlEnabled);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        CreateSimpleTable(kikimr);

        std::pair<ui32, ui32> intermediateResult{0, 0};
        for (ui32 i = 0; i < queries.size(); ++i) {
            auto queryClient = kikimr.GetQueryClient();
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), expectedResult[i], result.GetIssues().ToString());
            intermediateResult.first += expectedCompileCounters[i].first;
            intermediateResult.second += expectedCompileCounters[i].second;
            UNIT_ASSERT_VALUES_EQUAL(GetNewRBOCompileCounters(kikimr), intermediateResult);
        }
    }

    std::vector<std::string> GetQueriesToTestFallbackToYql() {
        std::vector<std::string> queries = {
            // Insert is not supported.
            R"(
                INSERT INTO `/Root/t1` (a, b, c) VALUES (1, 2, 3);
            )",
            // Simple supported query in new RBO.
            R"(
                select t1.a from `/Root/t1` as t1;
            )",
        };

        return queries;
    }

    std::vector<std::pair<ui32, ui32>> GetCompileCountersToTestFallbackToYql() {
        // Represents the number of successes and fails for each query with new RBO compiler pipeline.
        std::vector<std::pair<ui32, ui32>> expectedCompileCounters = {
            {0, 1},
            {1, 0}
        };

        return expectedCompileCounters;
    }

    Y_UNIT_TEST(FallbackToYqlEnabled) {
        // All queries should succeded because fallback to yql is enabled.
        const std::vector<bool> expectedResult{true, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/true, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult);
    }

    Y_UNIT_TEST(FallbackToYqlDisabled) {
        // First 2 queries should fail because fallback to yql is disabled.
        const std::vector<bool> expectedResult{false, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/false, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult);
    }


    void CollectHashShuffleFuncs(const NJson::TJsonValue& planNode, TVector<TString>& hashFuncs) {
        if (!planNode.IsMap()) {
            return;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto nodeType = planMap.find("Node Type");
                nodeType != planMap.end() && nodeType->second.GetStringSafe().StartsWith("HashShuffle")) {
            hashFuncs.push_back(planMap.at("HashFunc").GetStringSafe());
        }

        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                CollectHashShuffleFuncs(child, hashFuncs);
            }
        }
    }

    TVector<TString> CollectHashShuffleFuncs(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);

        TVector<TString> hashFuncs;
        CollectHashShuffleFuncs(planRoot.GetMapSafe().at("SimplifiedPlan"), hashFuncs);
        return hashFuncs;
    }

    void CollectHashShuffleDescriptions(const NJson::TJsonValue& planNode, TVector<TString>& hashShuffles) {
        if (!planNode.IsMap()) {
            return;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto nodeType = planMap.find("Node Type");
                nodeType != planMap.end() && nodeType->second.GetStringSafe().StartsWith("HashShuffle")) {
            TVector<TString> keyColumns;
            for (const auto& key : planMap.at("KeyColumns").GetArraySafe()) {
                keyColumns.push_back(key.GetStringSafe());
            }

            hashShuffles.push_back(TStringBuilder()
                << planMap.at("HashFunc").GetStringSafe()
                << "(" << JoinSeq(", ", keyColumns) << ")");
        }

        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                CollectHashShuffleDescriptions(child, hashShuffles);
            }
        }
    }

    TVector<TString> CollectHashShuffleDescriptions(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);

        TVector<TString> hashShuffles;
        CollectHashShuffleDescriptions(planRoot.GetMapSafe().at("SimplifiedPlan"), hashShuffles);
        return hashShuffles;
    }

    TVector<TString> SortDescriptions(TVector<TString> descriptions) {
        std::sort(descriptions.begin(), descriptions.end());
        return descriptions;
    }

    bool HasPhysicalHashShuffleWithHashFunc(const TString& ast, const TString& hashFunc) {
        size_t shufflePos = ast.find("DqCnHashShuffle");
        while (shufflePos != TString::npos) {
            const size_t nextShufflePos = ast.find("DqCnHashShuffle", shufflePos + 1);
            const size_t hashFuncPos = ast.find(hashFunc, shufflePos);
            if (hashFuncPos != TString::npos && (nextShufflePos == TString::npos || hashFuncPos < nextShufflePos)) {
                return true;
            }
            shufflePos = nextShufflePos;
        }

        return false;
    }

    bool IsHashShufflePlanNode(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto nodeType = planMap.find("Node Type"); nodeType != planMap.end()) {
            return nodeType->second.GetStringSafe().StartsWith("HashShuffle");
        }

        return false;
    }

    bool IsGraceJoinPlanNode(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
            for (const auto& opNode : operators->second.GetArraySafe()) {
                const auto& op = opNode.GetMapSafe();
                const auto opName = op.at("Name").GetStringSafe();
                const bool isJoin = opName.Contains("Join");
                const bool isGrace = opName.Contains("Grace") ||
                    (op.contains("JoinAlgo") && op.at("JoinAlgo").GetStringSafe() == "Grace");
                if (isJoin && isGrace) {
                    return true;
                }
            }
        }

        return false;
    }

    ui32 CountGraceJoinPlanNodes(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return 0;
        }

        ui32 count = IsGraceJoinPlanNode(planNode) ? 1 : 0;

        const auto& planMap = planNode.GetMapSafe();
        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                count += CountGraceJoinPlanNodes(child);
            }
        }

        return count;
    }

    ui32 CountGraceJoinPlanNodes(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);
        return CountGraceJoinPlanNodes(planRoot.GetMapSafe().at("SimplifiedPlan"));
    }

    bool HasGraceJoinWithBothInputsHashShuffled(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (IsGraceJoinPlanNode(planNode)) {
            if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
                const auto& children = plans->second.GetArraySafe();
                if (children.size() == 2 && IsHashShufflePlanNode(children[0]) && IsHashShufflePlanNode(children[1])) {
                    return true;
                }
            }
        }

        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                if (HasGraceJoinWithBothInputsHashShuffled(child)) {
                    return true;
                }
            }
        }

        return false;
    }

    bool HasGraceJoinWithBothInputsHashShuffled(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);
        return HasGraceJoinWithBothInputsHashShuffled(planRoot.GetMapSafe().at("SimplifiedPlan"));
    }

    NKikimrKqp::TKqpSetting MakeHashCompatibilityStatsSetting(const TVector<TString>& tables) {
        TStringBuilder stats;
        stats << "{";
        for (size_t i = 0; i < tables.size(); ++i) {
            if (i) {
                stats << ",";
            }
            stats << "\"/Root/" << tables[i] << "\": {\"n_rows\": 1000000, \"byte_size\": 16000000}";
        }
        stats << "}";

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(stats);
        return statsSetting;
    }

    void CreateHashCompatibilityTables(TSession& tableSession, const TVector<TString>& tables) {
        for (const auto& table : tables) {
            auto result = tableSession.ExecuteSchemeQuery(Sprintf(R"(
                CREATE TABLE `/Root/%s` (
                    id Int32 NOT NULL,
                    k Int32,
                    payload Int32,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )", table.c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    std::pair<TString, TString> ExplainHashCompatibilityQueryWithAst(const TVector<TString>& tables, const TString& query, bool blockChannelsAuto = false) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);
        if (blockChannelsAuto) {
            appConfig.MutableTableServiceConfig()->SetBlockChannelsMode(
                NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);
        }

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({MakeHashCompatibilityStatsSetting(tables)});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateHashCompatibilityTables(tableSession, tables);

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        return {TString{*result.GetStats()->GetPlan()}, TString{*result.GetStats()->GetAst()}};
    }

    TString ExplainHashCompatibilityQuery(const TVector<TString>& tables, const TString& query) {
        return ExplainHashCompatibilityQueryWithAst(tables, query).first;
    }

    // A flat 3-way join on TPCH tables with overridden statistics and fixed
    // join order & type to only test SE, not anything around it
    Y_UNIT_TEST(ShuffleEliminationSimpleJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows":  150000, "byte_size":  15000000},
            "/Root/orders":   {"n_rows": 1500000, "byte_size": 150000000},
            "/Root/lineitem": {"n_rows": 6000000, "byte_size": 600000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(session, "schema/tpch.sql", /*useColumnStore*/ true);

        // Fix the order to only test shuffle elimination, not the join order.
        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = 'JoinOrder((l o) c)';

            SELECT c.c_custkey, o.o_orderkey, l.l_linenumber
            FROM `/Root/customer` c
            JOIN `/Root/orders` o ON c.c_custkey = o.o_custkey
            JOIN `/Root/lineitem` l ON o.o_orderkey = l.l_orderkey
        )";

        auto queryDb = kikimr.GetQueryClient();
        auto querySession = queryDb.GetSession().GetValueSync().GetSession();

        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashShuffles.size(), 2u, plan);
        const bool hasLineitemShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(l.l_orderkey)");
            });
        const bool hasOrdersShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(o.o_custkey)");
            });
        const bool hasCustomerShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(c.c_custkey)");
            });

        UNIT_ASSERT_C(
            hasLineitemShuffle && hasOrdersShuffle && !hasCustomerShuffle,
            TStringBuilder() << "Expected only lineitem and orders-side shuffles, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationTPCHQ5CompositeJoinKeys) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows": 150000, "byte_size": 16117888},
            "/Root/orders": {"n_rows": 1500000, "byte_size": 92638032},
            "/Root/lineitem": {"n_rows": 6001215, "byte_size": 409400000},
            "/Root/supplier": {"n_rows": 10000, "byte_size": 1098296},
            "/Root/nation": {"n_rows": 25, "byte_size": 2424},
            "/Root/region": {"n_rows": 5, "byte_size": 1008}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, "data/", "schema/tpch.sql", /*useColumnStore*/ true);

        TString query = GetFullPath("data/yql-tpch/q", "5.yql");
        const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        query = toDecimal + "\n" + toDecimalMax + "\n" +
            R"(PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
)" + query;

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, false, Cout, 0);
        queryPlanPrinter.Print(plan);

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        const bool hasCustomerCompositeShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("c_custkey") && desc.Contains("c_nationkey");
            }
        );

        const bool hasFinalRightShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("o_custkey") && !desc.Contains("c_nationkey") && !desc.Contains("s_nationkey");
            }
        );

        UNIT_ASSERT_C(
            !hasCustomerCompositeShuffle && hasFinalRightShuffle,
            TStringBuilder() << "Expected DPHyp shuffle requirements to be preserved: customer side eliminated, "
                             << "right side shuffled by the enumerated orders key. Got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationSimpleJoinKeysBothSides) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows": 150000, "byte_size": 16117888},
            "/Root/orders": {"n_rows": 1500000, "byte_size": 92638032}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, "data/", "schema/tpch.sql", /*useColumnStore*/ true);

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(c o Shuffle)
                JoinOrder(c o)
            ';

            SELECT c.c_custkey, o.o_orderkey
            FROM `/Root/customer` AS c
            JOIN `/Root/orders` AS o
                ON c.c_nationkey = o.o_custkey
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, true, Cout, 0);
        queryPlanPrinter.Print(plan);

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(CountGraceJoinPlanNodes(plan), 1u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(hashShuffles.size(), 2u, plan);

        const bool hasCustomerShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(c.c_nationkey)");
            });
        const bool hasOrdersShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(o.o_custkey)");
            });

        UNIT_ASSERT_C(
            HasGraceJoinWithBothInputsHashShuffled(plan),
            TStringBuilder() << "Expected a GraceJoin with HashShuffle on both inputs, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
        UNIT_ASSERT_C(
            hasCustomerShuffle && hasOrdersShuffle,
            TStringBuilder() << "Expected both simple join sides to be reshuffled, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    // Minimal HashV2 compatibility regression.
    // Regression test for hash-function compatibility across two GraceJoins:
    // the second join may reuse the first join's output shuffling, so the
    // remaining input must be shuffled with the same hash function.
    Y_UNIT_TEST(ShuffleEliminationTwoJoinsHashFuncCompatibility) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/a": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/b": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/c": {"n_rows": 1000000, "byte_size": 16000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        for (const TString& table : {"a", "b", "c"}) {
            auto result = tableSession.ExecuteSchemeQuery(Sprintf(R"(
                CREATE TABLE `/Root/%s` (
                    id Int32 NOT NULL,
                    k Int32,
                    payload Int32,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )", table.c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinOrder((a b) c)
            ';

            SELECT a.k, b.payload, c.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.k = b.k
            JOIN `/Root/c` AS c ON a.k = c.k
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        // First join shuffles both sides with the default hash. The second join
        // should reuse that shuffling on the left and shuffle the right side
        // with the same hash function, not ColumnShardHashV1.
        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 3u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("ColumnShardHashV1")),
            0,
            TStringBuilder() << "Hash shuffles use incompatible functions: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // Minimal ColumnShardHashV1 preserved-partitioning case.
    Y_UNIT_TEST(ShuffleEliminationSingleJoinColumnShardHashCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinOrder(a b)
            ';

            SELECT a.id, b.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 1u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            hashFuncs.front(),
            TString("ColumnShardHashV1"),
            TStringBuilder() << "The remaining shuffle must match the preserved source hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationColumnShardHashPreservedInPhysicalAst) {
        const auto [plan, ast] = ExplainHashCompatibilityQueryWithAst({"a", "b"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinOrder(a b)
            ';

            SELECT a.id, b.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
        )", /*blockChannelsAuto=*/true);

        UNIT_ASSERT_C(
            HasPhysicalHashShuffleWithHashFunc(ast, "ColumnShardHashV1"),
            TStringBuilder() << "Expected a physical hash shuffle to preserve ColumnShardHashV1\n"
                             << plan << "\n" << ast);
    }

    Y_UNIT_TEST(ShuffleEliminationCompositeSourceSubsetKeyExecute) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/a": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/b": {"n_rows": 1000000, "byte_size": 24000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/a` (
                id Int64 NOT NULL,
                payload Int64 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/b` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                payload Int64 NOT NULL,
                PRIMARY KEY (id, k)
            )
            PARTITION BY HASH(id, k)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (ui64 id : {1, 2, 3}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("payload").Int64(id * 10)
                    .EndStruct();
            }
            rows.EndList();

            auto upsert = tableClient.BulkUpsert("/Root/a", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("k").Int64(10)
                .AddMember("payload").Int64(101)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("k").Int64(11)
                .AddMember("payload").Int64(102)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(2)
                .AddMember("k").Int64(20)
                .AddMember("payload").Int64(200)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(4)
                .AddMember("k").Int64(40)
                .AddMember("payload").Int64(400)
                .EndStruct();
            rows.EndList();

            auto upsert = tableClient.BulkUpsert("/Root/b", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(b a Shuffle)
                JoinOrder(b a)
            ';

            SELECT b.id, b.k, b.payload
            FROM `/Root/b` AS b
            JOIN `/Root/a` AS a ON b.id = a.id
            ORDER BY b.id, b.k
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explain = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());

        auto execute = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();

        UNIT_ASSERT_C(execute.IsSuccess(), execute.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(execute.GetResultSet(0)), R"([[1;10;101];[1;11;102];[2;20;200]])");
    }

    // All-ColumnShardHashV1 chain: no accidental HashV2 transition.
    Y_UNIT_TEST(ShuffleEliminationThreeJoinsColumnShardHashCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "d"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c d Shuffle)
                JoinOrder(((a b) c) d)
            ';

            SELECT a.id, b.payload, c.payload, d.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
            JOIN `/Root/c` AS c ON a.id = c.k
            JOIN `/Root/d` AS d ON a.id = d.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 3u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("ColumnShardHashV1")),
            3,
            TStringBuilder() << "All shuffles must match the preserved source hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // All-HashV2 chain: no accidental ColumnShardHashV1 propagation.
    Y_UNIT_TEST(ShuffleEliminationThreeJoinsHashFuncCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "d"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c d Shuffle)
                JoinOrder(((a b) c) d)
            ';

            SELECT a.k, b.payload, c.payload, d.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.k = b.k
            JOIN `/Root/c` AS c ON a.k = c.k
            JOIN `/Root/d` AS d ON a.k = d.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 4u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("HashV2")),
            4,
            TStringBuilder() << "All shuffles must stay compatible with the first join hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // General mixed case: both hash domains and transitions in one plan.
    Y_UNIT_TEST(ShuffleEliminationMixedHashFuncCompatibility) {
        // When both final join inputs are already shuffled, DPHyp still
        // has to shuffle one due to runtime limitations and tie breaks
        // based on statistics - so we pin those too.
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "i", "d", "e", "f", "g", "h"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c i Shuffle)
                JoinType(a b c i d Shuffle)
                JoinType(e f Shuffle)
                JoinType(e f g Shuffle)
                JoinType(e f g h Shuffle)
                JoinType(a b c i d e f g h Shuffle)
                JoinOrder(((((a b) c) i) d) (((e f) g) h))
            ';

            SELECT a.id, b.payload, c.payload, i.payload, d.payload, e.id, f.payload, g.payload, h.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
            JOIN `/Root/c` AS c ON a.id = c.k
            JOIN `/Root/i` AS i ON a.id = i.k
            JOIN `/Root/d` AS d ON a.k = d.k
            JOIN `/Root/e` AS e ON a.k = e.k
            JOIN `/Root/f` AS f ON e.id = f.k
            JOIN `/Root/g` AS g ON e.k = g.k
            JOIN `/Root/h` AS h ON e.k = h.k
        )");

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);
        // Preorder traversal: the left subtree keeps ColumnShard-preserved
        // id=k joins for several levels, then switches to default HashV2 k=k joins.
        // The right subtree also switches to HashV2 on e.k. At the final join both
        // sides have compatible one-column HashV2 partitioning, so DPHyp must
        // redundantly reshuffle exactly one side. The left subtree (a,b,c,i,d) is
        // larger than the right (e,f,g,h), so CBO preserves the larger left subtree
        // and reshuffles the smaller right subtree on e.k.
        const TVector<TString> expectedHashShuffles = {
            "HashV2(a.k)",
            "ColumnShardHashV1(b.k)",
            "ColumnShardHashV1(c.k)",
            "ColumnShardHashV1(i.k)",
            "HashV2(d.k)",
            "HashV2(e.k)",
            "HashV2(e.k)",
            "ColumnShardHashV1(f.k)",
            "HashV2(g.k)",
            "HashV2(h.k)",
        };

        UNIT_ASSERT_VALUES_EQUAL_C(
            SortDescriptions(hashShuffles),
            SortDescriptions(expectedHashShuffles),
            TStringBuilder() << "Unexpected mixed hash propagation plan: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    /*
    void InsertIntoAliasesRenames(NYdb::NTable::TTableClient &db, std::string tableName, int numRows) {
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < numRows; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(i)
                .AddMember("join_id").Int64(i + 1)
                .AddMember("c").Int64(i + 2)
                .EndStruct();
        }
        rows.EndList();
        auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
    }

    void AliasesRenamesTest(bool newRbo) {
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
            CREATE TABLE `/Root/foo_0` (
                id Int64 NOT NULL,
                join_id Int64 NOT NULL,
                c Int64,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/foo_1` (
                id Int64	NOT NULL,
                join_id Int64 NOT NULL,
                c Int64,
                primary key(id)
            ) with (Store = Column);

            CREATE TABLE `/Root/foo_2` (
                id Int64 NOT NULL,
                join_id Int64 NOT NULL,
                c Int64,
                primary key(id)
            ) with (Store = Column);

        )").GetValueSync();

        std::vector<std::pair<std::string, int>> tables{{"/Root/foo_0", 4}, {"/Root/foo_1", 3}, {"/Root/foo_2", 2}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoAliasesRenames(db, table, rowsNum);
        }
        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";

            WITH cte as (
                SELECT a1.id2, join_id FROM (SELECT id as "id2", join_id FROM foo_0) as a1)

            SELECT X1.id2, X2.id2
            FROM
               (SELECT id2
               FROM foo_1, cte
               WHERE foo_1.join_id = cte.join_id) as X1,

               (SELECT id2
               FROM foo_2, cte
               WHERE foo_2.join_id = cte.join_id) as X2;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([["0";"0"];["0";"1"];["1";"0"];["1";"1"];["2";"0"];["2";"1"]])");
    }

    Y_UNIT_TEST(AliasesRenames) {
        AliasesRenamesTest(true);
        AliasesRenamesTest(false);
    }

    Y_UNIT_TEST(PredicatePushdownLeftJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableT1;
        rowsTableT1.BeginList();
        for (size_t i = 0; i < 2; ++i) {
            rowsTableT1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").String(std::to_string(i) + "_b")
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableT2;
        rowsTableT2.BeginList();
        for (size_t i = 0; i < 1; ++i) {
            rowsTableT2.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").String(std::to_string(i) + "_b")
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT2.EndList();

        resultUpsert = db.BulkUpsert("/Root/t2", rowsTableT2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t1.a = 0;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t2.b = 'some_string';
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t2.b IS NULL;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;0]])",
            R"([])",
            R"([[1]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(UnionAll) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t3` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t4` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}, {"/Root/t3", 2}, {"/Root/t4", 1}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`
                UNION ALL
                SELECT t3.a FROM `/Root/t3`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`
                UNION ALL
                SELECT t3.a FROM `/Root/t3`
                UNION ALL
                SELECT t4.a FROM `/Root/t4`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` inner join `/Root/t2` on t1.a = t2.a where t1.a > 1
                UNION ALL
                SELECT t3.a FROM `/Root/t3` where t3.a = 1;
            )",
        };

        std::vector<std::string> results = {
            R"([[0];[1];[2];[3];[0];[1];[2]])",
            R"([[0];[1];[2];[3];[0];[1];[2];[0];[1]])",
            R"([[0];[1];[2];[3];[0];[1];[2];[0];[1];[0]])",
            R"([[2];[1]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(Bench_Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto time = TimeQuery(kikimr, R"(
                --!syntax_pg
                SELECT 1 as "a", 2 as "b";
            )", 10);

        Cout << "Time per query: " << time;

        //UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Bench_Filter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT id as "id2" FROM foo WHERE name = 'some_name';
        )",10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_CrossFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_JoinFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE f.id = bar.id and name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_10Joins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schema = R"(
CREATE TABLE `/Root/foo_0` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_1` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_2` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_3` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_4` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_5` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_6` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_7` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_8` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_9` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    )";

        auto query = R"(
            --!syntax_pg
     SET TablePathPrefix = "/Root/";

     SELECT foo_0.id as "id2"
     FROM foo_0, foo_1, foo_2, foo_3, foo_4, foo_5, foo_6, foo_7, foo_8, foo_9
     WHERE foo_0.join_id = foo_1.id AND foo_0.join_id = foo_2.id AND foo_0.join_id = foo_3.id AND foo_0.join_id = foo_4.id AND foo_0.join_id = foo_5.id AND
foo_0.join_id = foo_6.id AND foo_0.join_id = foo_7.id AND foo_0.join_id = foo_8.id AND foo_0.join_id = foo_9.id;

    )";

        auto time = TimeQuery(schema, query, 10);

        Cout << "Time per query: " << time;
    }

    */

    // Regression: Aggregate::ComputeMetadata was copying input metadata instead of starting
    // fresh, leaking its KeyColumns into the input Map. Repros with TPCH Q21 pattern.
    Y_UNIT_TEST(AggregateKeyColumnsNotLeakedToInputMap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/items` (
                order_id Int64 NOT NULL,
                line_id  Int64 NOT NULL,
                sup_id   Int64 NOT NULL,
                late     Int64 NOT NULL,
                PRIMARY KEY (order_id, line_id)
            ) WITH (Store = Column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            SELECT i1.sup_id, COUNT(*) AS cnt
            FROM `/Root/items` AS i1
            WHERE i1.late = 1
              AND EXISTS (
                  SELECT * FROM `/Root/items` AS i2
                  WHERE i2.order_id = i1.order_id AND i2.sup_id != i1.sup_id
              )
              AND NOT EXISTS (
                  SELECT * FROM `/Root/items` AS i3
                  WHERE i3.order_id = i1.order_id AND i3.sup_id != i1.sup_id AND i3.late = 1
              )
            GROUP BY i1.sup_id
            ORDER BY cnt DESC, i1.sup_id;
        )", NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(UnionAll) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
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
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}, {"/Root/t3", 2}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a FROM `/Root/t2` as t2
                UNION ALL
                SELECT t3.a as a FROM `/Root/t3` as t3
                ORDER BY a;
            )",
             R"(
                SELECT t1.a as a, t1.c as c FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a, Cast(99 as Int64) as c FROM `/Root/t2` as t2
                ORDER BY a, c;
            )",
        };

        const std::vector<std::string> results = {
            R"([[0];[0];[1];[1];[2];[2];[3]])",
            R"([[0];[0];[0];[1];[1];[1];[2];[2];[3]])",
            R"([[0;[1]];[0;[99]];[1;[2]];[1;[99]];[2;[3]];[2;[99]];[3;[4]]])",
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

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST(Rollup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema1(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT count(t1.a), t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT count(t1.b), t1.a as a FROM `/Root/t1` as t1
                group by rollup(t1.a)
                order by a;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, t1.d as d FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c, t1.d)
                order by b, c, d;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, t1.d as d, t1.e as e FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c, t1.d, t1.e)
                order by b, c, d, e;
            )",
            R"(
                SELECT sum(t1.b), min(t1.b), max(t1.b), avg(t1.b), t1.a as a, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.a, t1.c)
                order by a, c;
            )",
            R"(
                SELECT count(t1.a + 1) + 2, t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
        };

        const std::vector<std::string> results = {
            R"([[4u;#];[1u;[1]];[1u;[2]];[1u;[3]];[1u;[4]]])",
            R"([[4u;#];[1u;[0]];[1u;[1]];[1u;[2]];[1u;[3]]])",
            R"([[4u;#;#];[1u;[1];#];[1u;[1];[2]];[1u;[2];#];[1u;[2];[3]];[1u;[3];#];[1u;[3];[4]];[1u;[4];#];[1u;[4];[5]]])",
            R"([[4u;#;#;#];[1u;[1];#;#];[1u;[1];[2];#];[1u;[1];[2];[3]];[1u;[2];#;#];[1u;[2];[3];#];[1u;[2];[3];[4]];[1u;[3];#;#];[1u;[3];[4];#];[1u;[3];[4];[5]];[1u;[4];#;#];[1u;[4];[5];#];[1u;[4];[5];[6]]])",
            R"([[4u;#;#;#;#];[1u;[1];#;#;#];[1u;[1];[2];#;#];[1u;[1];[2];[3];#];[1u;[1];[2];[3];[4]];[1u;[2];#;#;#];[1u;[2];[3];#;#];[1u;[2];[3];[4];#];[1u;[2];[3];[4];[5]];[1u;[3];#;#;#];[1u;[3];[4];#;#];[1u;[3];[4];[5];#];[1u;[3];[4];[5];[6]];[1u;[4];#;#;#];[1u;[4];[5];#;#];[1u;[4];[5];[6];#];[1u;[4];[5];[6];[7]]])",
            R"([[[10];[1];[4];[2.5];#;#];[[1];[1];[1];[1.];[0];#];[[1];[1];[1];[1.];[0];[2]];[[2];[2];[2];[2.];[1];#];[[2];[2];[2];[2.];[1];[3]];[[3];[3];[3];[3.];[2];#];[[3];[3];[3];[3.];[2];[4]];[[4];[4];[4];[4.];[3];#];[[4];[4];[4];[4.];[3];[5]]])",
            R"([[6u;#];[3u;[1]];[3u;[2]];[3u;[3]];[3u;[4]]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

}

} // namespace NKqp
} // namespace NKikimr
