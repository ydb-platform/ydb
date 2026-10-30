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
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

bool GetBoolField(const NJson::TJsonValue& node, const TString& fieldName) {
    const auto& map = node.GetMapSafe();
    const auto field = map.find(fieldName);
    UNIT_ASSERT_C(field != map.end() && field->second.IsBoolean(), fieldName);
    return field->second.GetBoolean();
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

void CollectOperatorIds(const NJson::TJsonValue& planNode, THashSet<i64>& operatorIds) {
    if (!planNode.IsMap()) {
        return;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& operatorNode : operators->second.GetArraySafe()) {
            const auto& operatorMap = operatorNode.GetMapSafe();
            if (auto operatorId = operatorMap.find("OperatorId"); operatorId != operatorMap.end()) {
                operatorIds.insert(operatorId->second.GetIntegerSafe());
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            CollectOperatorIds(child, operatorIds);
        }
    }
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

NJson::TJsonValue GetRboAnalyzeSimplifiedPlan(const TString& txPlan) {
    NKqpProto::TKqpStatsQuery queryStats;
    return GetSimplifiedPlan(SerializeRBOAnalyzePlan(TVector<const TString>{txPlan}, queryStats));
}

const NJson::TJsonValue& FindRequiredOperatorByStringField(
    const NJson::TJsonValue& plan,
    const TString& fieldName,
    const TString& fieldValue)
{
    const auto* op = FindOperatorByStringField(plan, fieldName, fieldValue);
    UNIT_ASSERT_C(op, plan);
    return *op;
}

void AssertRboCpuValues(
    const NJson::TJsonValue& op,
    double expectedSelfCpu,
    double expectedCpu,
    const NJson::TJsonValue& plan)
{
    UNIT_ASSERT_VALUES_EQUAL_C(op.GetMapSafe().at("A-SelfCpu").GetDoubleSafe(), expectedSelfCpu, plan);
    UNIT_ASSERT_VALUES_EQUAL_C(op.GetMapSafe().at("A-Cpu").GetDoubleSafe(), expectedCpu, plan);
}

void AssertNoRboCpuValues(const NJson::TJsonValue& op, const NJson::TJsonValue& plan) {
    UNIT_ASSERT_C(!op.GetMapSafe().contains("A-SelfCpu"), plan);
    UNIT_ASSERT_C(!op.GetMapSafe().contains("A-Cpu"), plan);
}

void BulkUpsertExplainPlanTestRows(TKikimrRunner& kikimr) {
    auto db = kikimr.GetTableClient();

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (i64 i = 1; i <= 12; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i * 10)
            .AddMember("c").Int64(i * 100)
            .EndStruct();
    }
    rows.EndList();

    auto result = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateOriginalRowsHintTables(TKikimrRunner& kikimr) {
    auto db = kikimr.GetTableClient();
    auto sessionResult = db.CreateSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();
    auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/R` (
            id Int64 NOT NULL,
            primary key(id)
        ) WITH (STORE = column);

        CREATE TABLE `/Root/S` (
            id Int64 NOT NULL,
            primary key(id)
        ) WITH (STORE = column);

        CREATE TABLE `/Root/T` (
            id Int64 NOT NULL,
            primary key(id)
        ) WITH (STORE = column);
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

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

        const auto* hashShuffle = FindConnectionNode(simplifiedPlan, "HashShuffle");
        UNIT_ASSERT_C(hashShuffle, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*hashShuffle, "Node Type"), "HashShuffle", plan);
        UNIT_ASSERT_C(!GetStringField(*hashShuffle, "HashFunc").empty(), plan);
        const auto& hashShuffleMap = hashShuffle->GetMapSafe();
        UNIT_ASSERT_C(!hashShuffleMap.contains("OperatorId"), plan);
        UNIT_ASSERT_C(!hashShuffleMap.contains("Operators"), plan);
        UNIT_ASSERT_C(hashShuffleMap.contains("KeyColumns") && hashShuffleMap.at("KeyColumns").IsArray(), plan);
        UNIT_ASSERT_C(!hashShuffleMap.at("KeyColumns").GetArraySafe().empty(), plan);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
        const auto* executionHashShuffle = FindConnectionNode(planJson.GetMapSafe().at("Plan"), "HashShuffle");
        UNIT_ASSERT_C(executionHashShuffle, plan);
        UNIT_ASSERT_C(executionHashShuffle->GetMapSafe().contains("KeyColumns"), plan);
        UNIT_ASSERT_C(executionHashShuffle->GetMapSafe().contains("HashFunc"), plan);
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

    Y_UNIT_TEST(ExplainReadRowsHints) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.OptimizerHints = '
                Rows(left_alias # 123)
                Rows(t2 # 456)
            ';
            select left_alias.a, right_alias.b
            from `/Root/t1` as left_alias
            inner join `/Root/t2` as right_alias on left_alias.a = right_alias.b;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* leftRead = FindOperatorByStringField(simplifiedPlan, "Table", "t1");
        const auto* rightRead = FindOperatorByStringField(simplifiedPlan, "Table", "t2");

        UNIT_ASSERT_C(leftRead, plan);
        UNIT_ASSERT_C(rightRead, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*leftRead, "E-Rows"), "123", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*rightRead, "E-Rows"), "456", plan);
    }

    Y_UNIT_TEST(ExplainOriginalRowsHints) {
        TExplainPlanTestContext testContext;
        CreateOriginalRowsHintTables(testContext.GetKikimr());
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.OptimizerHints =
            '
                Rows(R # 20e8)
                Rows(T # 777)
                Rows(S # 30e8)
                Rows(R T # 1)
                Rows(R S # 10e8)
            ';
            SELECT * FROM
                `/Root/R` AS R INNER JOIN `/Root/S` AS S on R.id = S.id
                    INNER JOIN `/Root/T` AS T on R.id = T.id;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* readR = FindOperatorByStringField(simplifiedPlan, "Table", "R");
        const auto* readS = FindOperatorByStringField(simplifiedPlan, "Table", "S");
        const auto* readT = FindOperatorByStringField(simplifiedPlan, "Table", "T");

        UNIT_ASSERT_C(readR, plan);
        UNIT_ASSERT_C(readS, plan);
        UNIT_ASSERT_C(readT, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readR, "E-Rows"), "2000000000", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readS, "E-Rows"), "3000000000", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readT, "E-Rows"), "777", plan);
    }

    Y_UNIT_TEST(ExplainOriginalRowsHintsOldRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        CreateOriginalRowsHintTables(kikimr);
        auto session = CreateQuerySession(kikimr);
        auto plan = ExecuteExplain(session, R"(
            PRAGMA ydb.OptimizerHints =
            '
                Rows(R # 20e8)
                Rows(T # 777)
                Rows(S # 30e8)
                Rows(R T # 1)
                Rows(R S # 10e8)
            ';
            SELECT * FROM
                `/Root/R` AS R INNER JOIN `/Root/S` AS S on R.id = S.id
                    INNER JOIN `/Root/T` AS T on R.id = T.id;
        )");

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* readR = FindOperatorByStringField(simplifiedPlan, "Table", "R");
        const auto* readS = FindOperatorByStringField(simplifiedPlan, "Table", "S");
        const auto* readT = FindOperatorByStringField(simplifiedPlan, "Table", "T");

        UNIT_ASSERT_C(readR, plan);
        UNIT_ASSERT_C(readS, plan);
        UNIT_ASSERT_C(readT, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readR, "E-Rows"), "2000000000", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readS, "E-Rows"), "3000000000", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readT, "E-Rows"), "777", plan);
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
        UNIT_ASSERT_C(topSortBy.Contains("t1.a desc nulls first"), sortPlan);
        UNIT_ASSERT_C(topSortBy.Contains("t1.b asc nulls first"), sortPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*topSortOp, "Limit"), "5", sortPlan);

        const auto* mergeConnection = FindConnectionNode(simplifiedSortPlan, "Merge");
        UNIT_ASSERT_C(mergeConnection, sortPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*mergeConnection, "Node Type"), "Merge", sortPlan);
        const auto mergeSortBy = GetStringField(*mergeConnection, "SortBy");
        UNIT_ASSERT_C(mergeSortBy.Contains("t1.a desc nulls first"), sortPlan);
        UNIT_ASSERT_C(mergeSortBy.Contains("t1.b asc nulls first"), sortPlan);
        UNIT_ASSERT_C(mergeConnection->GetMapSafe().contains("SortColumns"), sortPlan);
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

    Y_UNIT_TEST(ExplainRangePushdown) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            SELECT t1.a, t1.b FROM `/Root/t1` AS t1 WHERE t1.a > 5;
        )");
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* readOp = FindOperatorByStringField(simplifiedPlan, "Name", "TableRangeScan");
        UNIT_ASSERT_C(readOp, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*readOp, "Table"), "t1", plan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadRangesKeys", "a"), plan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "a (5, +∞)"), plan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "b"), plan);
        UNIT_ASSERT_C(!readOp->GetMapSafe().contains("Predicate"), plan);
        UNIT_ASSERT_C(!readOp->GetMapSafe().contains("ReadRangesPointPrefixLen"), plan);

        auto disjointPlan = ExecuteExplain(session, R"(
            PRAGMA YqlSelect = 'force';
            SELECT t1.a, t1.b FROM `/Root/t1` AS t1 WHERE t1.a < 5 OR t1.a >= 10;
        )");
        const auto simplifiedDisjointPlan = GetSimplifiedPlan(disjointPlan);
        const auto* disjointReadOp = FindOperatorByStringField(simplifiedDisjointPlan, "Name", "TableRangeScan");
        UNIT_ASSERT_C(disjointReadOp, disjointPlan);
        UNIT_ASSERT_C(StringArrayFieldContains(*disjointReadOp, "ReadColumns", "a (-∞, 5)"), disjointPlan);
        UNIT_ASSERT_C(StringArrayFieldContains(*disjointReadOp, "ReadColumns", "a [10, +∞)"), disjointPlan);
        UNIT_ASSERT_C(StringArrayFieldContains(*disjointReadOp, "ReadColumns", "b"), disjointPlan);
        UNIT_ASSERT_C(!disjointReadOp->GetMapSafe().contains("Predicate"), disjointPlan);
        UNIT_ASSERT_C(!disjointReadOp->GetMapSafe().contains("ReadRangesPointPrefixLen"), disjointPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeSimplifiedPlanCpuWithActualRows) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "TableFullScan", "StageGuid": "stage-1",
                "Operators": [{"Name": "TableFullScan", "OperatorId": 1}],
                "Stats": {
                    "Table": [{"Path": "/Root/t1", "ReadRows": {"Sum": 6}, "ReadBytes": {"Sum": 64}}],
                    "CpuTimeUs": {"Max": 7000}
                }
            }],
            "SimplifiedPlan": {
                "Node Type": "TableFullScan",
                "Operators": [{"Name": "TableFullScan", "OperatorId": 1, "Table": "/Root/t1"}]
            }
        })";

        const auto simplifiedPlan = GetRboAnalyzeSimplifiedPlan(txPlan);
        const auto& fullScan = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL_C(fullScan.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, simplifiedPlan);
        AssertRboCpuValues(fullScan, 7, 7, simplifiedPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeDuplicateOperatorIdUsesFirstMatch) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "FirstExec", "StageGuid": "stage-first",
                "Operators": [{"Name": "FirstExec", "OperatorId": 7}],
                "Stats": {"OutputRows": {"Sum": 6}, "OutputBytes": {"Sum": 60}, "CpuTimeUs": {"Max": 7000}},
                "Plans": [{
                    "Node Type": "SecondExec", "StageGuid": "stage-second",
                    "Operators": [{"Name": "SecondExec", "OperatorId": 7}],
                    "Stats": {"OutputRows": {"Sum": 11}, "OutputBytes": {"Sum": 110}, "CpuTimeUs": {"Max": 11000}}
                }]
            }],
            "SimplifiedPlan": {
                "Node Type": "First", "Operators": [{"Name": "First", "OperatorId": 7}],
                "Plans": [{
                    "Node Type": "Second", "Operators": [{"Name": "Second", "OperatorId": 7}]
                }]
            }
        })";

        const auto simplifiedPlan = GetRboAnalyzeSimplifiedPlan(txPlan);
        const auto& first = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "First");
        const auto& second = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "Second");

        UNIT_ASSERT_VALUES_EQUAL_C(first.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, simplifiedPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(first.GetMapSafe().at("A-Size").GetDoubleSafe(), 60, simplifiedPlan);
        AssertRboCpuValues(first, 7, 7, simplifiedPlan);
        UNIT_ASSERT_C(!second.GetMapSafe().contains("A-Rows"), simplifiedPlan);
        UNIT_ASSERT_C(!second.GetMapSafe().contains("A-Size"), simplifiedPlan);
        AssertNoRboCpuValues(second, simplifiedPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeMissingExecutionOperatorIdFails) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "Exec", "StageGuid": "stage-1",
                "Operators": [{"Name": "Exec", "OperatorId": 1}]
            }],
            "SimplifiedPlan": {
                "Node Type": "Missing", "Operators": [{"Name": "Missing", "OperatorId": 2}]
            }
        })";

        NKqpProto::TKqpStatsQuery queryStats;
        const TVector<const TString> txPlans = {txPlan};
        UNIT_ASSERT_EXCEPTION(SerializeRBOAnalyzePlan(txPlans, queryStats), yexception);
    }

    Y_UNIT_TEST(ExplainAnalyzeBroadcastStatsUseParentTaskCount) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "Broadcast", "PlanNodeType": "Connection",
                "Stats": {"Tasks": 4},
                "Plans": [{
                    "Node Type": "Scan", "StageGuid": "stage-1",
                    "Operators": [{"Name": "Scan", "OperatorId": 1}],
                    "Stats": {"OutputRows": {"Sum": 8}, "OutputBytes": {"Sum": 80}, "CpuTimeUs": {"Max": 7000}}
                }]
            }],
            "SimplifiedPlan": {
                "Node Type": "Scan", "Operators": [{"Name": "Scan", "OperatorId": 1}]
            }
        })";

        const auto simplifiedPlan = GetRboAnalyzeSimplifiedPlan(txPlan);
        const auto& scan = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "Scan");

        UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-Rows").GetDoubleSafe(), 2, simplifiedPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-Size").GetDoubleSafe(), 20, simplifiedPlan);
        UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-SelfCpu").GetDoubleSafe(), 7, simplifiedPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeMultiOperatorCpuUsesTopOperator) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "Stage", "StageGuid": "stage-1",
                "Operators": [
                    {"Name": "Top", "OperatorId": 1},
                    {"Name": "Inner", "OperatorId": 2}
                ],
                "Stats": {"CpuTimeUs": {"Max": 7000}}
            }],
            "SimplifiedPlan": {
                "Node Type": "Top", "Operators": [{"Name": "Top", "OperatorId": 1}],
                "Plans": [{
                    "Node Type": "Inner", "Operators": [{"Name": "Inner", "OperatorId": 2}]
                }]
            }
        })";

        const auto simplifiedPlan = GetRboAnalyzeSimplifiedPlan(txPlan);
        const auto& top = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "Top");
        const auto& inner = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "Inner");

        AssertRboCpuValues(top, 7, 7, simplifiedPlan);
        AssertNoRboCpuValues(inner, simplifiedPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeCpuAbsentDoesNotAddCpuFields) {
        const TString txPlan = R"({
            "Plans": [{
                "Node Type": "Scan", "StageGuid": "stage-1",
                "Operators": [{"Name": "Scan", "OperatorId": 1}],
                "Stats": {"OutputRows": {"Sum": 6}}
            }],
            "SimplifiedPlan": {
                "Node Type": "Scan", "Operators": [{"Name": "Scan", "OperatorId": 1}]
            }
        })";

        const auto simplifiedPlan = GetRboAnalyzeSimplifiedPlan(txPlan);
        const auto& scan = FindRequiredOperatorByStringField(simplifiedPlan, "Name", "Scan");

        UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, simplifiedPlan);
        AssertNoRboCpuValues(scan, simplifiedPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeRangePushdown) {
        TExplainPlanTestContext testContext;
        BulkUpsertExplainPlanTestRows(testContext.GetKikimr());
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplainAnalyze(session, R"(
            PRAGMA YqlSelect = 'force';
            SELECT count(*) FROM `/Root/t1` AS t1 WHERE t1.a > 5;
        )");
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* readOp = FindOperatorByStringField(simplifiedPlan, "Name", "TableRangeScan");
        UNIT_ASSERT_C(readOp, plan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadRangesKeys", "a"), plan);
        UNIT_ASSERT_C(StringArrayFieldContains(*readOp, "ReadColumns", "a (5, +∞)"), plan);
        UNIT_ASSERT_C(readOp->GetMapSafe().contains("A-Rows"), plan);
        UNIT_ASSERT_C(readOp->GetMapSafe().at("A-Rows").GetDoubleSafe() > 0, plan);
        UNIT_ASSERT_C(readOp->GetMapSafe().contains("A-Size"), plan);
        UNIT_ASSERT_C(readOp->GetMapSafe().at("A-Size").GetDoubleSafe() > 0, plan);
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
        // The subplan is reduced to a single row by an aggregate that counts its rows along the way, so
        // that a subquery returning several rows fails the query instead of picking one of them.
        UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedScalarSubplanPlan, "Aggregation", ": max("), scalarSubplanPlan);
        UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedScalarSubplanPlan, "Aggregation", ": count("), scalarSubplanPlan);
        UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedScalarSubplanPlan, "Name", "Join"), scalarSubplanPlan);
    }

    Y_UNIT_TEST(ExplainAnalyzeScalarSubquery) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplainAnalyze(session, R"(
            PRAGMA YqlSelect = 'force';
            select t1.a
            from `/Root/t1` as t1
            where t1.a = (select max(t2.a) from `/Root/t2` as t2);
        )");

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
        const auto& planMap = planJson.GetMapSafe();
        const auto& simplifiedPlan = planMap.at("SimplifiedPlan");

        const auto* unionAll = FindConnectionNode(simplifiedPlan, "UnionAll");
        UNIT_ASSERT_C(unionAll, plan);
        UNIT_ASSERT_C(!unionAll->GetMapSafe().contains("OperatorId"), plan);
        UNIT_ASSERT_C(!unionAll->GetMapSafe().contains("Operators"), plan);

        THashSet<i64> executionOperatorIds;
        THashSet<i64> simplifiedOperatorIds;
        CollectOperatorIds(planMap.at("Plan"), executionOperatorIds);
        CollectOperatorIds(simplifiedPlan, simplifiedOperatorIds);
        UNIT_ASSERT_C(executionOperatorIds.empty(), plan);
        UNIT_ASSERT_C(simplifiedOperatorIds.empty(), plan);
    }

    Y_UNIT_TEST(ExplainHidesOperatorIds) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        const auto plan = ExecuteExplain(session, "SELECT a FROM `/Root/t1`;");

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
        const auto& planMap = planJson.GetMapSafe();
        const auto& simplifiedPlan = planMap.at("SimplifiedPlan");
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "TableFullScan"), plan);

        THashSet<i64> operatorIds;
        CollectOperatorIds(planMap.at("Plan"), operatorIds);
        CollectOperatorIds(simplifiedPlan, operatorIds);
        UNIT_ASSERT_C(operatorIds.empty(), plan);
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
        const auto* unionAll = FindConnectionNode(simplifiedConnectionPlan, "UnionAll");
        const auto* broadcast = FindConnectionNode(simplifiedConnectionPlan, "Broadcast");
        UNIT_ASSERT_C(unionAll, connectionPlan);
        UNIT_ASSERT_C(broadcast, connectionPlan);
        UNIT_ASSERT_C(!unionAll->GetMapSafe().contains("OperatorId"), connectionPlan);
        UNIT_ASSERT_C(!unionAll->GetMapSafe().contains("Operators"), connectionPlan);
        UNIT_ASSERT_C(!broadcast->GetMapSafe().contains("OperatorId"), connectionPlan);
        UNIT_ASSERT_C(!broadcast->GetMapSafe().contains("Operators"), connectionPlan);
        UNIT_ASSERT_C(!FindConnectionNode(simplifiedConnectionPlan, "Map"), connectionPlan);
    }

    Y_UNIT_TEST(ExplainMultipleSelect) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplain(session, R"(
            SELECT 1 as x; SELECT 2 as y;
        )");
    }

    Y_UNIT_TEST(ExplainAnalyzeMultipleSelect) {
        TExplainPlanTestContext testContext;
        auto& session = testContext.GetSession();
        auto plan = ExecuteExplainAnalyze(session, R"(
            SELECT * from `/Root/t1`; SELECT * from `/Root/t1`;
        )");
        plan = ExecuteExplainAnalyze(session, R"(
            SELECT 1; SELECT 1;
        )");
    }

    Y_UNIT_TEST_TWIN(ExplainAnalyzeMultipleResultContainers, ConcurrentResults) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings(CreateExplainPlanTestAppConfig()).SetWithSampleTables(false));
        auto session = CreateQuerySession(kikimr);
        // ExecuteQuery enables concurrent results internally. Use streaming to
        // exercise separate query blocks as well as a shared result container.
        auto stream = session.StreamExecuteQuery("SELECT 1 AS x; SELECT 2 AS y, 3 AS z;",
            NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()
                .ConcurrentResultSets(ConcurrentResults).StatsMode(NYdb::NQuery::EStatsMode::Full)).ExtractValueSync();
        UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());

        TMap<ui64, TString> results;
        bool hasPlan = false;
        for (;;) {
            auto part = stream.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }
            if (part.HasResultSet()) {
                results[part.GetResultSetIndex()] += FormatResultSetYson(part.ExtractResultSet());
            }
            if (part.GetStats() && part.GetStats()->GetPlan()) {
                hasPlan = true;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(results.at(0), "[[1]]");
        UNIT_ASSERT_VALUES_EQUAL(results.at(1), "[[2;3]]");
        UNIT_ASSERT(hasPlan);
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
        UNIT_ASSERT_C(mapName == "Map [next_id := /Root/foo.id + 1]", plan);
        const auto predicate = GetStringField(*filterOp, "Predicate");
        UNIT_ASSERT_C(predicate == "/Root/foo.b > 10", plan);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(*limitOp, "Limit"), "3", plan);

        // A unique public name needs no final output-renaming OrderedMap,
        // independently of which physical-stage lowering path is selected.
        for (const bool peephole : {false, true}) {
            const TString query = TStringBuilder()
                << "PRAGMA YqlSelect = 'force';\n"
                << "PRAGMA ydb.EnableNewRBOPhysicalStagePeephole = '" << (peephole ? "true" : "false") << "';\n"
                << "SELECT 1 FROM `/Root/foo` LIMIT 1;";
            const auto result = testContext.GetSession().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const TString smallPlan{*result.GetStats()->GetPlan()};
            UNIT_ASSERT_C(FindOperatorByStringField(GetSimplifiedPlan(smallPlan), "Name", "Map [column0 := 1]"), smallPlan);
            const TString ast{*result.GetStats()->GetAst()};
            UNIT_ASSERT_C(ast.Contains("column0") && !ast.Contains("column0_"), ast);
            UNIT_ASSERT_C(!ast.Contains("OrderedMap"), ast);

            const auto executed = testContext.GetSession().ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(executed.IsSuccess(), executed.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(executed.GetResultSet(0).GetColumnsMeta().at(0).Name, "column0");
        }
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
    }

    Y_UNIT_TEST(ExplainExpressionPrintingJoinFilters) {
        NTests::TIdTestContext f;
        const auto leftKey = f.Id("t1.id"), rightKey = f.Id("t2.id");
        const auto leftValue = f.Id("t1.b"), rightValue = f.Id("t2.c");
        TOpJoin join(f.Read({leftKey, leftValue}), f.Read({rightKey, rightValue}), f.Pos, "Inner",
            {{leftKey, rightKey}}, {MakeBinaryPredicate("<", f.Column(leftValue), f.Column(rightValue))});
        join.Props.JoinAlgo = EJoinAlgoType::GraceJoin;
        join.Props.UseBlockHashJoin = false;
        // Explain is built after display names are frozen, where unique names stay plain.
        f.Props.InfoUnitRegistry.FinalizeDisplayNames({leftKey, rightKey, leftValue, rightValue});
        auto json = join.ToJson(0, f.Props.InfoUnitRegistry);
        UNIT_ASSERT_VALUES_EQUAL_C(GetStringField(json, "Condition"), "t1.id = t2.id", json.GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL_C(json["Filters"].GetArraySafe().size(), 1, json.GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL_C(json["Filters"].GetArraySafe()[0].GetStringSafe(), "t1.b < t2.c", json.GetStringRobust());
    }

    Y_UNIT_TEST(ExplainAnalyzeMultipleSelectTPCDS23) {
        auto appConfig = CreateExplainPlanTestAppConfig();
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCDS],
            BenchmarkSchemaPath[EBenchType::TPCDS], true);
        auto session = CreateQuerySession(kikimr);
        const TString query = R"(
            $round = ($x, $y) -> { return $x; };
            $to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
            $to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
        )" + GetFullPath(BenchmarkQueryPath[EBenchType::TPCDS], "23.yql");
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[#]]");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).ColumnsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).RowsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).GetColumnsMeta()[0].Name, "c_last_name");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).GetColumnsMeta()[1].Name, "c_first_name");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).GetColumnsMeta()[2].Name, "sales");
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetPlan());
        PrintPlan(TString(*result.GetStats()->GetPlan()), /*analyzeMode=*/true);

        auto stream = session.StreamExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ConcurrentResultSets(false)
                .StatsMode(NYdb::NQuery::EStatsMode::Full)).ExtractValueSync();
        UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
        const auto sequential = CollectStreamResult(stream);
        CompareYson("[[#]]", sequential.ResultSetYson);
        UNIT_ASSERT(sequential.PlanJson);
    }

}

} // namespace NKikimr::NKqp
