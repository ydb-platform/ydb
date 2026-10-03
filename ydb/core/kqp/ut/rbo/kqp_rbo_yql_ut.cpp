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
#include "kqp_rbo_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
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
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;

TString FormatBenchmarkTraceTitle(TStringBuf suiteName, TStringBuf benchmarkName, ui32 queryId) {
    Y_UNUSED(suiteName);
    const TString benchmark(benchmarkName);
    if (benchmark.StartsWith("TPCDS")) {
        return TStringBuilder() << "TPCDS Q" << queryId;
    }
    if (benchmark.StartsWith("TPCH")) {
        return TStringBuilder() << "TPCH Q" << queryId;
    }
    return TStringBuilder() << benchmark << " Q" << queryId;
}

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
            && nodeType->second.GetStringSafe() == connectionName)
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

void CollectCallableNodes(const TExprNode::TPtr& node, TStringBuf callableName, TExprNode::TListType& result) {
    if (node->IsCallable(callableName)) {
        result.push_back(node);
    }

    for (const auto& child : node->ChildrenList()) {
        CollectCallableNodes(child, callableName, result);
    }
}

size_t CountOperatorInTraversal(TOpRoot& root, const IOperator* expected) {
    size_t count = 0;
    for (const auto& item : root) {
        count += item.Current == expected;
    }
    return count;
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

    // A scalar aggregate yields one row even when none of its results is used.
    Y_UNIT_TEST(ScalarAggregatesWithoutUsedResults) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (a Int64 NOT NULL, primary key(a));
            CREATE TABLE `/Root/t2` (a Int64 NOT NULL, b Int64, primary key(a));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder t1;
        t1.BeginList();
        for (const i64 a : {1, 3}) {
            t1.AddListItem().BeginStruct().AddMember("a").Int64(a).EndStruct();
        }
        t1.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", t1.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2;
        t2.BeginList();
        for (const i64 a : {1, 2}) {
            t2.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").OptionalInt64(a * 10).EndStruct();
        }
        t2.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", t2.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const std::vector<std::pair<TString, TString>> cases = {
            {"SELECT COUNT(*) AS c FROM (SELECT MAX(a) AS m FROM `/Root/t1` UNION ALL SELECT MAX(b) AS m FROM `/Root/t2`);",
             "[[2u]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2) ORDER BY t1.a;",
             "[[1];[3]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2 WHERE t2.a = t1.a) ORDER BY t1.a;",
             "[[1];[3]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE NOT EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2 WHERE t2.a = t1.a) ORDER BY t1.a;",
             "[]"},
            {"SELECT 1 AS one FROM (SELECT COUNT(*) AS c FROM `/Root/t1`);",
             "[[1]]"},
        };
        for (const auto& [query, expected] : cases) {
            auto result = session.ExecuteDataQuery(TString("PRAGMA YqlSelect = 'force';\n") + query,
                TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, query);
        }
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

    void TestMultipleSelects(bool columnTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
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
                SELECT id as id2 FROM `/Root/foo` WHERE name != '3_name' order by id;
                SELECT id FROM `/Root/foo`;
            )",

        };

        std::vector<std::string> results = {
            R"([[0];[1];[2];[4];[5];[6];[7];[8];[9]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        auto session2 = queryClient.GetSession().GetValueSync().GetSession();

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST_TWIN(MultipleSelects, ColumnStore) {
        TestMultipleSelects(ColumnStore);
    }

    Y_UNIT_TEST(PGInsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 String,
                _ydb_pk String NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );

            CREATE TABLE dst (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 Decimal(35,4),
                _ydb_pk Utf8 NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            INSERT INTO dst (`_q_000_f_000_type`, `_q_000_f_000_rtref`, `_q_000_f_000_rrref`, `_q_000_f_001_type`, `_q_000_f_001_rtref`, `_q_000_f_001_rrref`, `_q_000_f_002`, `_ydb_pk`) 
SELECT `_q_000_f_000_type` AS `_q_000_f_000_type`, `_q_000_f_000_rtref` AS `_q_000_f_000_rtref`, `_q_000_f_000_rrref` AS `_q_000_f_000_rrref`, `_q_000_f_001_type` AS `_q_000_f_001_type`, `_q_000_f_001_rtref` AS `_q_000_f_001_rtref`, `_q_000_f_001_rrref` AS `_q_000_f_001_rrref`, CAST(`_q_000_f_002` AS Decimal(35,4)) AS `_q_000_f_002`, CAST(RandomUuid(`_ydb_pk`) AS Utf8) AS `_ydb_pk` 
FROM (
    SELECT `t3`.`_q_000_f_000_type`, `t3`.`_q_000_f_000_rtref`, `t3`.`_q_000_f_000_rrref`, `t3`.`_q_000_f_001_type`, `t3`.`_q_000_f_001_rtref`, `t3`.`_q_000_f_001_rrref`, `t3`.`_q_000_f_002`, `t3`.`_ydb_pk`
    FROM src AS `t3`
) AS `direct_base_source`
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

    }

    Y_UNIT_TEST(InsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE dst (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            ALTER TABLE `/Root/dst` ADD INDEX Index1 GLOBAL ON (id, v);
            ALTER TABLE `/Root/dst` ADD INDEX Index12 GLOBAL ON (v);

        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(1)
            .AddMember("v").Int64(10)
            .EndStruct();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(2)
            .AddMember("v").Int64(20)
            .EndStruct();
        rows.EndList();

        auto result = db.BulkUpsert("/Root/src", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            INSERT INTO dst (id, v)
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

        auto selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto updateSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            UPDATE dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(updateSelectRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto deleteRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            DELETE FROM dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(deleteRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([])");

    }

    NKikimrConfig::TAppConfig CreateExplainPlanTestAppConfig(bool inlineJoinFiltersAfterCBO = true) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(inlineJoinFiltersAfterCBO);
        return appConfig;
    }

    void CreateExplainPlanTestTables(TKikimrRunner& kikimr) {
        auto db = kikimr.GetTableClient();
        auto sessionResult = db.CreateSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
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

    NYdb::NQuery::TSession CreateQuerySession(TKikimrRunner& kikimr) {
        auto db = kikimr.GetQueryClient();
        auto res = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(res);
        return res.GetSession();
    }

    class TExplainPlanTestContext {
    public:
        explicit TExplainPlanTestContext(bool inlineJoinFiltersAfterCBO = true)
            : AppConfig(CreateExplainPlanTestAppConfig(inlineJoinFiltersAfterCBO))
            , Kikimr(NKqp::TKikimrSettings(AppConfig).SetWithSampleTables(false))
            , Session(CreateSession())
        {
        }

        NYdb::NQuery::TSession& GetSession() {
            return Session;
        }

        TKikimrRunner& GetKikimr() {
            return Kikimr;
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

    Y_UNIT_TEST(OperatorIteratorMovePreservesDeepTraversal) {
        const TPositionHandle pos;
        TIntrusivePtr<IOperator> op = MakeIntrusive<TOpEmptySource>(pos);
        for (size_t i = 0; i < 30; ++i) {
            op = MakeIntrusive<TOpJoin>(std::move(op), MakeIntrusive<TOpEmptySource>(pos), pos, "Cross", TPairedIUs{});
        }
        auto* expected = op.get();
        TOpRoot root(std::move(op), pos, {});
        auto source = root.begin();
        TOpIterator moved(std::move(source));
        UNIT_ASSERT(source == TOpEnd{});
        ++source;
        UNIT_ASSERT(source == TOpEnd{});
        auto assigned = root.begin();
        assigned = std::move(moved);
        UNIT_ASSERT(moved == TOpEnd{});
        ++moved;
        UNIT_ASSERT(moved == TOpEnd{});
        size_t count = 0;
        IOperator* last = nullptr;
        for (; assigned != TOpEnd{}; ++assigned) {
            last = assigned->Current;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 61);
        UNIT_ASSERT_VALUES_EQUAL(last, expected);
    }

    Y_UNIT_TEST(MapUniqueRawInputIUsFollowExpressionMutations) {
        NTests::TIdTestContext f;
        const auto first = f.Id(), replacement = f.Id(), output = f.Id(), duplicate = f.Id(), again = f.Id();
        auto map = f.Copies(MakeIntrusive<TOpEmptySource>(f.Pos), {{output, first}, {duplicate, first}});
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{first});
        map->SetMapElementExpression(output, f.Column(replacement));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == (TUnorderedIUs{first, replacement}));
        map->RemoveMapElement(duplicate);
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{replacement});
        map->AddMapElement(again, TMapElement(f.Column(first)));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == (TUnorderedIUs{first, replacement}));
        TMapIUs definitions;
        definitions.Add(again, f.Column(first));
        map->SetMapElements(std::move(definitions));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{first});
    }

    Y_UNIT_TEST(MapElementClassifiesOnlyExactCopies) {
        NTests::TIdTestContext f;
        const auto source = f.Id();
        const TMapElement copy(f.Column(source));
        const TMapElement calculation(f.Constant());
        const TMapElement conversion(MakeUnaryCallable("Just", f.Column(source)));
        UNIT_ASSERT(copy.IsColumnAccess());
        UNIT_ASSERT_VALUES_EQUAL(copy.GetColumnAccess(), source);
        UNIT_ASSERT(!calculation.IsColumnAccess());
        UNIT_ASSERT(!conversion.IsColumnAccess());
    }

    Y_UNIT_TEST(SubplanRegistryMutationInvariants) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), dependency = f.Id(), local = f.Id(), secondLocal = f.Id(), missing = f.Id();
        auto& subplans = f.Props.Subplans;
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.Add(binding, {}, ESubplanType::EXISTS), yexception, "null subplan");
        subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS),
            yexception, "Duplicate subplan binding");
        TDependencyIUs captures;
        const auto* type = f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
        captures.Add(local, TCapturedIU{dependency, type});
        captures.Add(secondLocal, TCapturedIU{dependency, type});
        subplans.ReplacePlan(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)));
        subplans.RefreshDependencies(binding);
        UNIT_ASSERT(subplans.At(binding).DependentIUs == TUnorderedIUs{dependency});
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.ReplacePlan(binding, {}), yexception, "null subplan");
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.Remove(missing), yexception, "Unknown subplan binding");
    }

    Y_UNIT_TEST(SubplanBindingsCannotBeRenamed) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), replacement = f.Id();
        auto& subplans = f.Props.Subplans;
        subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.RenameExternalReferences({{binding, replacement}}),
            yexception, "cannot be rebound as parameters");
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.RenameExternalReferences({{replacement, binding}}),
            yexception, "cannot be rebound as parameters");
    }

    Y_UNIT_TEST(ExpressionInputIUsFollowSubplanRegistryMutations) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), dependency = f.Id(), local = f.Id();
        auto expression = f.Column(binding);
        UNIT_ASSERT(expression.GetRawInputIUs() == TUnorderedIUs{binding});
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{binding});
        f.Props.Subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT(expression.GetInputIUs(false, true).Empty());
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{dependency, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.ReplacePlan(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)));
        f.Props.Subplans.RefreshDependencies(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{dependency});
        f.Props.Subplans.Remove(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{binding});
    }

    Y_UNIT_TEST(ExpressionInputIUBufferSupportsAllResolutionModes) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), tuple = f.Id(), dependency = f.Id(), local = f.Id();
        auto expression = f.Column(binding);
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{dependency, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.Add(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)), ESubplanType::IN_SUBPLAN, {tuple}, local);
        f.Props.Subplans.RefreshDependencies(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, false).Empty());
        UNIT_ASSERT(expression.GetInputIUs(true, false) == TUnorderedIUs{binding});
        UNIT_ASSERT(expression.GetInputIUs(false, true) == (TUnorderedIUs{tuple, dependency}));
        UNIT_ASSERT(expression.GetInputIUs(true, true) == (TUnorderedIUs{binding, tuple, dependency}));
        UNIT_ASSERT(expression.GetInputIUs(false, false).Empty());
    }

    Y_UNIT_TEST(SubplanTraversalFollowsRegistryMutations) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), replacement = f.Id();
        auto filter = MakeIntrusive<TOpFilter>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, f.Column(binding));
        auto* consumer = filter.get();
        filter->SetFilterExpression(f.Column(replacement));
        UNIT_ASSERT(filter->GetUniqueRawInputIUs() == TUnorderedIUs{replacement});
        filter->SetFilterExpression(f.Column(binding));
        auto root = f.Root(std::move(filter), {});
        auto& subplans = root->PlanProps.Subplans;
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans).Empty());
        auto original = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto substitute = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* originalPtr = original.get();
        auto* substitutePtr = substitute.get();
        subplans.Add(binding, std::move(original), ESubplanType::EXISTS);
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans) == TUnorderedIUs{binding});
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, originalPtr), 1);
        subplans.ReplacePlan(binding, std::move(substitute));
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, originalPtr), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, substitutePtr), 1);
        subplans.Remove(binding);
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans).Empty());
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, substitutePtr), 0);
    }

    Y_UNIT_TEST(SubplanTraversalResumesRawIUResolutionAfterMove) {
        NTests::TIdTestContext f;
        const auto first = f.Id(), second = f.Id();
        auto firstPlan = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto secondPlan = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* firstPtr = firstPlan.get();
        auto* secondPtr = secondPlan.get();
        f.Props.Subplans.Add(first, std::move(firstPlan), ESubplanType::EXISTS);
        f.Props.Subplans.Add(second, std::move(secondPlan), ESubplanType::EXISTS);
        auto root = f.Root(MakeIntrusive<TOpFilter>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos,
            MakeBinaryPredicate("And", f.Column(first), f.Column(second))), {});
        auto iterator = root->begin();
        UNIT_ASSERT(iterator->Current == firstPtr);
        UNIT_ASSERT(iterator->SubplanIU && *iterator->SubplanIU == first);
        TOpIterator moved(std::move(iterator));
        UNIT_ASSERT(iterator == TOpEnd{});
        ++moved;
        UNIT_ASSERT(moved != TOpEnd{});
        UNIT_ASSERT(moved->Current == secondPtr);
        UNIT_ASSERT(moved->SubplanIU && *moved->SubplanIU == second);
    }

    Y_UNIT_TEST(ComputeParentsHandlesSharedDagAndIgnoresInactiveSubplans) {
        NTests::TIdTestContext f;
        auto hub = TReplicate::Create(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto* leftPtr = left.get();
        auto* rightPtr = right.get();
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "Cross", TPairedIUs{});
        auto* joinPtr = join.get();
        auto inactiveChild = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* inactivePtr = inactiveChild.get();
        f.Props.Subplans.Add(f.Id(), MakeIntrusive<TOpJoin>(std::move(inactiveChild),
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, "Cross", TPairedIUs{}), ESubplanType::EXISTS);
        auto root = f.Root(std::move(join), {});
        root->ComputeParents();
        UNIT_ASSERT_VALUES_EQUAL(joinPtr->Parents.size(), 1);
        UNIT_ASSERT(joinPtr->Parents[0].first == root.get());
        UNIT_ASSERT_VALUES_EQUAL(hub->GetOutputs().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(hub->GetInput()->Parents.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(leftPtr->Parents[0].first, joinPtr);
        UNIT_ASSERT_VALUES_EQUAL(leftPtr->Parents[0].second, 0);
        UNIT_ASSERT_VALUES_EQUAL(rightPtr->Parents[0].first, joinPtr);
        UNIT_ASSERT_VALUES_EQUAL(rightPtr->Parents[0].second, 1);
        UNIT_ASSERT(inactivePtr->Parents.empty());
        UNIT_ASSERT(&leftPtr->GetChild(0) == &rightPtr->GetChild(0));
        size_t visits = 0;
        for (const auto& item : *root) {
            visits += item.Current == hub->GetInput().Get();
        }
        UNIT_ASSERT_VALUES_EQUAL(visits, 1);
        auto replacement = MakeIntrusive<TOpEmptySource>(f.Pos);
        leftPtr->SetChild(0, replacement);
        UNIT_ASSERT(rightPtr->GetChild(0) == replacement);
        UNIT_ASSERT(hub->GetInput() == replacement);
        root->ComputeParents();
        UNIT_ASSERT_VALUES_EQUAL(replacement->Parents.size(), 2);
    }

    Y_UNIT_TEST(ProjectedKqpOpMapPreservesAvailableInputIds) {
        NTests::TIdTestContext f;
        auto& ctx = f.ExprCtx;
        const auto empty = ctx.NewCallable(f.Pos, "KqpOpEmptySource", {});
        auto constant = f.Constant().Node;
        constant->TailPtr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        const auto first = ctx.NewCallable(f.Pos, "KqpOpMap", {empty, ctx.NewList(f.Pos, {
            ctx.NewCallable(f.Pos, "KqpOpMapElementLambda", {empty, ctx.NewAtom(f.Pos, "payload"), constant, ctx.NewAtom(f.Pos, "false")}),
            ctx.NewCallable(f.Pos, "KqpOpMapElementLambda", {empty, ctx.NewAtom(f.Pos, "unmentioned"), constant, ctx.NewAtom(f.Pos, "false")})
        })});
        auto project = [&](TExprNode::TPtr input, TStringBuf from, TStringBuf to) {
            return ctx.NewCallable(f.Pos, "KqpOpMap", {input, ctx.NewList(f.Pos, {
                ctx.NewCallable(f.Pos, "KqpOpMapElementRename", {input, ctx.NewAtom(f.Pos, to), ctx.NewAtom(f.Pos, from)})
            }), ctx.NewAtom(f.Pos, "true")});
        };
        PlanConverter converter(f.TypeCtx, ctx);
        auto plan = converter.ExprNodeToOperator(project(project(first, "payload", "projected"), "projected", "out"));
        auto& outer = CastOperator<TOpMap>(*plan);
        auto& inner = CastOperator<TOpMap>(*outer.GetInput());
        auto& initial = CastOperator<TOpMap>(*inner.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(initial.GetOutputIUs().Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(inner.GetOutputIUs().Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(outer.GetOutputIUs().Size(), 4);
        UNIT_ASSERT(initial.GetOutputIUs().IsSubsetOf(outer.GetOutputIUs()));
        const auto outerId = *outer.GetMapElements().Keys().begin();
        const auto innerId = *inner.GetMapElements().Keys().begin();
        UNIT_ASSERT_VALUES_EQUAL(outer.GetMapElements().Find(outerId)->GetColumnAccess(), innerId);
        UNIT_ASSERT_VALUES_EQUAL(converter.PlanProps.InfoUnitRegistry.Get(outerId).GetFullName(), "out");
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

    Y_UNIT_TEST(QualifiedStarSubqueryFallsBackToYqlOptimizer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `doc` (
                `id` String,
                `flag` Bool,
                PRIMARY KEY (`id`)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem().BeginStruct()
            .AddMember("id").String("disabled")
            .AddMember("flag").Bool(false)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").String("enabled")
            .AddMember("flag").Bool(true)
            .EndStruct();
        rows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/doc", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        const auto compileCountersBefore = GetNewRBOCompileCounters(kikimr);
        auto result = querySession.ExecuteQuery(R"(
            SELECT `d`.`id` FROM (SELECT `d_src`.* FROM `doc` AS `d_src` WHERE `d_src`.`flag`) AS `d`;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[["enabled"]]])");
        UNIT_ASSERT_C(result.GetStats().has_value(), "Missing full query statistics");
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing query plan in full statistics");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
        UNIT_ASSERT_C(planJson.GetMapSafe().contains("SimplifiedPlan"), plan);
        const auto& planRoot = planJson.GetMapSafe().at("Plan").GetMapSafe();
        UNIT_ASSERT_VALUES_EQUAL_C(planRoot.at("Node Type").GetStringSafe(), "Query", plan);
        UNIT_ASSERT_C(planRoot.contains("Stats"), plan);

        const auto compileCountersAfter = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(compileCountersAfter.first, compileCountersBefore.first);
        UNIT_ASSERT_VALUES_EQUAL(compileCountersAfter.second, compileCountersBefore.second + 1);
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
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
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
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(101)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(4)
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

            SELECT d.k
            FROM (
                SELECT DISTINCT k, v
                FROM `/Root/dups`
            ) AS d
            ORDER BY d.k;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings())
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[10];[10];[20]])");
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

    Y_UNIT_TEST_TWIN(AsTable, EnablePeepholeNewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(EnablePeepholeNewRbo);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

        const auto params = TParamsBuilder()
            .AddParam("$param")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(1)
                        .AddMember("value").String("one")
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        const TString query = R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT id, value FROM AS_TABLE($param);
        )";

        auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
        UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetAst(), "Missing final AST");
        UNIT_ASSERT_STRING_CONTAINS(*explain.GetStats()->GetAst(),
            EnablePeepholeNewRbo ? "ToFlow $param" : "Iterator $param");
        UNIT_ASSERT_C(explain.GetStats()->GetPlan(), "Missing explain plan");
        const TString plan = *explain.GetStats()->GetPlan();
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* source = FindOperatorByStringField(simplifiedPlan, "Name", "EmptySource");
        UNIT_ASSERT_C(source, plan);
        UNIT_ASSERT_VALUES_EQUAL(GetStringField(*source, "Parameter"), "$param");

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1;"one"]])");

        const auto moreParams = TParamsBuilder()
            .AddParam("$param")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(3)
                        .AddMember("value").String("three")
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(1)
                        .AddMember("value").String("one")
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(2)
                        .AddMember("value").String("two")
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        const TString filteredQuery = R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT p.id, p.value || "!" AS marked
            FROM AS_TABLE($param) AS p
            WHERE p.id >= 2
            ORDER BY p.id;
        )";

        auto filteredExplain = session.ExecuteQuery(filteredQuery, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(filteredExplain.IsSuccess(), filteredExplain.GetIssues().ToString());
        UNIT_ASSERT_C(filteredExplain.GetStats() && filteredExplain.GetStats()->GetPlan(), "Missing filtered explain plan");
        const TString filteredPlan = *filteredExplain.GetStats()->GetPlan();
        const auto filteredSimplifiedPlan = GetSimplifiedPlan(filteredPlan);
        const auto* filteredSource = FindOperatorByStringField(filteredSimplifiedPlan, "Name", "EmptySource");
        UNIT_ASSERT_C(filteredSource, filteredPlan);
        UNIT_ASSERT_VALUES_EQUAL(GetStringField(*filteredSource, "Parameter"), "$param");
        UNIT_ASSERT_C(FindOperatorByStringField(filteredSimplifiedPlan, "Name", "Filter"), filteredPlan);

        result = session.ExecuteQuery(filteredQuery, NYdb::NQuery::TTxControl::NoTx(), moreParams).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[2;"two!"];[3;"three!"]])");

        result = session.ExecuteQuery(R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT p.id FROM AS_TABLE($param) AS p WHERE p.id > 10;
        )", NYdb::NQuery::TTxControl::NoTx(), moreParams).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[]");
    }

    constexpr std::array<i64, 7> SqlInLookupValues = {-1, 0, 1, 2, 3, 10, 2147483648LL};

    TKikimrSettings SqlInSettings(bool columnStore = false) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetAllowOlapDataQuery(columnStore);
        return TKikimrSettings(appConfig).SetWithSampleTables(false);
    }

    TString SqlInPeepholePragma(bool peephole) {
        return TStringBuilder()
            << "PRAGMA ydb.EnableNewRBOPhysicalStagePeephole = \"" << (peephole ? "true" : "false") << "\";\n";
    }

    TString SqlInFunction(bool ansi) {
        // YqlSelect does not support the ANSI IN pragma. Set the callable option
        // explicitly so both modes exercise the new RBO physical lowering.
        return TStringBuilder()
            << "$in = ($items, $lookup) -> { RETURN Yql::SqlIn($items, $lookup, "
            << (ansi ? "AsTuple(AsTuple(AsAtom('ansi')))" : "AsTuple()") << "); };\n";
    }

    void CreateSqlInTable(TKikimrRunner& kikimr) {
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/sql_in` (
                id Int64 NOT NULL,
                value Int64,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        TValueBuilder rows;
        rows.BeginList();
        for (i64 id : SqlInLookupValues) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("value").OptionalInt64(id == 10 ? std::nullopt : std::optional<i64>(id))
                .EndStruct();
        }
        rows.EndList();
        const auto upsert = client.BulkUpsert("/Root/sql_in", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
    }

    std::optional<bool> ExpectedSqlIn(std::optional<double> lookup, const TVector<std::optional<i64>>& items, bool ansi) {
        if (!lookup) {
            return ansi && items.empty() ? std::optional<bool>(false) : std::nullopt;
        }
        if (std::find(items.begin(), items.end(), lookup) != items.end()) {
            return true;
        }
        if (ansi && std::find(items.begin(), items.end(), std::nullopt) != items.end()) {
            return std::nullopt;
        }
        return false;
    }

    TValue MakeSqlInListParameter(EPrimitiveType primitive, bool optional, const TVector<std::optional<i64>>& items) {
        TTypeBuilder type;
        type.BeginList();
        if (optional) {
            type.BeginOptional();
        }
        type.Primitive(primitive);
        if (optional) {
            type.EndOptional();
        }
        type.EndList();

        TValueBuilder value(type.Build());
        value.BeginList();
        for (const auto& item : items) {
            value.AddListItem();
            if (!item) {
                value.EmptyOptional();
                continue;
            }
            if (optional) {
                value.BeginOptional();
            }
            switch (primitive) {
                case EPrimitiveType::Int64: value.Int64(*item); break;
                case EPrimitiveType::Int32: value.Int32(*item); break;
                case EPrimitiveType::Uint64: value.Uint64(*item); break;
                default: UNIT_FAIL("Unexpected test parameter type");
            }
            if (optional) {
                value.EndOptional();
            }
        }
        return value.EndList().Build();
    }

    Y_UNIT_TEST_TWIN(SqlInScalarListParameters, Ansi) {
        TKikimrRunner kikimr(SqlInSettings());
        CreateSqlInTable(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        for (const auto& [primitive, optional] : {
            std::pair{EPrimitiveType::Int64, false},
            std::pair{EPrimitiveType::Int64, true},
            std::pair{EPrimitiveType::Int32, false},
            std::pair{EPrimitiveType::Uint64, true},
        }) {
            TVector<TVector<std::optional<i64>>> lists = {{}, {1, 3, 3}, {7}, {0}};
            if (primitive == EPrimitiveType::Int32) {
                // 2147483648 must not wrap to a matching Int32 key.
                lists.push_back({-2147483648LL});
            }
            if (optional) {
                lists.push_back({1, std::nullopt});
                lists.push_back({std::nullopt});
            }
            for (const auto& items : lists) {
                const auto value = MakeSqlInListParameter(primitive, optional, items);
                const auto params = TParamsBuilder().AddParam("$items", value).Build();
                for (bool peephole : {true, false}) {
                    const TString query = TStringBuilder() << SqlInPeepholePragma(peephole)
                        << "DECLARE $items AS " << value.GetType().ToString() << ";\n"
                        << SqlInFunction(Ansi)
                        << R"(
                            SELECT id,
                                $in($items, id) AS found,
                                NOT $in($items, id) AS missing,
                                $in($items, value) AS nullable_found,
                                NOT $in($items, value) AS nullable_missing,
                                $in($items, CAST(id AS Double) + 0.5) AS fractional_found,
                                NOT $in($items, CAST(id AS Double) + 0.5) AS fractional_missing
                            FROM `/Root/sql_in`
                            ORDER BY id;
                        )";
                    const auto result = session.ExecuteDataQuery(
                        query, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
                    const auto& resultSet = result.GetResultSet(0);
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), SqlInLookupValues.size());

                    // Check the result types, including the non-nullable ANSI case.
                    for (size_t column = 1; column < 7; ++column) {
                        const bool nullable = column == 3 || column == 4 || (Ansi && optional);
                        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[column].Type.ToString(),
                            nullable ? "Bool?" : "Bool", query);
                    }
                    TResultSetParser parser(resultSet);
                    for (i64 id : SqlInLookupValues) {
                        UNIT_ASSERT(parser.TryNextRow());
                        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetInt64(), id);
                        for (size_t column = 1; column < 7; ++column) {
                            const bool nullableValue = column == 3 || column == 4;
                            const auto lookup = nullableValue && id == 10 ? std::nullopt
                                : std::optional<double>(id + (column >= 5 ? 0.5 : 0.0));
                            auto expected = ExpectedSqlIn(lookup, items, Ansi);
                            if (column % 2 == 0 && expected) {
                                expected = !*expected;
                            }
                            const bool nullable = nullableValue || (Ansi && optional);
                            const auto actual = nullable ? parser.ColumnParser(column).GetOptionalBool()
                                : std::optional<bool>(parser.ColumnParser(column).GetBool());
                            UNIT_ASSERT_C(actual == expected,
                                query << "\nid=" << id << ", column=" << column << ", list size=" << items.size());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(SqlInLiteralCollections, Ansi) {
        TKikimrRunner kikimr(SqlInSettings());
        CreateSqlInTable(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        TString baseline;
        for (bool peephole : {true, false}) {
            const TString query = SqlInPeepholePragma(peephole) + SqlInFunction(Ansi) + R"(
                SELECT id,
                    $in((1, 2), value) AS small_tuple,
                    NOT $in((1, 2, 3, 4, 5, 6), value) AS large_tuple,
                    $in([1, 2, 3, 4, 5, 6], value) AS large_list,
                    NOT $in(AsList(), value) AS empty_list,
                    $in((1, NULL), value) AS nullable_tuple,
                    $in(AsTuple(), value) AS empty_tuple,
                    $in([NULL], value) AS null_list,
                    $in([1, 2, 3, 4, 5, 6], NULL) AS null_lookup
                FROM `/Root/sql_in` ORDER BY id;
            )";
            const auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            const TString actual = FormatResultSetYson(result.GetResultSet(0));
            if (peephole) {
                baseline = actual;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(actual, baseline);
            }
        }
    }

    Y_UNIT_TEST_TWIN(SqlInParameterizedFilterAndAggregate, ColumnStore) {
        TKikimrRunner kikimr(SqlInSettings(ColumnStore));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        const TString schema = TStringBuilder() << R"(
            CREATE TABLE `/Root/inforg8025` (
                id Int64 NOT NULL,
                _period Int64,
                _fld8026rref String,
                _fld8027rref String,
                _fld8028rref String,
                _fld543 String,
                PRIMARY KEY (id)
            )
        )" << (ColumnStore ? "WITH (STORE = COLUMN);" : ";");
        const auto scheme = session.ExecuteSchemeQuery(schema).GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        TValueBuilder rows;
        rows.BeginList();
        for (i64 id : {1, 2, 3, 4}) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("_period").Int64(id)
                .AddMember("_fld8026rref").String("group")
                .AddMember("_fld8027rref").String("included")
                .AddMember("_fld8028rref").String(id == 3 ? "excluded" : "allowed")
                .AddMember("_fld543").String("tenant")
                .EndStruct();
        }
        const auto upsert = client.BulkUpsert("/Root/inforg8025", rows.EndList().Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        const auto params = TParamsBuilder()
            .AddParam("$const_0").Int64(3).Build()
            .AddParam("$const_1").BeginList().AddListItem().String("included").EndList().Build()
            .AddParam("$const_2").BeginList().AddListItem().OptionalString("excluded")
                .AddListItem().OptionalString(std::nullopt).EndList().Build()
            .AddParam("$const_3").String("tenant").Build()
            .AddParam("$const_4").String("group").Build()
            .Build();
        for (bool peephole : {true, false}) {
            const TString query = SqlInPeepholePragma(peephole) + R"(
                DECLARE $const_0 AS Int64;
                DECLARE $const_1 AS List<String>;
                DECLARE $const_2 AS List<String?>;
                DECLARE $const_3 AS String;
                DECLARE $const_4 AS String;

                SELECT t1._fld8026rref AS __ydb_aggregate_0, MAX(t1._period) AS __ydb_aggregate_1
                FROM `/Root/inforg8025` AS t1
                WHERE t1._period <= $const_0
                    AND t1._fld8027rref IN $const_1
                    AND t1._fld8028rref NOT IN $const_2
                    AND t1._fld543 = $const_3
                    AND t1._fld8026rref = $const_4
                GROUP BY t1._fld8026rref;
            )";
            const auto result = session.ExecuteDataQuery(
                query, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["group"];[2]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
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
                SELECT t1.a, t1.b FROM $subselect as t1 join $subselect as t2 on t1.a = t2.a WHERE t1.a == 1 and t1.b == 1 order by t1.a;
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

    Y_UNIT_TEST(MultiConsumerRowStorageSourceProducesDqPhyStage) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        auto schemaResult = dbSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/sales` (
                region_id Int64 NOT NULL,
                amount Int64 NOT NULL,
                primary key(region_id)
            );
            CREATE TABLE `/Root/quotas` (
                region_id Int64 NOT NULL,
                threshold Int64 NOT NULL,
                primary key(region_id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        // Insert test data.
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [regionId, amount] :
                 TVector<std::pair<i64, i64>>{{1, 100}, {2, 200}, {3, 300}, {4, 100}, {5, 500}}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("region_id").Int64(regionId)
                    .AddMember("amount").Int64(amount)
                    .EndStruct();
            }
            rows.EndList();
            auto resultUpsert = db.BulkUpsert("/Root/sales", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [regionId, threshold] :
                 TVector<std::pair<i64, i64>>{{3, 100}, {4, 500}, {10, 50}, {11, 150}}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("region_id").Int64(regionId)
                    .AddMember("threshold").Int64(threshold)
                    .EndStruct();
            }
            rows.EndList();
            auto resultUpsert = db.BulkUpsert("/Root/quotas", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
        }

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        // Find sales whose amount exceeds their region's quota. The equi key
        // (s.region_id = q.region_id) keeps the join keys non-empty, while the
        // residual comparison (s.amount > q.threshold) combined with LEFT-join
        // semantics forces the shared multi-consumer row-storage read.
        auto result = session.ExecuteQuery(
            R"(
                SELECT s.region_id, s.amount, q.threshold, q.region_id AS quota_region
                FROM `/Root/sales` AS s
                LEFT JOIN `/Root/quotas` AS q
                  ON s.region_id = q.region_id AND s.amount > q.threshold
                ORDER BY s.region_id;
            )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResultSetYson(result.GetResultSet(0)),
            R"([[1;100;#;#];[2;200;#;#];[3;300;[100];[3]];[4;100;#;#];[5;500;#;#]])");

       std::vector<std::pair<TString, TString>> residualQueries = {
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                WHERE EXISTS (
                    SELECT 1 FROM `/Root/quotas` AS q
                    WHERE s.region_id = q.region_id AND s.amount > q.threshold
                )
                ORDER BY s.region_id;
             )",
             R"([[3;300]])"},
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                WHERE NOT EXISTS (
                    SELECT 1 FROM `/Root/quotas` AS q
                    WHERE s.region_id = q.region_id AND s.amount > q.threshold
                )
                ORDER BY s.region_id;
             )",
             R"([[1;100];[2;200];[4;100];[5;500]])"},
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                INNER JOIN `/Root/quotas` AS q
                  ON s.region_id = q.region_id AND s.amount > q.threshold
                ORDER BY s.region_id;
             )",
             R"([[3;300]])"},
        };

        for (const auto& [query, expected] : residualQueries) {
            result = session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, query + ": " + result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, query);
        }
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
            UNIT_ASSERT_C(ast.find("RangeFinalize") != TString::npos, "Ranges not pushed");

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
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
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
                d Int64,
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

        const std::vector<std::string> queriesOnEmptyColumns = {
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
            R"(
                select some(t1.a), some(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select sum(distinct t1.a), max(distinct t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select count(distinct t1.a), count(distinct t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select avg(distinct t1.a), avg(distinct t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select avg(distinct t2.e), avg(distinct t2.d) from `/Root/t2` as t2;
            )"
        };

        const std::vector<std::string> resultsEmptyColumns = {
            R"([[0u]])",
            R"([[0u;0u]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[#;#]])",
            R"([[0u;0u]])",
            R"([[#;#]])",
            R"([[#;#]])"
        };

        for (ui32 i = 0; i < queriesOnEmptyColumns.size(); ++i) {
            const auto& query = queriesOnEmptyColumns[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
                .AddMember("d").Int64(i % 3)
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
            R"(
                PRAGMA YqlSelect = 'force';
                select max(distinct t1.a), min(distinct t1.b) from `/Root/t1` as t1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select max(t1.a), min(distinct t1.b) from `/Root/t1` as t1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select max(distinct t1.a), min(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select sum(distinct t1.a), sum(t1.a) from `/Root/t1` as t1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select sum(distinct t1.b), sum(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select sum(distinct t1.a) as r0, sum(t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select sum(distinct t1.c) as r0, sum(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select sum(distinct t1.c) as r0, sum(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select count(distinct t1.a) as r0, count(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select count(distinct t1.a) as r0, count(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select min(distinct t1.a) as r0, max(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select avg(distinct t1.a) as r0, avg(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select distinct coalesce(t1.a, 0) as a, coalesce(t1.b, 1) as b, unwrap(t1.c) as c from `/Root/t1` as t1 order by a, b, c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select count(distinct t1.a) as r0, count(t1.b) as r1, count(t1.c) as r2 from `/Root/t1` as t1 order by r0, r1, r2;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                select count(distinct t1.a) as r0, count(distinct t1.c) as r1, count(t1.d) as r2 from `/Root/t1` as t1 group by t1.b order by r0, r1, r2;
            )",
            // GROUP BY without aggregation functions must still emit one row per distinct key.
            R"(
                PRAGMA YqlSelect = 'force';
                select t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                PRAGMA AnsiImplicitCrossJoin;
                select t1.b, t2.c from `/Root/t1` as t1, `/Root/t2` as t2 where t1.b = t2.b group by t1.b, t2.c order by t1.b limit 100;
            )",
            R"(
                PRAGMA YqlSelectAllowUnnamedGroupByExpr;
                select t1.d + 1 as k from `/Root/t1` as t1 group by t1.d + 1 order by k;
            )",
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
                                            R"([[[4];[1]]])",
                                            R"([[[4];[1]]])",
                                            R"([[[4];[1]]])",
                                            R"([[[10];[10]]])",
                                            R"([[[3];[8]]])",
                                            R"([[4;[4]];[6;[6]]])",
                                            R"([[[2];4];[[2];6]])",
                                            R"([[[2];4];[[2];6]])",
                                            R"([[2u;2u];[3u;3u]])",
                                            R"([[2u;1u];[3u;1u]])",
                                            R"([[0;[2]];[1;[2]]])",
                                            R"([[2.;[2.]];[2.;[2.]]])",
                                            R"([[0;2;2];[1;1;2];[2;2;2];[3;1;2];[4;2;2]])",
                                            R"([[5u;5u;5u]])",
                                            R"([[2u;1u;2u];[3u;1u;3u]])",
                                            R"([[[1]];[[2]]])",
                                            R"([[[1];[2]];[[2];[2]]])",
                                            R"([[[1]];[[2]];[[3]]])"
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
                .AddMember("d").Int64(3)
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
                SELECT stddev_samp(t1.a) as res, t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
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

    Y_UNIT_TEST_TWIN(BasicHashJoin, UseBlockHashJoin) {
        BasicHashJoinTest(UseBlockHashJoin);
    }

    Y_UNIT_TEST_QUAD(BlockHashJoinCrossWithJoinFilters, UseBlockHashJoin, UseBlockHashJoinForCross) {
        constexpr bool UseBlockCrossJoin = UseBlockHashJoin && UseBlockHashJoinForCross;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(UseBlockHashJoin);
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoinForCross(UseBlockHashJoinForCross);
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

    Y_UNIT_TEST(LookupJoins_oldRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        // appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
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
                INDEX Index1_12 GLOBAL ON (SubKey1, SubKey2),
                INDEX Index1_21 GLOBAL ON (SubKey2, Value1),
                INDEX Index1_212 GLOBAL ON (SubKey2) COVER (Value2)
            );

            CREATE TABLE `/Root/Table2` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2),
                INDEX Index2_12 GLOBAL ON (SubKey1, SubKey2),
                INDEX Index2_21 GLOBAL ON (SubKey2, Value1),
                INDEX Index2_212 GLOBAL ON (SubKey2) COVER (Value2)
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

            UPSERT INTO `/Root/Table2` (Key, SubKey1, SubKey2, Value1, Value2) VALUES
                (0, 0, "0", "1", "1"),
                (0, 0, "1", "2", "2"),
                (0, 1, "0", "3", "3"),
                (0, 1, "1", "4", "4"),
                (1, 0, "0", "15", "15"),
                (1, 0, "1", "16", "16"),
                (1, 1, "0", "17", "17"),
                (1, 1, "1", "18", "18");    
            )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                -- MapJoin, PK left / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.Key = 0 AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, Index12 left side stream lookup for Value1+Value2 / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0" AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, PK left / Index21 right side stream lookup for Value2
                SELECT t1.Value2, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.Key = 0 AND t2.SubKey2 = "0"
                ORDER BY t1.Value2, t2.Value1;
            )",
            R"(
                -- MapJoin, Index212 left / Index212 right, no stream lookup needed
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.SubKey2 = "1" AND t2.SubKey2 = "1"
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- LookupJoin, PK left / PK right (probe t2 by Key)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.Key >= 0
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index12 left / PK right (t1 via Index12 filter, probe t2 PK)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0"
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, PK left / Index21 right (probe t2 by SubKey2 and need t2.Value1)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.Key = 1
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index212 left / Index212 right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.SubKey2 >= "0"
                ORDER BY t1.Value2, t2.Value2;
            )",
        };

        std::vector<std::string> results = {
            R"([[["1"];["1"]];[["2"];["2"]];[["3"];["3"]];[["4"];["4"]]])",
            R"([[["1"];["1"]]])",
            R"([[["1"];["1"]];[["3"];["3"]]])",
            R"([[["2"];["2"]];[["4"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["2"];["1"]];[["2"];["2"]];[["2"];["3"]];[["2"];["4"]];[["3"];["1"]];[["3"];["2"]];[["3"];["3"]];[["3"];["4"]];[["4"];["1"]];[["4"];["2"]];[["4"];["3"]];[["4"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]];[["6"];["15"]];[["6"];["16"]];[["6"];["17"]];[["6"];["18"]];[["7"];["15"]];[["7"];["16"]];[["7"];["17"]];[["7"];["18"]];[["8"];["15"]];[["8"];["16"]];[["8"];["17"]];[["8"];["18"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]]])",
            R"([[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["15"]];[["1"];["17"]];[["1"];["3"]];[["2"];["16"]];[["2"];["18"]];[["2"];["2"]];[["2"];["4"]];[["3"];["1"]];[["3"];["15"]];[["3"];["17"]];[["3"];["3"]];[["4"];["16"]];[["4"];["18"]];[["4"];["2"]];[["4"];["4"]];[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
        };

        struct TCase {
            bool Lookup;
            TVector<TString> Impls;
        };
        const std::vector<TCase> cases = {
            {false, {}},
            {false, {"Index1_12/indexImplTable"}},
            {false, {"Index2_21/indexImplTable"}},
            {false, {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
            {true,  {}},
            {true,  {"Index1_12/indexImplTable"}},
            {true,  {"Index2_21/indexImplTable"}},
            {true,  {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
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

            const bool lookupPlan = plan.Contains("InnerJoin (Lookup)") || plan.Contains("TableLookupJoin");
            if (cases[i].Lookup) {
                UNIT_ASSERT_C(lookupPlan, "query #" << i << " expected a lookup join, plan:\n" << plan);
            } else {
                UNIT_ASSERT_C(plan.Contains("InnerJoin (Map)"), "query #" << i << " expected a map join, plan:\n" << plan);
                UNIT_ASSERT_C(!lookupPlan, "query #" << i << " expected no lookup join, plan:\n" << plan);
            }

            if (cases[i].Impls.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " expected only main-table reads, ast:\n" << ast);
            } else {
                for (const auto& impl : cases[i].Impls) {
                    UNIT_ASSERT_C(ast.Contains(impl), "query #" << i << " expected " << impl << ", ast:\n" << ast);
                    UNIT_ASSERT_C(plan.Contains("indexImplTable"), "query #" << i << " expected an index read, plan:\n" << plan);
                }
            }
        }
    }

    Y_UNIT_TEST(LookupJoins_newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

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

            CREATE TABLE `/Root/Table2` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto upsert = [&](const char* table, const std::vector<std::tuple<i32, i32, TString, TString, TString>>& data) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [key, subKey1, subKey2, value1, value2] : data) {
                rows.AddListItem().BeginStruct()
                    .AddMember("Key").OptionalInt32(key)
                    .AddMember("SubKey1").OptionalInt32(subKey1)
                    .AddMember("SubKey2").OptionalString(subKey2)
                    .AddMember("Value1").OptionalString(value1)
                    .AddMember("Value2").OptionalString(value2)
                    .EndStruct();
            }
            rows.EndList();
            auto r = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
        };

        upsert("/Root/Table", {{0, 0, "0", "1", "1"}, {0, 0, "1", "2", "2"}, {0, 1, "0", "3", "3"}, {0, 1, "1", "4", "4"},
                               {1, 0, "0", "5", "5"}, {1, 0, "1", "6", "6"}, {1, 1, "0", "7", "7"}, {1, 1, "1", "8", "8"}});

        upsert("/Root/Table2", {{0, 0, "0", "1", "1"}, {0, 0, "1", "2", "2"}, {0, 1, "0", "3", "3"}, {0, 1, "1", "4", "4"},
                                {1, 0, "0", "15", "15"}, {1, 0, "1", "16", "16"}, {1, 1, "0", "17", "17"}, {1, 1, "1", "18", "18"}});

        for (const auto& addIndex : {
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_12 GLOBAL ON (SubKey1, SubKey2);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_21 GLOBAL ON (SubKey2, Value1);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_212 GLOBAL ON (SubKey2) COVER (Value2);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_12 GLOBAL ON (SubKey1, SubKey2);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_21 GLOBAL ON (SubKey2, Value1);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_212 GLOBAL ON (SubKey2) COVER (Value2);",
             }) {
            result = session.ExecuteSchemeQuery(addIndex).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                -- MapJoin, PK left / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.Key = 0 AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, Index12 left side stream lookup for Value1+Value2 / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0" AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, PK left / Index21 right side stream lookup for Value2
                SELECT t1.Value2, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.Key = 0 AND t2.SubKey2 = "0"
                ORDER BY t1.Value2, t2.Value1;
            )",
            R"(
                -- MapJoin, Index212 left / Index212 right, no stream lookup needed
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.SubKey2 = "1" AND t2.SubKey2 = "1"
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- LookupJoin, PK left / PK right (probe t2 by Key)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.Key >= 0
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index12 left / PK right (t1 via Index12 filter, probe t2 PK)              
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0"
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, PK left / Index21 right (probe t2 by SubKey2 and need t2.Value1)           
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.Key = 1
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index212 left / Index212 right             
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.SubKey2 >= "0"
                ORDER BY t1.Value2, t2.Value2;
            )",
        };

        std::vector<std::string> results = {
            R"([[["1"];["1"]];[["2"];["2"]];[["3"];["3"]];[["4"];["4"]]])",
            R"([[["1"];["1"]]])",
            R"([[["1"];["1"]];[["3"];["3"]]])",
            R"([[["2"];["2"]];[["4"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["2"];["1"]];[["2"];["2"]];[["2"];["3"]];[["2"];["4"]];[["3"];["1"]];[["3"];["2"]];[["3"];["3"]];[["3"];["4"]];[["4"];["1"]];[["4"];["2"]];[["4"];["3"]];[["4"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]];[["6"];["15"]];[["6"];["16"]];[["6"];["17"]];[["6"];["18"]];[["7"];["15"]];[["7"];["16"]];[["7"];["17"]];[["7"];["18"]];[["8"];["15"]];[["8"];["16"]];[["8"];["17"]];[["8"];["18"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]]])",
            R"([[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["15"]];[["1"];["17"]];[["1"];["3"]];[["2"];["16"]];[["2"];["18"]];[["2"];["2"]];[["2"];["4"]];[["3"];["1"]];[["3"];["15"]];[["3"];["17"]];[["3"];["3"]];[["4"];["16"]];[["4"];["18"]];[["4"];["2"]];[["4"];["4"]];[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
        };

        struct TCase {
            bool Lookup;
            TVector<TString> Impls;
        };
        const std::vector<TCase> cases = {
            {false, {}},
            {false, {"Index1_12/indexImplTable"}},
            {false, {"Index2_21/indexImplTable"}},
            {false, {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
            {true,  {}},
            {true,  {"Index1_12/indexImplTable"}},
            {true,  {"Index2_21/indexImplTable"}},
            {true,  {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
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

            const bool lookupPlan = plan.Contains("InnerJoin (Lookup)") || plan.Contains("TableLookupJoin");
            if (cases[i].Lookup) {
                UNIT_ASSERT_C(lookupPlan, "query #" << i << " expected a lookup join, plan:\n" << plan);
            } else {
                UNIT_ASSERT_C(plan.Contains("InnerJoin (BlockHash)"),
                              "query #" << i << " expected a map join, plan:\n" << plan);
                UNIT_ASSERT_C(!lookupPlan, "query #" << i << " expected no lookup join, plan:\n" << plan);
            }

            if (cases[i].Impls.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " expected only main-table reads, ast:\n" << ast);
            } else {
                for (const auto& impl : cases[i].Impls) {
                    UNIT_ASSERT_C(ast.Contains(impl), "query #" << i << " expected " << impl << ", ast:\n" << ast);
                    UNIT_ASSERT_C(plan.Contains("indexImplTable"), "query #" << i << " expected an index read, plan:\n" << plan);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(IndexLookupJoinChains, PhysicalStagePeephole) {
        const TString schema = R"(
            CREATE TABLE `/Root/t1` (
                a Int32,
                b Int32,
                c Int32,
                d String,
                e Int32,
                f Int64,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t2` (
                a Int32,
                b String,
                c String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t3` (
                a Int32,
                b String,
                c String,
                d Int32,
                e Int32,
                PRIMARY KEY (a, b)
            );

            CREATE TABLE `/Root/t4` (
                a Int32,
                b String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t5` (
                a Int32,
                b Int32,
                c Int32,
                d String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t6` (
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                PRIMARY KEY (a)
            );
        )";

        struct TCase {
            const char* Name;
            TString Query;
            // How many joins are expected to be executed as a stream lookup join.
            ui32 LookupJoins;
            // Set when the old optimizer cannot be used as a reference for the result.
            const char* ExpectedYson = nullptr;
        };

        const TVector<TCase> cases = {
            {"two lookups by primary key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                ORDER BY a;
            )", 2},

            {"lookup probed by a column of a previous lookup", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c, t4.b AS t4b
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                    INNER JOIN `/Root/t4` AS t4 ON t3.e = t4.a
                ORDER BY a;
            )", 2},

            {"three lookups, a filter and an aggregation", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t4.b AS t4b, COUNT(*) AS cnt, SUM(t1.e * t3.d) AS total
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                    INNER JOIN `/Root/t4` AS t4 ON t3.e = t4.a
                WHERE t2.c = "x"
                GROUP BY t4.b
                ORDER BY t4b;
            )", 3},

            {"left join chain keeps unmatched rows", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    LEFT JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                ORDER BY a;
            )", 2},

            {"aggregation over an unmatched left join", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT COUNT(*) AS total, COUNT(t2.b) AS matched, SUM(t1.e) AS e
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t2` AS t2 ON t1.b = t2.a;
            )", 1},

            {"key prefix lookup with several matches per key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, COUNT(*) AS cnt, SUM(t3.d) AS total
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a
                GROUP BY t1.a
                ORDER BY a;
            )", 1},

            {"predicate on the probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a
                WHERE t3.d >= 20
                ORDER BY a, t3c;
            )", 1},

            {"self join by primary key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT x.a AS a, y.e AS e
                FROM `/Root/t1` AS x
                    INNER JOIN `/Root/t1` AS y ON x.b = y.a
                ORDER BY a;
            )", 1},

            {"point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a = 1
                ORDER BY a, t3c;
            )", 1},

            {"inner join over a subquery with a point predicate", R"(
                PRAGMA ydb.CostBasedOptimizationLevel='0';
                SELECT t1.a AS a, t3.b AS t3b
                FROM `/Root/t1` AS t1
                    INNER JOIN (SELECT a, b FROM `/Root/t3` WHERE a = 1) AS t3 ON t1.d = t3.b
                ORDER BY a, t3b;
            )", 1},

            // Point predicate is ok with 2 points for inner join.
            {"several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a IN (1, 2)
                ORDER BY a, t3c;
            )", 1},

            {"null point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a IS NULL
                ORDER BY a, t3c;
            )", 1},

            {"point predicate on a join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                WHERE t3.a = 1
                ORDER BY a, t3c;
            )", 1},

            {"left join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN (SELECT * FROM `/Root/t3` WHERE a = 2) AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 1},

            // Here is a bug for old optimizer, we cannot use stream lookup join for point predicates > 1 with left joins.
            {"left join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN (SELECT * FROM `/Root/t3` WHERE a IN (1, 2)) AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 0,
             R"([[[1];["p1a"]];[[1];["p2a"]];[[2];["p1b"]];[[3];["p1a"]];[[3];["p2a"]];[[4];["p1a"]];)"
             R"([[4];["p2a"]];[[5];["p1a"]];[[5];["p2a"]];[[6];["p1a"]];[[6];["p2a"]];[[7];["p1a"]];[[7];["p2a"]]])"},

            {"semi join from an in subplan", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.c IN (SELECT a FROM `/Root/t3`)
                ORDER BY a;
            )", 1},

            {"left only join from subselect", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.a NOT IN (SELECT a FROM `/Root/t2`)
                ORDER BY a;
            )", 0},

            // No optional keys.
            {"left only join on not null columns", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t6 # 100) Bytes(t6 # 1000)';
                SELECT t6.a AS a
                FROM `/Root/t6` AS t6
                WHERE t6.a NOT IN (SELECT a FROM `/Root/t6` WHERE b = 2)
                ORDER BY a;
            )", 1},

            {"semi join with a filtered probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.c IN (SELECT a FROM `/Root/t3` WHERE d >= 30)
                ORDER BY a;
            )", 1},

            {"left only join with a filtered probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.a NOT IN (SELECT a FROM `/Root/t3` WHERE d >= 30 AND d <= 50)
                ORDER BY a;
            )", 0},

            {"semi join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d IN (SELECT b FROM `/Root/t3` WHERE a = 2)
                ORDER BY a;
            )", 1},

            {"left only join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d NOT IN (SELECT b FROM `/Root/t3` WHERE a = 2)
                ORDER BY a;
            )", 0},

            {"semi join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d IN (SELECT b FROM `/Root/t3` WHERE a IN (1, 2))
                ORDER BY a;
            )", 0,
             R"([[[1]];[[2]];[[3]];[[4]];[[5]];[[6]];[[7]]])"},

            {"left only join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d NOT IN (SELECT b FROM `/Root/t3` WHERE a IN (1, 2))
                ORDER BY a;
            )", 0,
             R"([])"},

            {"join key is not a key prefix", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 0},

            // Need support for cast.
            {"join key types differ", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.f = t2.a
                ORDER BY a;
            )", 0},

            {"inner join with a residual non-key join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t5.d AS t5d
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t5` AS t5 ON t1.b = t5.a AND t1.c = t5.b
                ORDER BY a, t5d;
            )", 1},

            {"left join with a residual non-key join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t5.d AS t5d
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t5` AS t5 ON t1.b = t5.a AND t1.c = t5.b
                ORDER BY a, t5d;
            )", 1},
        };

        struct TQueryResult {
            TString Yson;
            TString Ast;
            TString Plan;
        };

        auto runQueries = [&](bool newRbo) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
            if (!PhysicalStagePeephole) {
                appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
            }
            appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
            appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
            appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
            appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
            appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

            TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto schemeResult = session.ExecuteSchemeQuery(schema).GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

            auto bulkUpsert = [&](const char* table, NYdb::TValueBuilder& rows) {
                auto result = db.BulkUpsert(table, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            };
            // For debugging.
            const bool enableAstDump = false;

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c] : TVector<std::tuple<i32, TString, TString>>{
                         {1, "n1", "x"}, {2, "n2", "y"}, {3, "n3", "x"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .AddMember("c").OptionalString(c)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t2", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b] : TVector<std::tuple<i32, TString>>{{10, "s1"}, {20, "s2"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t4", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d] : TVector<std::tuple<i32, i32, i32, TString>>{
                         {1, 1, 10, "m1"}, {1, 2, 20, "m2"}, {2, 2, 30, "m3"}, {3, 4, 40, "m4"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalInt32(b)
                        .AddMember("c").OptionalInt32(c)
                        .AddMember("d").OptionalString(d)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t5", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b] : TVector<std::tuple<i32, i32>>{{1, 1}, {2, 1}, {3, 2}, {4, 2}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").Int32(a)
                        .AddMember("b").Int32(b)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t6", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d, e] : TVector<std::tuple<i32, TString, TString, i32, i32>>{
                         {1, "a", "p1a", 10, 10}, {1, "b", "p1b", 20, 20}, {2, "a", "p2a", 30, 10},
                         {3, "a", "p3a", 40, 20}, {4, "a", "p4a", 50, 99}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .AddMember("c").OptionalString(c)
                        .AddMember("d").OptionalInt32(d)
                        .AddMember("e").OptionalInt32(e)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t3", rows);
            }

            {
                // A null in the first key column: a point predicate can select it, so the constant
                // cell of a lookup key prefix has to be allowed to hold a null.
                NYdb::TValueBuilder rows;
                rows.BeginList();
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt32(std::nullopt)
                    .AddMember("b").OptionalString("a")
                    .AddMember("c").OptionalString("pna")
                    .AddMember("d").OptionalInt32(60)
                    .AddMember("e").OptionalInt32(30)
                    .EndStruct();
                rows.EndList();
                bulkUpsert("/Root/t3", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d, e] :
                     TVector<std::tuple<i32, std::optional<i32>, std::optional<i32>, TString, i32>>{
                         {1, 1, 1, "a", 2},
                         {2, 1, 1, "b", 3},
                         {3, 2, 2, "a", 1},
                         {4, 3, 4, "a", 5},
                         {5, 9, 3, "a", 7},
                         {6, std::nullopt, 1, "a", 1},
                         {7, 2, std::nullopt, "a", 4}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalInt32(b)
                        .AddMember("c").OptionalInt32(c)
                        .AddMember("d").OptionalString(d)
                        .AddMember("e").OptionalInt32(e)
                        .AddMember("f").OptionalInt64(b ? std::optional<i64>(*b) : std::nullopt)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t1", rows);
            }

            auto explainSession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

            TVector<TQueryResult> results;
            for (const auto& testCase : cases) {
                auto result = session.ExecuteDataQuery(testCase.Query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), testCase.Name << " (new RBO: " << newRbo << "): " << result.GetIssues().ToString());

                auto explained = explainSession.ExecuteQuery(testCase.Query, NYdb::NQuery::TTxControl::NoTx(),
                        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                UNIT_ASSERT_C(explained.IsSuccess(), testCase.Name << ": " << explained.GetIssues().ToString());

                results.push_back({FormatResultSetYson(result.GetResultSet(0)), TString{*explained.GetStats()->GetAst()},
                                   TString{*explained.GetStats()->GetPlan()}});

                if (enableAstDump && newRbo && getenv("DUMP_AST") && TString(testCase.Name) == getenv("DUMP_AST")) {
                    Cout << "=== AST DUMP [" << testCase.Name << "] ===\n"
                         << *explained.GetStats()->GetAst() << "\n=== AST DUMP END ===\n";
                }
            }
            return results;
        };

        auto countOccurrences = [](const TString& text, TStringBuf needle) {
            ui32 count = 0;
            for (size_t pos = text.find(needle); pos != TString::npos; pos = text.find(needle, pos + needle.size())) {
                ++count;
            }
            return count;
        };

        const auto newRboResults = runQueries(/*newRbo=*/true);
        const auto yqlResults = runQueries(/*newRbo=*/false);
        UNIT_ASSERT_VALUES_EQUAL(newRboResults.size(), cases.size());
        UNIT_ASSERT_VALUES_EQUAL(yqlResults.size(), cases.size());

        for (size_t i = 0; i < cases.size(); ++i) {
            const auto& testCase = cases[i];
            const auto& newRbo = newRboResults[i];

            // Check that results are the same.
            if (testCase.ExpectedYson) {
                UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Yson, TString(testCase.ExpectedYson), testCase.Name);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Yson, yqlResults[i].Yson, testCase.Name);
            }

            const auto lookupJoins = countOccurrences(newRbo.Ast, "KqpIndexLookupJoin");
            UNIT_ASSERT_VALUES_EQUAL_C(lookupJoins, testCase.LookupJoins, testCase.Name << ", ast:\n" << newRbo.Ast);
            UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Plan.Contains("TableLookupJoin"), testCase.LookupJoins != 0,
                testCase.Name << ", plan:\n" << newRbo.Plan);
        }
    }

    Y_UNIT_TEST(JoinFiltersBasic) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(true);
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
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(true);
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
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(true);
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
            TScopedRboTraceTitleOverride traceTitle(
                FormatBenchmarkTraceTitle("KqpRboYql", "TPCH_YDB_PERF", qId),
                TString(q.data(), q.size()));
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

    enum EBenchType { TPCH = 0, TPCDS, CLICKBENCH };
    static constexpr std::array<const char*, 3> BenchmarkSchemaPathPrefix{R"(data/)", R"(data/)", R"(data/)"};
    static constexpr std::array<const char*, 3> BenchmarkSchemaPath{R"(schema/tpch.sql)", R"(schema/tpcds.sql)", R"(schema/clickbench.sql)"};
    static constexpr std::array<const char*, 3> BenchmarkQueryPath{R"(data/yql-tpch/q)", R"(data/yql-tpcds/q)", R"(data/yql-clickbench/q)"};
    static constexpr const char* BenchmarkTraceSuiteName = "KqpRboYql";
    static constexpr std::array<const char*, 3> BenchmarkTraceName{"TPCH_YQL", "TPCDS_YQL", "CLICKBENCH_YQL"};
    static constexpr std::array<ui32, 3> BenchmarkQueryCount{22, 99, 43};

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

    void RunPerf_YqlTest(const EBenchType type, const bool columnStore, std::set<ui32>&& queriesStatus, std::set<ui32>&& skipList, const bool newRbo,
                             const bool printStatus = false, const bool compareResults = false, const bool checkNewRBOCbo = false,
                             std::set<ui32>&& queriesWithoutCboCheck = {}) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
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
            TScopedRboTraceTitleOverride traceTitle(
                FormatBenchmarkTraceTitle(BenchmarkTraceSuiteName, BenchmarkTraceName[type], qId),
                q);

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

    void RunWindowFunctionsTest(const bool newRbo, const bool columnStore, TVector<TString>& names, TVector<TString>& results,
                                TVector<TString>& issues) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const TString schema = TStringBuilder() << R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                f Decimal(22,9),
                g Utf8,
                h Double,
                PRIMARY KEY (a)
            )
        )" << (columnStore ? " WITH (Store = Column);" : ";");
        auto schemeResult = tableSession.ExecuteSchemeQuery(schema).GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            // a, b (partition), c (order), d (second partition), e (measure); nullopt is NULL.
            using TCell = std::optional<i64>;
            const TVector<std::tuple<i64, TCell, TCell, TCell, TCell>> rowData = {
                { 1, 1,       10,       100,      5},           // ordinary partition
                { 2, 1,       20,       100,      3},
                { 3, 1,       20,       200,      7},           // tie on the order key c
                { 4, 1,       30,       200,      std::nullopt}, // NULL measure inside a partition
                { 5, 1,       40,       100,      5},           // tie on the measure e
                { 6, 2,       10,       100,      -2},          // negative measure
                { 7, 2,       20,       100,      0},           // zero measure
                { 8, 2,       30,       200,      8},
                { 9, 3,       10,       100,      std::nullopt}, // partition where every measure is NULL
                {10, 3,       20,       100,      std::nullopt},
                {11, 4,       10,       100,      42},          // single row partition
                {12, std::nullopt, 10,  100,      1},           // NULL partition key ...
                {13, std::nullopt, 20,  100,      2},           // ... both rows form one partition
                {14, 5,       std::nullopt, 100,  3},           // NULL order key
                {15, 5,       10,       100,      4},
                {16, 5,       10,       200,      6},           // tie on c, split by the second key d
                {17, 6,       10,       std::nullopt, 9},       // NULL secondary partition key
                {18, 6,       20,       std::nullopt, 11},
                {19, 6,       30,       100,      13},
                {20, 7,       10,       100,      1000000000},  // large measure
            };

            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b, c, d, e] : rowData) {
                auto addCell = [](NYdb::TValueBuilder& builder, const TString& name, const TCell& cell) {
                    builder.AddMember(name);
                    if (cell) {
                        builder.BeginOptional().Int64(*cell).EndOptional();
                    } else {
                        builder.EmptyOptional(NYdb::EPrimitiveType::Int64);
                    }
                };
                rows.AddListItem().BeginStruct();
                rows.AddMember("a").Int64(a);
                addCell(rows, "b", b);
                addCell(rows, "c", c);
                addCell(rows, "d", d);
                addCell(rows, "e", e);
                rows.AddMember("f");
                if (e) {
                    rows.BeginOptional().Decimal(NYdb::TDecimalValue(TStringBuilder() << *e << ".5", 22, 9)).EndOptional();
                } else {
                    rows.EmptyOptional(NYdb::TTypeBuilder().Decimal(NYdb::TDecimalType(22, 9)).Build());
                }
                const THashMap<i64, TString> names = {{10, "ddd"}, {20, "aaa"}, {30, "ccc"}, {40, "bbb"}};
                rows.AddMember("g");
                if (c) {
                    rows.BeginOptional().Utf8(names.at(*c)).EndOptional();
                } else {
                    rows.EmptyOptional(NYdb::EPrimitiveType::Utf8);
                }
                rows.AddMember("h");
                if (c) {
                    rows.BeginOptional().Double(*c / 8.0).EndOptional();
                } else {
                    rows.EmptyOptional(NYdb::EPrimitiveType::Double);
                }
                rows.EndStruct();
            }
            rows.EndList();

            auto seedResult = kikimr.GetTableClient().BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(seedResult.IsSuccess(), seedResult.GetIssues().ToString());
        }

        const TVector<std::pair<TString, TString>> queries = {
            {"partitioned rank", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
            {"partitioned sum", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Sum(Sum(e)) OVER (PARTITION BY b) AS total_sales
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
            {"partitioned average", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Avg(Sum(e)) OVER (PARTITION BY b) AS average_sales
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
           {"global rank", R"(
                PRAGMA YqlSelect = "force";

                SELECT c, Sum(e) AS sales,
                    Rank() OVER (ORDER BY Sum(e) ASC) AS global_rank
                FROM `/Root/t1`
                GROUP BY c
                ORDER BY c;
            )"},
            {"rank with rollup partition expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Rank() OVER (
                        PARTITION BY
                            Grouping(b) + Grouping(c),
                            CASE WHEN Grouping(c) == 0 THEN b ELSE NULL END
                        ORDER BY Sum(e) ASC, c DESC
                    ) AS rank_within_parent
                FROM `/Root/t1`
                GROUP BY ROLLUP(b, c)
                ORDER BY b, c, sales;
            )"},
            {"cumulative sum", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Sum(Sum(e)) OVER (
                        PARTITION BY b
                        ORDER BY c
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_sales
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
            {"cumulative maximum of a value", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Max(e) OVER (
                        PARTITION BY b
                        ORDER BY c, a
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_max
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"sliding frame ending at the current row", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS sliding_sum,
                    Max(e) OVER w AS sliding_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"centred frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS centred_sum,
                    Max(e) OVER w AS centred_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"forward looking frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS ahead_sum,
                    Min(e) OVER w AS ahead_min
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"trailing frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS trailing_sum,
                    Max(e) OVER w AS trailing_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
                ORDER BY a;
            )"},
            {"suffix frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS suffix_sum,
                    Max(e) OVER w AS suffix_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"explicit whole partition frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS partition_sum,
                    Max(e) OVER w AS partition_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range whole partition frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS partition_sum,
                    Max(e) OVER w AS partition_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range default frame written out", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS range_sum,
                    Min(e) OVER w AS range_min
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"range default frame implied", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS range_sum,
                    Min(e) OVER w AS range_min
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                )
                ORDER BY a;
            )"},
            {"range frame with ties", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS range_sum,
                    Count(e) OVER w AS range_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"running decimal aggregates", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, f,
                    Sum(f) OVER w AS running_sum,
                    Avg(f) OVER w AS running_avg,
                    Min(f) OVER w AS running_min,
                    Max(f) OVER w AS running_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"whole partition decimal aggregates", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, f,
                    Sum(f) OVER w AS partition_sum,
                    Avg(f) OVER w AS partition_avg,
                    Count(f) OVER w AS partition_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range decimal aggregates", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, f,
                    Sum(f) OVER w AS range_sum,
                    Avg(f) OVER w AS range_avg
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                )
                ORDER BY a;
            )"},
            {"range frame over a decimal order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, f, e,
                    Sum(e) OVER w AS range_sum,
                    Count(e) OVER w AS range_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY f DESC
                )
                ORDER BY a;
            )"},
            {"forward frame count and average", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Count(e) OVER w AS ahead_count,
                    Avg(e) OVER w AS ahead_avg
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"ranking with a centred frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    RowNumber() OVER w AS row_number_in_group,
                    Rank() OVER w AS rank_in_group,
                    Sum(e) OVER w AS centred_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"global sliding frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, e,
                    Sum(e) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sliding_sum
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"centred decimal average", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, f,
                    Avg(f) OVER w AS centred_avg,
                    Max(f) OVER w AS centred_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"running frame reaching ahead", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS ahead_sum,
                    Count(e) OVER w AS ahead_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"trailing frame count with ranking", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Rank() OVER w AS rank_in_group,
                    Count(e) OVER w AS trailing_count,
                    Avg(e) OVER w AS trailing_avg
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING
                )
                ORDER BY a;
            )"},
            {"suffix aggregates with ranking", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e, f,
                    RowNumber() OVER w AS row_number_in_group,
                    Count(e) OVER w AS suffix_count,
                    Min(e) OVER w AS suffix_min,
                    Avg(f) OVER w AS suffix_avg
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"global whole partition frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, e,
                    Sum(e) OVER () AS total,
                    Max(e) OVER () AS maximum
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"global suffix frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, e,
                    Sum(e) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS suffix_sum
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"range frame around the current value", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS around_sum,
                    Count(e) OVER w AS around_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range frame ending at the current value", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS recent_sum,
                    Max(e) OVER w AS recent_max
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"range frame ending before the current value", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Count(e) OVER w AS earlier_count,
                    Avg(e) OVER w AS earlier_avg
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING
                )
                ORDER BY a;
            )"},
            {"range frame reaching ahead", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS ahead_sum,
                    Rank() OVER w AS rank_in_group
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range frame after the current value", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Count(e) OVER w AS later_count,
                    Min(e) OVER w AS later_min
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN 5 FOLLOWING AND 15 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range suffix frame", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS suffix_sum,
                    DenseRank() OVER w AS dense_rank_in_group
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                )
                ORDER BY a;
            )"},
            {"descending range frame with offsets", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Sum(e) OVER w AS recent_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c DESC
                    RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"global range frame with offsets", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, e,
                    Sum(e) OVER (ORDER BY a RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS nearby_sum
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"ranking over an order expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, d,
                    Rank() OVER w AS rank_in_group,
                    RowNumber() OVER w AS row_number_in_group
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY Abs(c - d), a
                )
                ORDER BY a;
            )"},
            {"range frame over an order expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, d, e,
                    Sum(e) OVER w AS range_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY Abs(c - d)
                )
                ORDER BY a;
            )"},
            {"rows frame over an order expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, d, e,
                    Sum(e) OVER w AS centred_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY Abs(c - d), a
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range offsets over an order expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, d, e,
                    Sum(e) OVER w AS nearby_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY Abs(c - d)
                    RANGE BETWEEN 50 PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"range offsets over a double order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, h, e,
                    Sum(e) OVER w AS nearby_sum,
                    Count(e) OVER w AS nearby_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY h
                    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
                )
                ORDER BY a;
            )"},
            {"range offsets ending before a double order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, h, e,
                    Sum(e) OVER w AS earlier_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY h
                    RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
                ORDER BY a;
            )"},
            {"count star over different frames", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c,
                    Count(*) OVER (PARTITION BY b) AS partition_rows,
                    Count(*) OVER (PARTITION BY b ORDER BY c) AS rows_so_far,
                    Count(*) OVER (PARTITION BY b ORDER BY c, a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nearby_rows,
                    Count(*) OVER (PARTITION BY b ORDER BY c RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS recent_rows
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"range frame over a string order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, g, e,
                    Sum(e) OVER w AS range_sum,
                    Count(e) OVER w AS range_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY g
                )
                ORDER BY a;
            )"},
            {"range frame over a descending string order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, g, e,
                    Sum(e) OVER w AS range_sum,
                    Min(e) OVER w AS range_min
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY g DESC
                )
                ORDER BY a;
            )"},
            {"range frame over two order keys", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, d, e, c,
                    Sum(c) OVER w AS range_sum,
                    Count(c) OVER w AS range_count
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY d, e
                )
                ORDER BY a;
            )"},
            {"ranking and range aggregates over two order keys", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, d, e,
                    Rank() OVER w AS rank_in_group,
                    DenseRank() OVER w AS dense_rank_in_group,
                    Sum(c) OVER w AS range_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY d DESC, e
                )
                ORDER BY a;
            )"},
            {"global range frame over a string order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, g, e,
                    Sum(e) OVER (ORDER BY g) AS range_sum
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"ranking over a string order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, g,
                    Rank() OVER w AS rank_in_group,
                    DenseRank() OVER w AS dense_rank_in_group
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY g
                )
                ORDER BY a;
            )"},
            {"running sum over a string order key", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, g, e,
                    Sum(e) OVER w AS running_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY g, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"multi column partition", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, d, Sum(e) AS sales,
                    Avg(Sum(e)) OVER (PARTITION BY b, d) AS avg_sales
                FROM `/Root/t1`
                GROUP BY b, d, c
                ORDER BY b, d, sales;
            )"},
            {"multi column order", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, d, Sum(e) AS sales,
                    Rank() OVER (
                        PARTITION BY b
                        ORDER BY d ASC, c DESC
                    ) AS rank_in_group
                FROM `/Root/t1`
                GROUP BY b, c, d
                ORDER BY b, c, d;
            )"},
            {"two windows sharing a specification", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Max(e) OVER (
                        PARTITION BY b
                        ORDER BY c, a
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_max,
                    Min(e) OVER (
                        PARTITION BY b
                        ORDER BY c, a
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_min
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"named window shared by several functions", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c, e,
                    Rank() OVER w AS rank_in_group,
                    Max(e) OVER w AS running_max,
                    Sum(e) OVER w AS running_sum
                FROM `/Root/t1`
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                ORDER BY a;
            )"},
            {"named window shared by several functions over aggregates", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Rank() OVER w AS rank_in_group,
                    Sum(Sum(e)) OVER w AS running_sales,
                    Max(Sum(e)) OVER w AS running_max
                FROM `/Root/t1`
                GROUP BY b, c
                WINDOW w AS (
                    PARTITION BY b
                    ORDER BY c
                )
                ORDER BY b, c;
            )"},
            {"named window without an order", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Avg(Sum(e)) OVER w AS avg_sales,
                    Sum(Sum(e)) OVER w AS total_sales
                FROM `/Root/t1`
                GROUP BY b, c
                WINDOW w AS (PARTITION BY b)
                ORDER BY b, c;
            )"},
            {"two windows with different specifications", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Sum(e) AS sales,
                    Avg(Sum(e)) OVER (PARTITION BY b) AS avg_sales,
                    Rank() OVER (PARTITION BY b ORDER BY c) AS rank_in_group
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
            {"window result inside an expression", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c,
                    Sum(e) * 100 / Sum(Sum(e)) OVER (PARTITION BY b) AS revenue_ratio
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
            {"window over a windowed subquery", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, cumulative_sales,
                    Max(cumulative_sales) OVER (
                        PARTITION BY b
                        ORDER BY c
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS peak_cumulative_sales
                FROM (
                    SELECT b, c,
                        Sum(Sum(e)) OVER (
                            PARTITION BY b
                            ORDER BY c
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cumulative_sales
                    FROM `/Root/t1`
                    GROUP BY b, c
                )
                ORDER BY b, c;
            )"},
            {"filter on a window result", R"(
                PRAGMA YqlSelect = "force";
                PRAGMA OrderedColumns;

                SELECT * FROM (
                    SELECT b, c, Sum(e) AS sales,
                        Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
                    FROM `/Root/t1`
                    GROUP BY b, c
                ) WHERE rank_in_group <= 10
                ORDER BY b, c;
            )"},
            {"sort and limit above a window", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, sales, rank_in_group FROM (
                    SELECT b, c, Sum(e) AS sales,
                        Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
                    FROM `/Root/t1`
                    GROUP BY b, c
                ) ORDER BY rank_in_group, sales, b, c LIMIT 100;
            )"},
            {"join on a window result", R"(
                PRAGMA YqlSelect = "force";

                SELECT x.b AS b, x.rank_in_group AS asc_rank, y.rank_in_group AS desc_rank
                FROM (
                    SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c ASC) AS rank_in_group
                    FROM `/Root/t1`
                ) AS x
                JOIN (
                    SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c DESC) AS rank_in_group
                    FROM `/Root/t1`
                ) AS y
                ON x.b == y.b AND x.rank_in_group == y.rank_in_group
                ORDER BY b, asc_rank;
            )"},
            {"input names matching physical window temporaries", R"(
                PRAGMA YqlSelect = "force";

                SELECT `__kqp_win_acc_0_`, `__kqp_win_pos_1_`, `__kqp_win_peer_0_`,
                    Sum(`__kqp_win_acc_0_`) OVER w AS total,
                    Rank() OVER w AS rank
                FROM (
                    SELECT b + 2 AS `__kqp_win_acc_0_`, a + 1 AS `__kqp_win_pos_1_`,
                        c + 1 AS `__kqp_win_peer_0_`
                    FROM `/Root/t1`
                )
                WINDOW w AS (ORDER BY `__kqp_win_peer_0_`)
                ORDER BY `__kqp_win_pos_1_`;
            )"},
            {"two global windows in one select", R"(
                PRAGMA YqlSelect = "force";

                SELECT a, b, c,
                    Rank() OVER (ORDER BY c ASC) AS asc_rank,
                    Rank() OVER (ORDER BY c DESC) AS desc_rank
                FROM `/Root/t1`
                ORDER BY a;
            )"},
            {"rank over group by without aggregates", R"(
                PRAGMA YqlSelect = "force";

                SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c) AS rnk
                FROM `/Root/t1`
                GROUP BY b, c
                ORDER BY b, c;
            )"},
        };

        for (const auto& [name, query] : queries) {
            auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            names.push_back(name);
            if (result.IsSuccess()) {
                results.push_back(TString{FormatResultSetYson(result.GetResultSet(0))});
            } else {
                results.push_back({});
                issues.push_back(TStringBuilder() << name << ": " << result.GetIssues().ToString());
            }
        }
    }

    const THashSet<TString> WindowQueriesNotLoweredYet{
    };

    Y_UNIT_TEST_TWIN(WindowFunctions, ColumnStore) {
        TVector<TString> oldNames, oldResults, oldIssues;
        RunWindowFunctionsTest(/*newRbo=*/false, ColumnStore, oldNames, oldResults, oldIssues);
        UNIT_ASSERT_VALUES_EQUAL_C(oldIssues.size(), 0, "The old optimizer must run every window query: "
                                                            << JoinSeq("; ", oldIssues));

        TVector<TString> newNames, newResults, newIssues;
        RunWindowFunctionsTest(/*newRbo=*/true, ColumnStore, newNames, newResults, newIssues);
        UNIT_ASSERT_VALUES_EQUAL(oldNames.size(), newNames.size());

        const TString table = ColumnStore ? "column" : "row";
        for (ui32 i = 0; i < oldNames.size(); ++i) {
            const auto& name = oldNames[i];
            const bool lowered = !newResults[i].empty();
            if (WindowQueriesNotLoweredYet.contains(name)) {
                UNIT_ASSERT_C(!lowered, "'" << name << "' now runs with the New RBO on a " << table
                                            << " table, remove it from WindowQueriesNotLoweredYet");
                continue;
            }
            UNIT_ASSERT_C(lowered, "The New RBO must run '" << name << "' on a " << table << " table: "
                                                            << JoinSeq("; ", newIssues));
            UNIT_ASSERT_VALUES_EQUAL_C(newResults[i], oldResults[i],
                                       "New RBO returned different rows for '" << name << "' on a " << table << " table");
        }
    }

    Y_UNIT_TEST_TWIN(WindowSortWithoutBlocksUnderWindowFunctionsV2, WindowFunctionsV2) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableWindowFunctionsV2(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                PRIMARY KEY (a)
            ) WITH (Store = Column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        const TString query = TStringBuilder() << R"(
            PRAGMA YqlSelect = "force";
            PRAGMA ydb.WindowFunctionsV2 = ")" << (WindowFunctionsV2 ? "true" : "false") << R"(";

            SELECT a, Sum(c) OVER (PARTITION BY b ORDER BY c) AS s
            FROM `/Root/t1`;
        )";

        auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetAst(), "AST is not available");
        const TString ast(*result.GetStats()->GetAst());

        UNIT_ASSERT_C(ast.Contains("WideChopper"), ast);
        if (WindowFunctionsV2) {
            UNIT_ASSERT_C(!ast.Contains("WideSortBlocks"), ast);
            UNIT_ASSERT_C(ast.Contains("(WideSort "), ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("WideSortBlocks"), ast);
        }
    }

    std::set<ui32> MakePerf_YqlSingleQuerySkipList(const EBenchType type, const ui32 queryId) {
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

        RunPerf_YqlTest(EBenchType::TPCH, /*columnstore=*/true, std::move(expectedSuccessQueries), MakePerf_YqlSingleQuerySkipList(EBenchType::TPCH, queryId),
                        /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true);
    }

    Y_UNIT_TEST(TPCH_9) {
        RunTPCH_YqlSingleQueryTest(9, true);
    }

    void RunPerf_YqlTest(const EBenchType type, ui32 queryId, const bool columnStore, const bool newRbo) {
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

            TScopedRboTraceTitleOverride traceTitle(
                FormatBenchmarkTraceTitle(BenchmarkTraceSuiteName, BenchmarkTraceName[type], queryId),
                q);
            auto queryClient = kikimr.GetQueryClient();
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                                .ExtractValueSync();
            Y_ENSURE(result.IsSuccess());
        }
    }

    void TimePerf_YqlTest(const EBenchType type, ui32 queryId, const bool columnStore, const bool newRbo, const bool useCBO, int nIterations) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        if (!useCBO) {
            appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
        }
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        auto kikimrSettings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);

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

            clock_t the_time;
            double elapsed_time;
            the_time = clock();

            for (int i=0; i<nIterations; i++) {
                auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                                .ExtractValueSync();
                    
                Y_ENSURE(result.IsSuccess());
            }

            TString testName;
            switch(type) {
                case EBenchType::TPCH:
                    testName = "tpch";
                    break;
                case EBenchType::TPCDS:
                    testName = "tpcds";
                    break;
                case EBenchType::CLICKBENCH:
                    testName = "clickbench";
                    break;
                default:
                    Y_ENSURE(false, "Unknown benchmark");
            }

            elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
            Cout << testName << "," << queryId << "," << newRbo << "," << elapsed_time / nIterations << "\n";
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCH) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        // TPCH
        for (int i=1; i<=22; i++) {
            TimePerf_YqlTest(EBenchType::TPCH, i, true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::TPCH, i, true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCDS) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        TVector<int> tpcdsQueries = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, /*17,*/ 18, 19, 20,
                        21, 22, /*23,*/ 24, 25, 26, /*27,*/ 28, 29, 30, 31, 32, 33, 34, 35, /*36,*/ 37, 38, 39, 40,
                        41, 42, 43, /*44,*/ 45, 46, /*47,*/ 48, 49, 50, /*51,*/ 52, 53, 54, 55, 56, /*57,*/ 58, 59, 60,
                        61, 62, 63, 64, 65, 66, 67, 68, 69, /*70,*/ 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                        81, 82, 83, 84, 85, /*86,*/ 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99};

        for (size_t i=0; i<tpcdsQueries.size(); i++) {
            TimePerf_YqlTest(EBenchType::TPCDS, tpcdsQueries[i], true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::TPCDS, tpcdsQueries[i], true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_CLICKBENCH) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        TVector<int> clickQueries = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
                                           22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42};
        for (size_t i=0; i<clickQueries.size(); i++) {
            TimePerf_YqlTest(EBenchType::CLICKBENCH, clickQueries[i], true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::CLICKBENCH, clickQueries[i], true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST_TWIN(DecorrelationEqualNullsJoinKeys, EqualNullsJoinKeys) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetEnableBlockHashJoinEqualNulls(EqualNullsJoinKeys);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (a Int64 NOT NULL, e Int64, primary key(a)) WITH (STORE = column);
            CREATE TABLE `/Root/t2` (a Int64 NOT NULL, b Int64 NOT NULL, c Int64 NOT NULL, primary key(a)) WITH (STORE = column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder t1Rows;
        t1Rows.BeginList();
        for (i64 a = 1; a <= 4; ++a) {
            t1Rows.AddListItem().BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("e").OptionalInt64(a == 1 ? std::nullopt : std::make_optional(a % 4))
                .EndStruct();
        }
        t1Rows.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", t1Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2Rows;
        t2Rows.BeginList();
        for (i64 a = 1; a <= 4; ++a) {
            t2Rows.AddListItem().BeginStruct()
                .AddMember("a").Int64(a)
                .AddMember("b").Int64(a % 3)
                .AddMember("c").Int64(a * a)
                .EndStruct();
        }
        t2Rows.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", t2Rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = R"(
            SELECT t1.a FROM `/Root/t1` as t1
            WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t1.e IS NULL OR t2.b == t1.e) == 16
            ORDER BY t1.a;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explainResult = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
        const auto ast = TString{*explainResult.GetStats()->GetAst()};

        if (EqualNullsJoinKeys) {
            UNIT_ASSERT_C(ast.Contains("EqualNulls"), "expected the join to carry EqualNulls settings, ast:\n" << ast);
            UNIT_ASSERT_C(!ast.Contains("StablePickle"), "expected no StablePickle encoding, ast:\n" << ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("StablePickle"), "expected StablePickle encoded join keys, ast:\n" << ast);
            UNIT_ASSERT_C(!ast.Contains("EqualNulls"), "expected no EqualNulls settings, ast:\n" << ast);
        }

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1]])");
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

    Y_UNIT_TEST(PushFilterBeforeInlining) {
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
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                c Double,
                d Int64,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                PRIMARY KEY(a)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        struct TRow {
            i64 Id;
            i64 A;
            i64 B;
            double C;
            i64 D;
        };
        const TVector<TRow> t1Rows = {
            {1, 1, 1, 10.0, 100},
            {2, 1, 2, 30.0, 200},
            {3, 2, 1, 50.0, 1000},
            {4, 2, 2, 100.0, 2000},
        };

        NYdb::TValueBuilder t1Builder;
        t1Builder.BeginList();
        for (const auto& row : t1Rows) {
            t1Builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(row.Id)
                .AddMember("a").Int64(row.A)
                .AddMember("b").Int64(row.B)
                .AddMember("c").Double(row.C)
                .AddMember("d").Int64(row.D)
                .EndStruct();
        }
        t1Builder.EndList();
        auto upsertResult = tableClient.BulkUpsert("/Root/t1", t1Builder.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2Builder;
        t2Builder.BeginList();
        for (const auto a : {1, 2}) {
            t2Builder.AddListItem().BeginStruct().AddMember("a").Int64(a).EndStruct();
        }
        t2Builder.EndList();
        upsertResult = tableClient.BulkUpsert("/Root/t2", t2Builder.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        const auto check = [&](const TString& name, const TString& query, const TString& expected) {
            auto explainResult = querySession.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(explainResult.IsSuccess(), name + ": " + explainResult.GetIssues().ToString());

            const auto plan = TString{*explainResult.GetStats()->GetPlan()};
            UNIT_ASSERT_C(!plan.Contains("CrossJoin"), name + ":\n" + plan);

            auto result = querySession.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), name + ": " + result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, name);
        };

        check("uncorrelated scalar subquery in ==", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND d == (SELECT MAX(d) FROM `/Root/t1` AS t3);
        )", R"([[[2000]]])");

        check("correlated scalar subquery with non-eliminable domain", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND c < (SELECT AVG(t3.c) FROM `/Root/t1` AS t3
                       WHERE t3.a == t2.a AND t3.b != t1.b);
        )", R"([[[1100]]])");

        check("correlated exists and not exists with non-eliminable domains", R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            SELECT SUM(d) AS total
            FROM `/Root/t1` AS t1, `/Root/t2` AS t2
            WHERE t1.a == t2.a
              AND EXISTS (SELECT * FROM `/Root/t1` AS t3
                          WHERE t3.a == t1.a AND t3.b != t1.b AND t3.c > t1.c)
              AND NOT EXISTS (SELECT * FROM `/Root/t1` AS t4
                              WHERE t4.a == t1.a AND t4.b != t1.b AND t4.d > 1500);
        )", R"([[[100]]])");
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

    /*
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
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], true);

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
    }
    */

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

    Y_UNIT_TEST(MapMetadataAliasFanout) {
        NTests::TIdTestContext f;
        const auto id = f.Id("id"), payload = f.Id("payload"), alias = f.Id("id_alias");
        auto read = f.Read({id, payload});
        read->Props.Metadata.emplace();
        read->Props.Metadata->KeyColumns = {id};
        read->Props.Metadata->ShuffledByColumns = {id};
        auto map = f.Copies(std::move(read), {{alias, id}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(map->Props.Metadata);
        UNIT_ASSERT(map->Props.Metadata->KeyColumns == TOrderedIUs<>{id});
        UNIT_ASSERT(map->Props.Metadata->ShuffledByColumns == TOrderedIUs<>{id});
    }

    Y_UNIT_TEST(DistinctShuffleEliminationPreservesRenamedKeys) {
        NTests::TIdTestContext f;
        const auto id = f.Id(), k = f.Id(), intermediateId = f.Id(), intermediateK = f.Id();
        struct TCase {
            bool Enabled;
            TOrderedIUs<> ShuffledBy, Keys, Expected;
        };
        for (const auto& testCase : TVector<TCase>{
                {true, {id}, {id}, {intermediateId}},
                {true, {id}, {id, k}, {intermediateId}},
                {true, {k, id}, {id, k}, {intermediateK, intermediateId}},
                {true, {id, k}, {id}, {}},
                {true, {}, {id}, {}},
                {false, {id}, {id}, {}}}) {
            f.Props.ColumnLineage.Clear();
            f.Config->OptShuffleElimination = testCase.Enabled;
            auto read = f.Read({id, k});
            read->Props.Metadata.emplace();
            read->Props.Metadata->ShuffledByColumns = testCase.ShuffledBy;
            TAggregationIUs traits, finalTraits;
            TOrderedIUs<> finalKeys;
            for (const auto key : testCase.Keys.Items()) {
                const auto intermediate = key == id ? intermediateId : intermediateK;
                traits.Add(intermediate, TOpAggregationTraits{key, "distinct"});
                finalTraits.Add(key, TOpAggregationTraits{intermediate, "distinct"});
                finalKeys.Append(intermediate);
            }
            auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits), testCase.Keys,
                EOpPhase::Intermediate, true, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            UNIT_ASSERT(aggregate->Props.Metadata->ShuffledByColumns == testCase.Expected);
            auto finalAggregate = MakeIntrusive<TOpAggregate>(std::move(aggregate), std::move(finalTraits),
                std::move(finalKeys), EOpPhase::Final, true, f.Pos);
            finalAggregate->ComputeMetadata(f.RboCtx, f.Props);
            const auto expectedFinal = testCase.Expected.Items().empty() ? TOrderedIUs<>{} : testCase.ShuffledBy;
            UNIT_ASSERT(finalAggregate->Props.Metadata->ShuffledByColumns == expectedFinal);
        }
    }

    Y_UNIT_TEST(AggregateSeparatesSourceStatisticsAndHintIdentityFromProvenance) {
        NTests::TIdTestContext f;
        const auto key = f.Id("key"), copy = f.Id("copy");
        auto read = f.Read({key});
        read->Props.Metadata.emplace();
        read->Props.Metadata->SourceStatsColumns.Add(key);
        auto& lineage = f.Props.ColumnLineage;
        const auto relation = lineage.AddRelation("t", "/Root/table");
        lineage.Add(key, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        read->Props.Metadata->HintRelations.Add(key, relation);
        using TStatsMap = NYql::TOptimizerStatistics::TColumnStatMap;
        f.TypeCtx.ColumnStatisticsByTableName["/Root/table"] = MakeIntrusive<TStatsMap>(
            THashMap<TString, NYql::TColumnStatistics>{{"key", NYql::TColumnStatistics{}}});
        UNIT_ASSERT(BuildOptimizerStatistics(*read, lineage, false, f.TypeCtx).ColumnStatistics);

        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), TAggregationIUs{},
            TOrderedIUs<>{key}, EOpPhase::Undefined, false, f.Pos);
        aggregate->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT_VALUES_EQUAL(lineage.Find(key)->TableName, "/Root/table");
        UNIT_ASSERT(aggregate->Props.Metadata->SourceStatsColumns.Empty());
        UNIT_ASSERT(!BuildOptimizerStatistics(*aggregate, lineage, false, f.TypeCtx).ColumnStatistics);
        UNIT_ASSERT(lineage.GetAliases(aggregate->Props.Metadata->HintRelations) == TVector<TString>{"_aggregate"});

        auto map = f.Copies(std::move(aggregate), {{copy, key}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT_VALUES_EQUAL(lineage.Find(copy)->TableName, "/Root/table");
        UNIT_ASSERT(map->Props.Metadata->SourceStatsColumns.Empty());
        auto hub = TReplicate::Create(std::move(map), f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        second->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(second->Props.Metadata->SourceStatsColumns.Empty());
        UNIT_ASSERT(!BuildOptimizerStatistics(*second, lineage, false, f.TypeCtx).ColumnStatistics);
        UNIT_ASSERT(lineage.GetAliases(second->Props.Metadata->HintRelations) == TVector<TString>{"_aggregate"});
    }

    Y_UNIT_TEST(CopiesAndReplicatePortsTranslateSourceStatisticsEligibility) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), copy = f.Id();
        auto read = f.Read({source});
        read->Props.Metadata.emplace();
        read->Props.Metadata->SourceStatsColumns.Add(source);
        const auto relation = f.Props.ColumnLineage.AddRelation("t", "/Root/table");
        read->Props.Metadata->HintRelations.Add(source, relation);
        f.Props.ColumnLineage.Add(source, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        auto map = f.Copies(std::move(read), {{copy, source}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(map->Props.Metadata->SourceStatsColumns == (TUnorderedIUs{source, copy}));
        auto hub = TReplicate::Create(std::move(map), f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        second->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(second->Props.Metadata->SourceStatsColumns == second->GetOutputIUs());
        UNIT_ASSERT(!second->Props.Metadata->SourceStatsColumns.HasAny({source, copy}));
    }

    Y_UNIT_TEST(StatisticsBuilderRetainsEmptyTableBoundaryForMixedOutputs) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), grouped = f.Id();
        auto boundary = f.Read({source, grouped});
        boundary->Props.Metadata.emplace();
        boundary->Props.Metadata->SourceStatsColumns.Add(source);
        const auto relation = f.Props.ColumnLineage.AddRelation("t", "/Root/table");
        for (const auto id : {source, grouped}) {
            f.Props.ColumnLineage.Add(id, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        }
        using TStatsMap = NYql::TOptimizerStatistics::TColumnStatMap;
        f.TypeCtx.ColumnStatisticsByTableName["/Root/table"] = MakeIntrusive<TStatsMap>(
            THashMap<TString, NYql::TColumnStatistics>{{"key", NYql::TColumnStatistics{}}});
        UNIT_ASSERT(!BuildOptimizerStatistics(*boundary, f.Props.ColumnLineage, false, f.TypeCtx).ColumnStatistics);
    }

    Y_UNIT_TEST(CBOTreeRecomputesPackedOutputIUs) {
        NTests::TIdTestContext f;
        const auto left = f.Id(), stale = f.Id(), current = f.Id();
        auto join = MakeIntrusive<TOpJoin>(f.Read({left}), f.Read({stale}), f.Pos, "Inner", TPairedIUs{});
        UNIT_ASSERT_VALUES_EQUAL(join->GetOutputIUs().Size(), 2);
        join->SetRightInput(f.Read({current}));
        auto tree = MakeIntrusive<TOpCBOTree>(std::move(join), f.Pos);
        auto* treePtr = tree.get();
        auto root = f.Root(std::move(tree), {{left, "left"}, {current, "current"}});
        UNIT_ASSERT(treePtr->GetOutputIUs() == (TUnorderedIUs{left, current}));
    }

    Y_UNIT_TEST(CopiedOperatorPropsDoNotReuseOutputIUs) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), old = f.Id(), fresh = f.Id();
        TMapIUs oldDefinitions;
        oldDefinitions.Add(old, f.Constant());
        auto oldMap = MakeIntrusive<TOpMap>(f.Read({source}), f.Pos, std::move(oldDefinitions));
        UNIT_ASSERT_VALUES_EQUAL(oldMap->GetOutputIUs().Size(), 2);
        TMapIUs newDefinitions;
        newDefinitions.Add(fresh, f.Constant());
        auto newMap = MakeIntrusive<TOpMap>(oldMap->GetInput(), f.Pos, oldMap->Props, std::move(newDefinitions));
        UNIT_ASSERT(newMap->GetOutputIUs() == (TUnorderedIUs{source, fresh}));
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

    Y_UNIT_TEST(DuplicateStageConnectionsKeepAllRequirementsLive) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), c = f.Id(), output = f.Id();
        auto hub = TReplicate::Create(f.Read({a, b, c}), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto* leftPtr = left.get();
        auto* rightPtr = right.get();
        const auto rightA = *right->GetRebindings().Find(a), rightC = *right->GetRebindings().Find(c);
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(output, TUnionInputRow{{a, rightA}});
        auto merge = MakeIntrusive<TOpUnionAll>(std::move(left), std::move(right), f.Pos, std::move(columns));
        auto* mergePtr = merge.get();
        auto root = f.Root(std::move(merge), {{output, "a"}});
        auto& graph = root->PlanProps.StageGraph;
        const auto producerStage = graph.AddStage(), unionStage = graph.AddStage();
        hub->GetInput()->Props.StageId = producerStage;
        leftPtr->Props.StageId = rightPtr->Props.StageId = producerStage;
        leftPtr->Props.StageOutputIndex = 0;
        rightPtr->Props.StageOutputIndex = 1;
        mergePtr->Props.StageId = unionStage;
        graph.Connect(producerStage, unionStage, MakeIntrusive<TMergeConnection>(TSortIUs{{b, {true, true}}}, 0));
        graph.Connect(producerStage, unionStage, MakeIntrusive<TShuffleConnection>(TOrderedIUs<>{rightC}, 1));
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(leftPtr) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveOut(rightPtr) == (TUnorderedIUs{rightA, rightC}));
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b, c}));
    }

    Y_UNIT_TEST(AggregateShuffleEliminationUsesAndPreservesMapConnection) {
        struct TCase {
            bool ShuffleEliminationEnabled;
            bool AggregateShuffleEliminationEnabled;
            bool TwoShuffleKeys;
            bool EliminateShuffle;
        };
        for (const auto& testCase : TVector<TCase>{{false, true, false, false},
                {true, false, false, true}, {true, true, true, false}}) {
            NTests::TIdTestContext f;
            f.Config->OptShuffleElimination = testCase.ShuffleEliminationEnabled;
            f.Config->OptShuffleEliminationForAggregation = testCase.AggregateShuffleEliminationEnabled;
            const auto id = f.Id(), k = f.Id(), payload = f.Id(), result = f.Id();
            auto read = f.Read({id, k, payload});
            const auto inputStage = f.Props.StageGraph.AddSourceStage(NYql::EStorageType::ColumnStorage);
            read->Props.StageId = inputStage;
            read->StorageType = NYql::EStorageType::ColumnStorage;
            read->Props.Metadata.emplace();
            read->Props.Metadata->ShuffledByColumns = testCase.TwoShuffleKeys ? TOrderedIUs<>{id, k} : TOrderedIUs<>{id};
            TAggregationIUs traits;
            traits.Add(result, TOpAggregationTraits{payload, "sum"});
            auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits),
                testCase.TwoShuffleKeys ? TOrderedIUs<>{id} : TOrderedIUs<>{id, k}, EOpPhase::Intermediate, false, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            auto* agg = aggregate.get();
            auto root = f.Root(std::move(aggregate), {{result, "sum"}});
            TAssignStagesStage().RunStage(*root, f.RboCtx);
            const auto aggregateStage = *agg->Props.StageId;
            const auto& connections = root->PlanProps.StageGraph.GetConnections(inputStage, aggregateStage);
            UNIT_ASSERT_VALUES_EQUAL(connections.size(), 1);
            if (testCase.EliminateShuffle) {
                UNIT_ASSERT(agg->Props.Metadata->ShuffledByColumns == TOrderedIUs<>{id});
                UNIT_ASSERT(IsConnection<TMapConnection>(connections.front()));
                UNIT_ASSERT(TPropagateAggregateThroughStageRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
                UNIT_ASSERT_VALUES_EQUAL(*root->GetInput()->Props.StageId, inputStage);
                const auto& after = root->PlanProps.StageGraph.GetConnections(inputStage, aggregateStage);
                UNIT_ASSERT_VALUES_EQUAL(after.size(), 1);
                UNIT_ASSERT(IsConnection<TMapConnection>(after.front()));
            } else {
                UNIT_ASSERT(agg->Props.Metadata->ShuffledByColumns.Items().empty());
                UNIT_ASSERT(IsConnection<TShuffleConnection>(connections.front()));
            }
        }
    }

    Y_UNIT_TEST(PhysicalSemiJoinUsesPerEdgeLiveIn) {
        NTests::TIdTestContext f;
        const auto a = f.Id("a"), b = f.Id("b");
        auto read = f.Read({a, b});
        f.SetType(*read);
        auto hub = TReplicate::Create(std::move(read), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto rightA = *right->GetRebindings().Find(a);
        f.SetType(*left);
        f.SetType(*right);
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "LeftSemi", TPairedIUs{{a, rightA}});
        auto* joined = join.get();
        join->Props.JoinAlgo = EJoinAlgoType::GraceJoin;
        auto root = f.Root(std::move(join), {{a, "a"}, {b, "b"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveIn(joined, 0) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveIn(joined, 1) == TUnorderedIUs{rightA});
        const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
        auto build = [&](bool blocks) {
            return TPhysicalJoinBuilder(*joined, f.ExprCtx, f.Pos, names).BuildPhysicalOp(
                f.ExprCtx.NewArgument(f.Pos, "left"), f.ExprCtx.NewArgument(f.Pos, "right"), blocks, f.TypeCtx);
        };
        auto physical = build(false);
        TExprNode::TListType cores;
        CollectCallableNodes(physical, "GraceJoinCore", cores);
        UNIT_ASSERT_VALUES_EQUAL(cores.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cores.front()->Child(6)->ChildrenSize(), 0);
        physical = build(true);
        cores.clear();
        CollectCallableNodes(physical, "BlockHashJoinCore", cores);
        UNIT_ASSERT_VALUES_EQUAL(cores.size(), 1);
        const auto blocks = cores.front()->ChildPtr(1);
        UNIT_ASSERT(blocks->IsCallable("WideToBlocks"));
        const auto flow = blocks->HeadPtr();
        UNIT_ASSERT(flow->IsCallable("FromFlow"));
        const auto expand = flow->HeadPtr();
        UNIT_ASSERT(expand->IsCallable("ExpandMap"));
        const auto lambda = expand->TailPtr();
        UNIT_ASSERT(lambda->IsLambda());
        UNIT_ASSERT_VALUES_EQUAL(lambda->ChildrenSize(), 2);
        UNIT_ASSERT(lambda->Tail().IsCallable("Member"));
        UNIT_ASSERT_VALUES_EQUAL(lambda->Tail().Tail().Content(), names.Get(rightA));
    }

    Y_UNIT_TEST(PhysicalAggregationDoesNotEmitDeadKeyColumns) {
        NTests::TIdTestContext f;
        const auto key = f.Id("key"), value = f.Id("value"), sum = f.Id("sum_value");
        auto read = f.Read({key, value});
        f.SetType(*read);
        TAggregationIUs traits;
        traits.Add(sum, TOpAggregationTraits{value, "sum"});
        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits),
            TOrderedIUs<>{key}, EOpPhase::Final, false, f.Pos);
        f.SetType(*aggregate);
        auto* agg = aggregate.get();
        auto root = f.Root(std::move(aggregate), {{sum, "sum_value"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(agg) == TUnorderedIUs{sum});
        const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
        auto physical = TPhysicalAggregationBuilder(*agg, f.ExprCtx, f.Pos, names, true)
            .BuildPhysicalOp(f.ExprCtx.NewArgument(f.Pos, "input"), std::nullopt);
        TExprNode::TListType narrowMaps;
        CollectCallableNodes(physical, "NarrowMap", narrowMaps);
        UNIT_ASSERT_VALUES_EQUAL(narrowMaps.size(), 1);
        const auto body = TCoLambda(narrowMaps.front()->ChildPtr(1)).Body().Ptr();
        UNIT_ASSERT(body->IsCallable("AsStruct"));
        UNIT_ASSERT_VALUES_EQUAL(body->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(body->Head().Head().Content(), names.Get(sum));
    }

    Y_UNIT_TEST(MergeUnionAllFlattensLeftDeepChain) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), p = f.Id(), b = f.Id(), q = f.Id(), c = f.Id(), r = f.Id();
        const auto innerA = f.Id(), innerP = f.Id(), outA = f.Id(), outP = f.Id();
        auto first = f.Read({a, p}), second = f.Read({b, q}), third = f.Read({c, r});
        const TVector<IOperator*> leaves{first.get(), second.get(), third.get()};
        auto inner = f.Union(std::move(first), std::move(second), {{innerA, a, b}, {innerP, p, q}});
        auto outer = f.Union(std::move(inner), std::move(third), {{outA, innerA, c}, {outP, innerP, r}});
        auto* merged = outer.get();
        auto root = f.Root(std::move(outer), {{outA, "a"}, {outP, "payload"}});
        UNIT_ASSERT(TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_C(root->GetInput().Get() == merged, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(merged->GetChildCount(), 3);
        for (size_t i = 0; i < leaves.size(); ++i) {
            UNIT_ASSERT_C(merged->GetChild(i).Get() == leaves[i], root->PlanToString(f.ExprCtx));
        }
        UNIT_ASSERT(merged->GetColumns().Keys() == (TUnorderedIUs{outA, outP}));
        UNIT_ASSERT(merged->GetColumns().Find(outA)->Inputs == (TUnionInputRow{{a, b, c}}.Inputs));
        UNIT_ASSERT(merged->GetColumns().Find(outP)->Inputs == (TUnionInputRow{{p, q, r}}.Inputs));
    }

    Y_UNIT_TEST(MergeUnionAllFlattensNestedBranches) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), c = f.Id(), d = f.Id(), left = f.Id(), right = f.Id(), output = f.Id();
        auto first = f.Read({a}), second = f.Read({b}), third = f.Read({c}), fourth = f.Read({d});
        const TVector<IOperator*> leaves{first.get(), second.get(), third.get(), fourth.get()};
        auto outer = f.Union(f.Union(std::move(first), std::move(second), {{left, a, b}}),
            f.Union(std::move(third), std::move(fourth), {{right, c, d}}), {{output, left, right}});
        auto* merged = outer.get();
        auto root = f.Root(std::move(outer), {{output, "a"}});
        UNIT_ASSERT(TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_C(root->GetInput().Get() == merged, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(merged->GetChildCount(), 4);
        for (size_t i = 0; i < leaves.size(); ++i) {
            UNIT_ASSERT_C(merged->GetChild(i).Get() == leaves[i], root->PlanToString(f.ExprCtx));
        }
        UNIT_ASSERT(merged->GetColumns().Find(output)->Inputs == (TUnionInputRow{{a, b, c, d}}.Inputs));
    }

    Y_UNIT_TEST(MergeUnionAllKeepsOrderedUnion) {
        for (const bool innerOrdered : {false, true}) {
            NTests::TIdTestContext f;
            const auto a = f.Id(), b = f.Id(), c = f.Id(), innerId = f.Id(), output = f.Id();
            auto inner = f.Union(f.Read({a}), f.Read({b}), {{innerId, a, b}}, innerOrdered);
            auto* innerPtr = inner.get();
            auto outer = f.Union(std::move(inner), f.Read({c}), {{output, innerId, c}}, !innerOrdered);
            auto root = f.Root(std::move(outer), {{output, "a"}});
            UNIT_ASSERT(!TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
            UNIT_ASSERT_VALUES_EQUAL(root->GetInput()->GetChildCount(), 2);
            UNIT_ASSERT_C(root->GetInput()->GetChild(0).Get() == innerPtr, root->PlanToString(f.ExprCtx));
        }
    }

    Y_UNIT_TEST(MergeUnionAllKeepsSharedUnion) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), innerId = f.Id(), output = f.Id();
        auto hub = TReplicate::Create(f.Union(f.Read({a}), f.Read({b}), {{innerId, a, b}}),
            f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto local = *right->GetRebindings().Find(innerId);
        auto root = f.Root(f.Union(std::move(left), std::move(right), {{output, innerId, local}}), {{output, "a"}});
        UNIT_ASSERT(!TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_VALUES_EQUAL(root->GetInput()->GetChildCount(), 2);
        UNIT_ASSERT_C(root->GetInput()->GetChild(0)->Kind == EOperator::Replicate, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_C(root->GetInput()->GetChild(1)->Kind == EOperator::Replicate, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT(hub->GetInput()->Kind == EOperator::UnionAll);
    }

    Y_UNIT_TEST(TPCH_YQL) {
        // RunTPCHYqlBenchmark(/*columnstore*/ true, {}, {}, /*new rbo*/ false);
        RunPerf_YqlTest(EBenchType::TPCH, /*columnstore=*/true, {
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
                        /*rbo never finish*/ {}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true,
                        /*queriesWithoutCboCheck=*/{13});
    }

    // Compiled 97 from 99.
    Y_UNIT_TEST(TPCDS_YQL) {
        RunPerf_YqlTest(EBenchType::TPCDS, /*columnstore=*/true,
                        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, /*17,*/ 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                        41, 42, 43, /*44,*/ 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
                        61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                        81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99},
                        /*rbo never finish*/ {}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true,
                        // Still explain these queries, but do not require the CBO stats invariant when CBO is explicitly disabled
                        // in the query or until the known gaps are fixed.
                        /*queriesWithoutCboCheck=*/{4, 15, 31, 58, 64, 66, 72, 78, 85});
    }

    Y_UNIT_TEST(ClickBench_YQL) {
        // Queries - q19, q29, q40, q43 not supported, because of yql error - not support `GROUP BY ... AS <alias>`.
        RunPerf_YqlTest(EBenchType::CLICKBENCH, /*columnstore=*/true,
                        /*queriesStatus=*/{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
                                           22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42},
                        /*skipList=*/{}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true,
                        /*checkNewRBOCbo=*/false, /*queriesWithoutCboCheck=*/{});
    }

    Y_UNIT_TEST(ClickBench_YQL_Single) {
        const ui32 query = 25;
        auto skipList = MakePerf_YqlSingleQuerySkipList(EBenchType::CLICKBENCH, query);
        RunPerf_YqlTest(EBenchType::CLICKBENCH, /*columnstore=*/true,
                        /*queriesStatus=*/{query},
                        /*skipList=*/std::move(skipList), /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true,
                        /*checkNewRBOCbo=*/false, /*queriesWithoutCboCheck=*/{});
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
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id == 1)) OR bar.id == 2 order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (NOT EXISTS(SELECT foo.id FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id2 > bar.id2)) OR bar.id == 0 order by bar.id;
            )",
            R"(
                SELECT bar.id FROM `/Root/bar` as bar where (bar.id2 NOT IN (SELECT foo.id2 FROM `/Root/foo` as foo where foo.id == bar.id AND foo.id == 1)) OR bar.id == 2 order by bar.id;
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
            R"([[0];[2];[3]])",
            R"([[0];[1];[2];[3]])",
            R"([[0];[2];[3]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "query " << i << ": " << result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), results[i], "query " << i);
        }
    }

    void TestDecorrelation(bool columnTables, bool equalNullsJoinKeys = false) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetEnableBlockHashJoinEqualNulls(equalNullsJoinKeys);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString schemeQuery;
        for (const auto* table : {"t1", "t2", "t3"}) {
            schemeQuery += TStringBuilder() << R"(
                CREATE TABLE `/Root/)" << table << R"(` (
                    a   Int64   NOT NULL,
                    b   Int64   NOT NULL,
                    c   Int64   NOT NULL,
                    d   String,
                    e   Int64,
                    primary key(a)
                ))" << (columnTables ? " WITH (STORE = column)" : "") << ";\n";
        }

        auto schemeResult = session.ExecuteSchemeQuery(schemeQuery).GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        //   t1: a in [1, 12], b = a % 4, c = 10 * a, d = 'g' || (a % 3), e = a % 4 except null for a == 1
        //   a | 1   2   3   4   5   6   7   8   9   10  11  12
        //   b | 1   2   3   0   1   2   3   0   1   2   3   0
        //   c | 10  20  30  40  50  60  70  80  90  100 110 120
        //   d | g1  g2  g0  g1  g2  g0  g1  g2  g0  g1  g2  g0
        //   e | -   2   3   0   1   2   3   0   1   2   3   0
        //
        //   t2: a in [1, 12], b = a % 3, c = a * a, d = 'g' || (a % 4), e = a % 4 except null for a == 1
        //   a | 1   2   3   4   5   6   7   8   9   10  11  12
        //   b | 1   2   0   1   2   0   1   2   0   1   2   0
        //   c | 1   4   9   16  25  36  49  64  81  100 121 144
        //   d | g1  g2  g3  g0  g1  g2  g3  g0  g1  g2  g3  g0
        //   e | -   2   3   0   1   2   3   0   1   2   3   0
        //
        //   t3: a in [1, 6], b = a % 2, c = a, d = 'g' || (a % 2), e as above
        auto fill = [&](const TString& table, i64 rowCount, auto&& b, auto&& c, auto&& d) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 a = 1; a <= rowCount; ++a) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("a").Int64(a)
                    .AddMember("b").Int64(b(a))
                    .AddMember("c").Int64(c(a))
                    .AddMember("d").String(d(a))
                    .AddMember("e").OptionalInt64(a == 1 ? std::nullopt : std::make_optional(a % 4))
                    .EndStruct();
            }
            rows.EndList();

            auto result = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto group = [](i64 n) { return TString("g") + std::to_string(n); };
        fill("/Root/t1", 12, [](i64 a) { return a % 4; }, [](i64 a) { return 10 * a; }, [&](i64 a) { return group(a % 3); });
        fill("/Root/t2", 12, [](i64 a) { return a % 3; }, [](i64 a) { return a * a; }, [&](i64 a) { return group(a % 4); });
        fill("/Root/t3", 6, [](i64 a) { return a % 2; }, [](i64 a) { return a; }, [&](i64 a) { return group(a % 2); });

        auto queryClient = kikimr.GetQueryClient();

        std::vector<std::pair<std::string, std::string>> cases = {
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT max(t2.c) - 44 FROM `/Root/t2` as t2)
                ORDER BY t1.a;
             )",
             R"([[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c * 2 >= (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
                ORDER BY t1.a;
             )",
             R"([[5];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT max(t2.c - t1.b) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (SELECT t2.a - t1.a FROM `/Root/t2` as t2 WHERE t2.a >= t1.a)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.a IN (SELECT t2.a + t1.b FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
                ORDER BY t1.a;
             )",
             R"([[5];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (SELECT CAST(count(*) AS Int64) FROM `/Root/t2` as t2 WHERE t2.a <= t1.a GROUP BY t2.b)
                ORDER BY t1.a;
             )",
             R"([[1];[5];[6];[7];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a <= t1.a GROUP BY t2.b HAVING count(*) > 3)
                ORDER BY t1.a;
             )",
             R"([[10];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (NOT EXISTS (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a == t1.a AND t2.b == 0)) OR t1.a == 1
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[7];[8];[10];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.a NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a == t1.a AND t2.b == 0)) OR t1.a == 3
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[7];[8];[10];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.a IN (
                    SELECT t2.a + t1.b FROM `/Root/t2` as t2 WHERE t2.b == 0
                    UNION ALL
                    SELECT t3.a * 2 FROM `/Root/t3` as t3 WHERE t3.a <= t1.b
                )
                ORDER BY t1.a;
             )",
             R"([[2];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (
                    SELECT t2.a FROM `/Root/t2` as t2 JOIN `/Root/t3` as t3 ON t2.b == t3.b
                    WHERE t2.a > t1.a * 2 GROUP BY t2.a
                )
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (
                    SELECT max(t2.c) FROM `/Root/t2` as t2
                    WHERE t2.a <= t1.a AND t2.b IN (SELECT t3.b FROM `/Root/t3` as t3 WHERE t3.a <= t2.a)
                )
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.d IN (SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 1)
                ORDER BY t1.a;
             )",
             R"([[3];[4];[5]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.d NOT IN (SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 1)) OR t1.a == 3
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[6];[7];[8];[9];[10];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.b NOT IN (SELECT IF(t2.a == 1, NULL, t2.b) FROM `/Root/t2` as t2)) OR t1.a == 3
                ORDER BY t1.a;
             )",
             R"([[3]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.b NOT IN (SELECT IF(t2.a == 3, NULL, t2.b) FROM `/Root/t2` as t2 WHERE t2.a == t1.a)) OR t1.a == 1
                ORDER BY t1.a;
             )",
             R"([[1];[4];[5];[6];[7];[8];[9];[10];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2)) OR t1.a == 12
                ORDER BY t1.a;
             )",
             R"([[3];[7];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a > 100)) OR t1.a == 12
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (t1.e NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a == t1.a)) OR t1.a == 12
                ORDER BY t1.a;
             )",
             R"([[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (
                    SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a > t1.a * 2 GROUP BY t2.a
                )
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (
                    SELECT t2.d FROM `/Root/t2` as t2 WHERE t2.b == t1.b GROUP BY t2.d HAVING count(*) > 0
                )
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.a == t1.a)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == t1.e) IS NULL
                ORDER BY t1.a;
             )",
             R"([[1];[3];[7];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t1.e IS NULL OR t2.b == t1.e) == 144
                ORDER BY t1.a;
             )",
             R"([[1];[4];[8];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.b == t1.b) == 4
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.b == t1.b) == 0
                ORDER BY t1.a;
             )",
             R"([[3];[7];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.e == t1.e)
                ORDER BY t1.a;
             )",
             R"([[2];[3];[4];[5];[6];[7];[8];[9];[10];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0) AND t2.a <= 3)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0) AND t2.a <= 3)
                ORDER BY t1.a;
             )",
             R"([[3];[7];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 1) AND t2.a <= 3)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b NOT IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 1) AND t2.a <= 3)
                ORDER BY t1.a;
             )",
             R"([[3];[7];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT max(t2.c) FROM `/Root/t2` as t2 WHERE t2.e == t1.e) IS NULL
                ORDER BY t1.a;
             )",
             R"([[1]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE (SELECT count(*) FROM `/Root/t2` as t2 WHERE t2.e == t1.e) == 0
                ORDER BY t1.a;
             )",
             R"([[1]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE NOT (t1.c > 100) AND t1.a IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 3)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.a == t1.a + 9)
                  AND t1.a IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 5)
                ORDER BY t1.a;
             )",
             R"([[4];[5]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.a NOT IN (SELECT t2.e FROM `/Root/t2` as t2)
                ORDER BY t1.a;
             )",
             R"([])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.e NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 2)
                ORDER BY t1.a;
             )",
             R"([[3];[4];[7];[8];[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.a NOT IN (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.a <= 10)
                ORDER BY t1.a;
             )",
             R"([[11];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT min(t2.c) FROM `/Root/t2` as t2 WHERE t2.b == COALESCE(t1.e, 0))
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.a == t3.a
                              WHERE t3.c == t1.a)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.b == t3.b
                              WHERE t3.a == t1.e)
                ORDER BY t1.a;
             )",
             R"([[2];[3];[5];[6];[7];[9];[10];[11]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE NOT EXISTS (SELECT 1 FROM `/Root/t2` as t2 RIGHT JOIN `/Root/t3` as t3 ON t2.a == t3.a
                                  WHERE t2.c == t1.a)
                ORDER BY t1.a;
             )",
             R"([[2];[3];[5];[6];[7];[8];[10];[11];[12]])"},

            // ORDER BY without LIMIT does not change the subquery result.
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a ORDER BY t2.a DESC)
                ORDER BY t1.a;
             )",
             R"([[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.a == t1.a ORDER BY t2.c)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])"},

            // ORDER BY ... LIMIT applies per outer row.
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE EXISTS (SELECT 1 FROM `/Root/t2` as t2 WHERE t2.b == t1.b LIMIT 1)
                ORDER BY t1.a;
             )",
             R"([[1];[2];[4];[5];[6];[8];[9];[10];[12]])"},

            // Without the limit 12 qualifies too: t2.b == 0 for t2.a == 9.
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a ORDER BY t2.a DESC LIMIT 2)
                ORDER BY t1.a;
             )",
             R"([[4];[5];[6];[8];[9];[10]])"},

            // The second largest t2.c per t2.b: 81, 49, 64.
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c DESC LIMIT 1 OFFSET 1)
                ORDER BY t1.a;
             )",
             R"([[5];[9];[10];[12]])"},

            {R"(
                SELECT t1.a, (SELECT t2.a FROM `/Root/t2` as t2 WHERE t2.b == t1.b AND t2.a > t1.a ORDER BY t2.a LIMIT 1) AS next
                FROM `/Root/t1` as t1
                ORDER BY t1.a;
             )",
             R"([[1;[4]];[2;[5]];[3;#];[4;[6]];[5;[7]];[6;[8]];[7;#];[8;[9]];[9;[10]];[10;[11]];[11;#];[12;#]])"},

            // Not shared, so not copied: the aggregate some of the inner scalar subquery is decorrelated in place.
            // The inner subquery keeps t2.a <= 6.
            {R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (
                    SELECT t2.b FROM `/Root/t2` as t2
                    WHERE t2.a < t1.a AND t2.c >= (SELECT t3.c FROM `/Root/t3` as t3 WHERE t3.a == t2.a)
                    GROUP BY t2.b, t2.d
                )
                ORDER BY t1.a;
             )",
             R"([[4];[5];[6];[8];[9];[10];[12]])"},
        };

        for (ui32 i = 0; i < cases.size(); ++i) {
            const auto& [query, expected] = cases[i];
            auto querySession = queryClient.GetSession().GetValueSync().GetSession();
            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "query " << i << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, "query " << i);
        }

        const std::vector<std::string> multiRowQueries = {
            R"(SELECT t1.a FROM `/Root/t1` as t1 WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2) ORDER BY t1.a;)",

            R"(SELECT t1.a FROM `/Root/t1` as t1
               WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b)
               ORDER BY t1.a;)",

            R"(SELECT t1.a FROM `/Root/t1` as t1
               WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b GROUP BY t2.c)
               ORDER BY t1.a;)",

            // Dropping the sort keeps the check.
            R"(SELECT t1.a FROM `/Root/t1` as t1
               WHERE t1.c > (SELECT t2.c FROM `/Root/t2` as t2 WHERE t2.b == t1.b ORDER BY t2.c)
               ORDER BY t1.a;)",
        };

        for (ui32 i = 0; i < multiRowQueries.size(); ++i) {
            auto errorSession = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                errorSession.ExecuteQuery(TString(multiRowQueries[i]), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "multi row query " << i << " unexpectedly succeeded");
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Scalar subquery returned more than one row",
                                          "multi row query " << i);
        }

        // The grouping set branches share the correlated filter through a Replicate. Decorrelation copies a shared
        // correlated operator for each branch, but a copy is evaluated again, so with a nondeterministic expression
        // the branches could see other rows. Every predicate below keeps every row: once such a filter is supported,
        // the expected result is [[4];[5];[6];[8];[9];[10];[12]].
        const std::vector<std::string> nondeterministicPredicates = {
            // Random returns a value below 1.
            "Random(t2.a) < 2.0",
            // Only the peephole path turns a call without arguments into a parameter.
            R"(CurrentUtcTimestamp() > Timestamp("2000-01-01T00:00:00Z"))",
            R"(CurrentUtcDate(t2.a) > Date("2000-01-01"))",
            // This UDF is deterministic, but nothing says so for UDFs in general.
            "Digest::IntHash64(CAST(t2.a AS Uint64)) >= 0",
        };

        for (const auto& predicate : nondeterministicPredicates) {
            const TString query = TStringBuilder() << R"(
                SELECT t1.a FROM `/Root/t1` as t1
                WHERE t1.b IN (
                    SELECT t2.b FROM `/Root/t2` as t2 WHERE t2.a < t1.a AND )" << predicate << R"(
                    GROUP BY ROLLUP(t2.b, t2.d)
                )
                ORDER BY t1.a;
            )";
            const auto status = queryClient.RetryQuerySync([&](NYdb::NQuery::TSession session) -> NYdb::TStatus {
                return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(!status.IsSuccess(), predicate << " unexpectedly succeeded");
            UNIT_ASSERT_STRING_CONTAINS_C(status.GetIssues().ToString(), "correlation cannot be pushed through Replicate", predicate);
        }
    }

    Y_UNIT_TEST_QUAD(Decorrelation, ColumnStore, EqualNullsJoinKeys) {
        TestDecorrelation(ColumnStore, EqualNullsJoinKeys);
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
            )",
            R"(
                SELECT a, a AS x FROM `/Root/t1`
                ORDER BY b DESC;
            )",
            R"(
                SELECT a, a AS x FROM `/Root/t1`
                ORDER BY b DESC
                LIMIT 1;
            )"
        };

        std::vector<std::string> results = {
            R"([[3];[2];[1];[0]])",
            R"([[3;[4]];[2;[3]];[1;[2]];[0;[1]]])",
            R"([[3];[2];[2];[1];[1];[0];[0]])",
            R"([[3;3];[2;2];[1;1];[0;0]])",
            R"([[3;3]])"
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
	            b Int64 NOT NULL,
                primary key(a, b)
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
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b desc limit 1;
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
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b asc limit 1;
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
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.b desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b desc limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            const TString explainQuery = TStringBuilder()
                << "PRAGMA ydb.EnableNewRBOPhysicalStagePeephole = \"true\";\n" << query;
            auto result =
                session.ExecuteQuery(explainQuery, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "SortBlocks"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqCnMerge"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
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

    void TestBlockHashCombine(bool columnTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDqHashOperatorsUseBlocks(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
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
        for (i64 i = 1; i <= 3; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(10)
                .EndStruct();
        }
        rows.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        const std::vector<std::string> queries = {
            R"(
                select sum(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select min(t1.a), max(t1.a), count(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.a), max(t1.b) from `/Root/t1` as t1;
            )",
        };

        const std::vector<std::string> results = {
            R"([[6;[10]]])",
            R"([[1;3;3u;[10]]])",
            R"([[[6];[10]]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "DqPhyHashCombine"), 2, ast);
            // Row tables convert to blocks around each combine, column tables stay in blocks
            // from the block read up to the result stage.
            UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "WideToBlocks"), columnTables ? 0 : 2, ast);
            UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "WideFromBlocks"), columnTables ? 1 : 2, ast);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST_TWIN(BlockHashCombine, ColumnStore) {
        TestBlockHashCombine(ColumnStore);
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

    bool HasFallbackIssue(const NYdb::NIssue::TIssues& issues) {
        for (const auto& issue : issues) {
            if (issue.GetSeverity() == NYdb::NIssue::ESeverity::Info &&
                issue.GetMessage().find("Compilation with the new RBO failed") != std::string::npos)
            {
                return true;
            }
        }
        return false;
    }

    void TestFallbackToYql(bool fallbackToYqlEnabled, const std::vector<std::string>& queries,
                           const std::vector<std::pair<ui32, ui32>>& expectedCompileCounters, const std::vector<bool>& expectedResult,
                           NQuery::EExecMode execMode) {
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
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(execMode))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), expectedResult[i], result.GetIssues().ToString());

            const bool expectedFallbackIssue = fallbackToYqlEnabled && expectedCompileCounters[i].second == 1;
            UNIT_ASSERT_VALUES_EQUAL_C(HasFallbackIssue(result.GetIssues()), expectedFallbackIssue,
                                       result.GetIssues().ToString());

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
            // Simple query with a fallback pragma
            R"(
                PRAGMA ydb.OptFallbackToLegacyOptimizer = 'true';
                select t1.a from `/Root/t1` as t1;
            )"
        };

        return queries;
    }

    std::vector<std::pair<ui32, ui32>> GetCompileCountersToTestFallbackToYql() {
        // Represents the number of successes and fails for each query with new RBO compiler pipeline.
        std::vector<std::pair<ui32, ui32>> expectedCompileCounters = {
            {0, 1},
            {1, 0},
            {0, 1}
        };

        return expectedCompileCounters;
    }

    Y_UNIT_TEST(FallbackToYqlEnabled) {
        // All queries should succeded because fallback to yql is enabled.
        const std::vector<bool> expectedResult{true, true, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/true, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Explain);
    }

    Y_UNIT_TEST(FallbackToYqlDisabled) {
        // First 2 queries should fail because fallback to yql is disabled.
        const std::vector<bool> expectedResult{false, true, false};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/false, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Explain);
    }

    Y_UNIT_TEST(FallbackToYqlEnabledExecute) {
        // All queries should succeded because fallback to yql is enabled.
        const std::vector<bool> expectedResult{true, true, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/true, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Execute);
    }

    Y_UNIT_TEST(FallbackToYqlDisabledExecute) {
        // First 2 queries should fail because fallback to yql is disabled.
        const std::vector<bool> expectedResult{false, true, false};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/false, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Execute);
    }


    void CollectHashShuffleFuncs(const NJson::TJsonValue& planNode, TVector<TString>& hashFuncs) {
        if (!planNode.IsMap()) {
            return;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto nodeType = planMap.find("Node Type");
                nodeType != planMap.end() && nodeType->second.GetStringSafe() == "HashShuffle") {
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
                nodeType != planMap.end() && nodeType->second.GetStringSafe() == "HashShuffle") {
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
            return nodeType->second.GetStringSafe() == "HashShuffle";
        }

        return false;
    }

    bool IsJoinPlanNode(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
            for (const auto& opNode : operators->second.GetArraySafe()) {
                const auto& op = opNode.GetMapSafe();
                if (op.contains("JoinKind")) {
                    return true;
                }
            }
        }

        return false;
    }

    ui32 CountJoinPlanNodes(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return 0;
        }

        ui32 count = IsJoinPlanNode(planNode) ? 1 : 0;

        const auto& planMap = planNode.GetMapSafe();
        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                count += CountJoinPlanNodes(child);
            }
        }

        return count;
    }

    ui32 CountJoinPlanNodes(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);
        return CountJoinPlanNodes(planRoot.GetMapSafe().at("SimplifiedPlan"));
    }

    bool HasJoinWithBothInputsHashShuffled(const NJson::TJsonValue& planNode) {
        if (!planNode.IsMap()) {
            return false;
        }

        const auto& planMap = planNode.GetMapSafe();
        if (IsJoinPlanNode(planNode)) {
            if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
                const auto& children = plans->second.GetArraySafe();
                if (children.size() == 2 && IsHashShufflePlanNode(children[0]) && IsHashShufflePlanNode(children[1])) {
                    return true;
                }
            }
        }

        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            for (const auto& child : plans->second.GetArraySafe()) {
                if (HasJoinWithBothInputsHashShuffled(child)) {
                    return true;
                }
            }
        }

        return false;
    }

    bool HasJoinWithBothInputsHashShuffled(const TString& plan) {
        NJson::TJsonValue planRoot;
        NJson::ReadJsonTree(plan, &planRoot, true);
        return HasJoinWithBothInputsHashShuffled(planRoot.GetMapSafe().at("SimplifiedPlan"));
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

    TPreparedQueryHolder::TConstPtr CompilePreparedQuery(TKikimrRunner& kikimr, const TString& query) {
        // The KQP proxy registers the compile service during bootstrap; session creation is the readiness barrier.
        const auto sessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

        const ui32 nodeIdx = 0;
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor(nodeIdx);

        TKqpQuerySettings querySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        TKqpQueryId queryId(
            TString(DefaultKikimrPublicClusterName),
            "/Root",
            /*databaseId*/ "",
            /*userSid*/ "root@builtin",
            query,
            querySettings,
            /*paramTypes*/ nullptr,
            TGUCSettings{});

        TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("root@builtin", {});
        runtime.Send(new IEventHandle(
            MakeKqpCompileServiceID(runtime.GetNodeId(nodeIdx)),
            edgeActor,
            new TEvKqp::TEvCompileRequest(
                userToken,
                /*clientAddress*/ "",
                /*uid*/ Nothing(),
                TMaybe<TKqpQueryId>(std::move(queryId)),
                /*keepInCache*/ false,
                /*isQueryActionPrepare*/ false,
                /*perStatementResult*/ false,
                /*deadline*/ TInstant::Max(),
                /*dbCounters*/ nullptr,
                std::make_shared<TGUCSettings>(),
                /*applicationName*/ Nothing(),
                std::make_shared<std::atomic<bool>>(true),
                MakeIntrusive<TUserRequestContext>("shuffle-elimination-ut", "/Root", "compile-session"))));

        const auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edgeActor, TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "Compile request timed out");
        UNIT_ASSERT_C(response->Get()->CompileResult, "Compile result is missing");
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
            response->Get()->CompileResult->Issues.ToString());
        UNIT_ASSERT_C(response->Get()->CompileResult->PreparedQuery, "Prepared query is missing");
        return response->Get()->CompileResult->PreparedQuery;
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

    Y_UNIT_TEST(AggregationShuffleEliminationSettingDoesNotEnableTransactionLayout) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        const auto preparedQuery = CompilePreparedQuery(kikimr, R"(
            PRAGMA ydb.OptShuffleElimination = "false";
            PRAGMA ydb.OptShuffleEliminationForAggregation = "true";

            SELECT 1;
        )");

        const auto& transactions = preparedQuery->GetTransactions();
        UNIT_ASSERT(!transactions.empty());
        for (const auto& transaction : transactions) {
            UNIT_ASSERT(!transaction->EnableShuffleElimination());
        }
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
PRAGMA ydb.OptimizerHints = '
    JoinType(region nation Shuffle)
    JoinType(region nation supplier Shuffle)
    JoinType(region nation supplier customer Shuffle)
    JoinType(region nation supplier customer orders Shuffle)
    JoinType(region nation supplier customer orders lineitem Shuffle)
    JoinOrder(((((region nation) supplier) customer) orders) lineitem)
';
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

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinAlgo", "BlockHash"), plan);

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(CountJoinPlanNodes(plan), 1u, plan);
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
            HasJoinWithBothInputsHashShuffled(plan),
            TStringBuilder() << "Expected a join with HashShuffle on both inputs, got: "
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

    Y_UNIT_TEST_TWIN(DistinctShuffleEliminationTPCHQ4, CompositePartitionKey) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetAllowOlapDataQuery(true);
        config->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        config->SetDefaultCostBasedOptimizationLevel(4);
        config->SetDefaultEnableShuffleElimination(true);
        config->SetDefaultHashShuffleFuncType(NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({MakeTPCHStatsSetting()});
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schema = tableSession.ExecuteSchemeQuery(Sprintf(R"(
            CREATE TABLE `/Root/lineitem` (
                l_orderkey Int64 NOT NULL,
                l_linenumber Int32 NOT NULL,
                l_commitdate Date NOT NULL,
                l_receiptdate Date NOT NULL,
                PRIMARY KEY (l_orderkey, l_linenumber)
            )
            PARTITION BY HASH(%s)
            WITH (STORE = COLUMN, PARTITION_COUNT = 8);

            CREATE TABLE `/Root/orders` (
                o_orderkey Int64 NOT NULL,
                o_orderpriority Utf8 NOT NULL,
                o_orderdate Date NOT NULL,
                PRIMARY KEY (o_orderkey)
            )
            PARTITION BY HASH(o_orderkey)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )", CompositePartitionKey ? "l_orderkey, l_linenumber" : "l_orderkey")).GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());

        const auto july = TInstant::ParseIso8601("1993-07-01T00:00:00Z");
        const auto october = TInstant::ParseIso8601("1993-10-01T00:00:00Z");
        NYdb::TValueBuilder orders, lineitems;
        orders.BeginList();
        lineitems.BeginList();
        for (i64 id = 1; id <= 32; ++id) {
            orders.AddListItem().BeginStruct()
                .AddMember("o_orderkey").Int64(id)
                .AddMember("o_orderpriority").Utf8(id % 2 ? "HIGH" : "LOW")
                .AddMember("o_orderdate").Date(id <= 16 ? july : october)
                .EndStruct();
            // Two qualifying line items per matching order must count only once.
            for (i32 line = 1; line <= 3; ++line) {
                lineitems.AddListItem().BeginStruct()
                    .AddMember("l_orderkey").Int64(id)
                    .AddMember("l_linenumber").Int32(line)
                    .AddMember("l_commitdate").Date(july)
                    .AddMember("l_receiptdate").Date(id % 4 && line < 3 ? october : july)
                    .EndStruct();
            }
        }
        orders.EndList();
        lineitems.EndList();
        for (auto& [table, rows] : TVector<std::pair<TString, NYdb::TValue>>{
                 {"/Root/orders", orders.Build()}, {"/Root/lineitem", lineitems.Build()}}) {
            const auto upsert = tableClient.BulkUpsert(table, std::move(rows)).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        std::string query = NResource::Find("resfs/file/tpch/queries/yql/q4.sql");
        Replace(query, "{% include 'header.sql.jinja' %}", "PRAGMA YqlSelect = 'force';");
        Replace(query, "{{orders}}", "`/Root/orders`");
        Replace(query, "{{lineitem}}", "`/Root/lineitem`");
        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        const auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
        const auto plan = TString{*explain.GetStats()->GetPlan()};
        const auto shuffles = CollectHashShuffleDescriptions(plan);
        UNIT_ASSERT_VALUES_EQUAL_C(shuffles.size(), CompositePartitionKey ? 3 : 2, plan);
        // Identify the DISTINCT exchange by its key, the intermediate DISTINCT
        // result over l_orderkey, separately from the join and priority-aggregation exchanges.
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto distinctShuffles = std::count_if(shuffles.begin(), shuffles.end(), [&](const TString& shuffle) {
            const TString key{TStringBuf(shuffle).After('(').RBefore(')')};
            return FindOperatorByStringField(simplifiedPlan, "Aggregation",
                "{" + key + ": distinct(/Root/lineitem.l_orderkey)}") != nullptr;
        });
        UNIT_ASSERT_VALUES_EQUAL_C(distinctShuffles, CompositePartitionKey ? 1 : 0, plan);

        const auto execute = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(execute.IsSuccess(), execute.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(execute.GetResultSet(0)), R"([["HIGH";8u];["LOW";4u]])");
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

    Y_UNIT_TEST(ShuffleEliminationRightJoinAliasReproducer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/_temp/_pool_249": {"n_rows": 12, "byte_size": 1024},
            "/Root/_temp/_pool_252": {"n_rows": 66464, "byte_size": 8517376},
            "/Root/_temp/_pool_256": {"n_rows": 36, "byte_size": 2048},
            "/Root/public/_accumrg28120": {"n_rows": 32601254, "byte_size": 4172960512}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto schemeClient = kikimr.GetSchemeClient();
        auto mkDir = schemeClient.MakeDirectory("/Root/_temp").GetValueSync();
        UNIT_ASSERT_C(mkDir.IsSuccess(), mkDir.GetIssues().ToString());
        mkDir = schemeClient.MakeDirectory("/Root/public").GetValueSync();
        UNIT_ASSERT_C(mkDir.IsSuccess(), mkDir.GetIssues().ToString());

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/_temp/_pool_252` (
                `_q_000_f_000` Decimal(10, 0),
                `_q_000_f_001rref` String,
                `_q_000_f_002rref` String,
                `_q_000_f_003rref` String,
                `_q_000_f_004rref` String,
                `_q_000_f_005` Decimal(15, 3),
                `_q_000_f_006` Decimal(31, 18),
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/_temp/_pool_256` (
                `_q_000_f_000rref` String,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/_temp/_pool_249` (
                `_q_000_f_000_type` String,
                `_q_000_f_000_rtref` String,
                `_q_000_f_000_rrref` String,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/public/_accumrg28120` (
                `_period` Timestamp64 NOT NULL,
                `_recordertref` String NOT NULL,
                `_recorderrref` String NOT NULL,
                `_lineno` Decimal(9, 0) NOT NULL,
                `_active` Bool NOT NULL,
                `_recordkind` Decimal(1, 0) NOT NULL,
                `_fld28121rref` String NOT NULL,
                `_fld28122rref` String NOT NULL,
                `_fld28123rref` String NOT NULL,
                `_fld28124rref` String NOT NULL,
                `_fld28125` Decimal(15, 3) NOT NULL,
                `_fld28126` Decimal(15, 2) NOT NULL,
                `_fld28127_type` String NOT NULL,
                `_fld28127_rtref` String NOT NULL,
                `_fld28127_rrref` String NOT NULL,
                `_fld28128_type` String NOT NULL,
                `_fld28128_rtref` String NOT NULL,
                `_fld28128_rrref` String NOT NULL,
                `_fld28129rref` String NOT NULL,
                `_fld28130rref` String NOT NULL,
                `_fld28131rref` String NOT NULL,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        const TString query = R"sql(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";

            DECLARE $const_0 AS String;
            DECLARE $const_1 AS String;
            DECLARE $const_2 AS Timestamp64;
            DECLARE $const_3 AS Timestamp64;
            DECLARE $const_4 AS Decimal(1, 0);

            SELECT
                `s2`.`__ydb_1_7`,
                `s2`.`__ydb_1_8`,
                `s2`.`__ydb_1_9`,
                `s2`.`__ydb_1_10`,
                `s2`.`__ydb_1_13`,
                `s2`.`__ydb_1_14`,
                `s2`.`__ydb_1_15`,
                `t4`.`_q_000_f_000`,
                `s2`.`__ydb_1_16`,
                `s2`.`__ydb_1_17`,
                `s2`.`__ydb_1_18`,
                `s2`.`__ydb_1_19`,
                `s2`.`__ydb_1_20`,
                `s2`.`__ydb_1_21`,
                `s2`.`__ydb_6_1`,
                `s2`.`__ydb_1_12`
            FROM `/Root/_temp/_pool_252` AS `t4`
            RIGHT JOIN (
                SELECT
                    `s1`.`__ydb_1_7` AS `__ydb_1_7`,
                    `s1`.`__ydb_1_8` AS `__ydb_1_8`,
                    `s1`.`__ydb_1_9` AS `__ydb_1_9`,
                    `s1`.`__ydb_1_10` AS `__ydb_1_10`,
                    `s1`.`__ydb_1_13` AS `__ydb_1_13`,
                    `s1`.`__ydb_1_14` AS `__ydb_1_14`,
                    `s1`.`__ydb_1_15` AS `__ydb_1_15`,
                    `s1`.`__ydb_1_16` AS `__ydb_1_16`,
                    `s1`.`__ydb_1_17` AS `__ydb_1_17`,
                    `s1`.`__ydb_1_18` AS `__ydb_1_18`,
                    `s1`.`__ydb_1_19` AS `__ydb_1_19`,
                    `s1`.`__ydb_1_20` AS `__ydb_1_20`,
                    `s1`.`__ydb_1_21` AS `__ydb_1_21`,
                    `s1`.`__ydb_1_12` AS `__ydb_1_12`,
                    `s1`.`__ydb_6_1` AS `__ydb_6_1`
                FROM `/Root/_temp/_pool_256` AS `t2`
                INNER JOIN (
                    SELECT
                        `t1`.`_fld28121rref` AS `__ydb_1_7`,
                        `t1`.`_fld28122rref` AS `__ydb_1_8`,
                        `t1`.`_fld28123rref` AS `__ydb_1_9`,
                        `t1`.`_fld28124rref` AS `__ydb_1_10`,
                        `t1`.`_fld28127_type` AS `__ydb_1_13`,
                        `t1`.`_fld28127_rtref` AS `__ydb_1_14`,
                        `t1`.`_fld28127_rrref` AS `__ydb_1_15`,
                        `t1`.`_fld28128_type` AS `__ydb_1_16`,
                        `t1`.`_fld28128_rtref` AS `__ydb_1_17`,
                        `t1`.`_fld28128_rrref` AS `__ydb_1_18`,
                        `t1`.`_fld28129rref` AS `__ydb_1_19`,
                        `t1`.`_fld28130rref` AS `__ydb_1_20`,
                        `t1`.`_fld28131rref` AS `__ydb_1_21`,
                        `t1`.`_fld28126` AS `__ydb_1_12`,
                        `t6`.`__ydb_6_1` AS `__ydb_6_1`
                    FROM `/Root/public/_accumrg28120` AS `t1`
                    LEFT JOIN (
                        SELECT
                            `t1`.`_ydb_pk` AS `__ydb_pk_0`,
                            `t6`.`_q_000_f_000` AS `__ydb_6_1`,
                            `t6`.`_q_000_f_001rref` AS `__ydb_6_2`,
                            `t6`.`_q_000_f_002rref` AS `__ydb_6_3`,
                            `t6`.`_q_000_f_003rref` AS `__ydb_6_4`,
                            `t6`.`_q_000_f_004rref` AS `__ydb_6_5`,
                            `t6`.`_ydb_pk` AS `_ydb_pk`
                        FROM `/Root/public/_accumrg28120` AS `t1`
                        INNER JOIN `/Root/_temp/_pool_252` AS `t6`
                            ON ((`t6`.`_q_000_f_001rref` = `t1`.`_fld28128_rrref`))
                            AND ((`t6`.`_q_000_f_002rref` = `t1`.`_fld28129rref`))
                            AND ((`t6`.`_q_000_f_003rref` = `t1`.`_fld28130rref`))
                            AND ((`t6`.`_q_000_f_004rref` = `t1`.`_fld28131rref`))
                        WHERE (($const_0 = `t1`.`_fld28128_type`))
                            AND (($const_1 = `t1`.`_fld28128_rtref`))
                    ) AS `t6`
                        ON (`t1`.`_ydb_pk` = `t6`.`__ydb_pk_0`)
                    LEFT ONLY JOIN `/Root/_temp/_pool_249` AS `t8`
                        ON ((`t1`.`_fld28127_type` = `t8`.`_q_000_f_000_type`))
                        AND ((`t1`.`_fld28127_rtref` = `t8`.`_q_000_f_000_rtref`))
                        AND ((`t1`.`_fld28127_rrref` = `t8`.`_q_000_f_000_rrref`))
                    WHERE ((`t1`.`_period` >= $const_2))
                        AND ((`t1`.`_period` <= $const_3))
                        AND (`t1`.`_active`)
                        AND ((`t1`.`_recordkind` = $const_4))
                ) AS `s1`
                    ON ((`s1`.`__ydb_1_7` = `t2`.`_q_000_f_000rref`))
            ) AS `s2`
                ON ((`t4`.`_q_000_f_001rref` = `s2`.`__ydb_1_7`))
                AND ((`t4`.`_q_000_f_002rref` = `s2`.`__ydb_1_8`))
                AND ((`t4`.`_q_000_f_003rref` = `s2`.`__ydb_1_9`))
                AND ((`t4`.`_q_000_f_004rref` = `s2`.`__ydb_1_10`))
        )sql";

        auto result = tableSession.ExplainDataQuery(query).GetValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        UNIT_ASSERT_C(!result.GetPlan().empty(), result.GetPlan());
    }

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
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
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
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            WITH cte as (
                SELECT a1.id2, join_id FROM (SELECT id as id2, join_id FROM `/Root/foo_0` as foo_0) as a1)

            SELECT X1.id2, X2.id2
            FROM
               (SELECT id2
                FROM `/Root/foo_1` as foo_1, cte
                WHERE foo_1.join_id = cte.join_id) as X1,

               (SELECT id2
                FROM `/Root/foo_2` as foo_2, cte
                WHERE foo_2.join_id = cte.join_id) as X2

            ORDER BY X1.id2, X2.id2;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[0;0];[0;1];[1;0];[1;1];[2;0];[2;1]])");
    }

    Y_UNIT_TEST(AliasesRenames) {
        AliasesRenamesTest(true);
        //AliasesRenamesTest(false);
    }

    /*
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

    Y_UNIT_TEST(SetOps) {
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
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1 WHERE t1.a == 0
                INTERSECT
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                EXCEPT
                SELECT t2.a as a FROM `/Root/t2` as t2 WHERE t2.a IN (0,1)
                ORDER BY a;
            )",
        };

        const std::vector<std::string> results = {
            R"([[0]])",
            R"([[0];[1];[2];[3]])",
            R"([[2];[3]])"
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
            R"(
                SELECT count(distinct t1.c), t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT t1.b as b, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
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
            R"([[4u;#];[1u;[1]];[1u;[2]];[1u;[3]];[1u;[4]]])",
            R"([[#;#];[[1];#];[[1];[2]];[[2];#];[[2];[3]];[[3];#];[[3];[4]];[[4];#];[[4];[5]]])",
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

    Y_UNIT_TEST(RollupGrouping) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
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
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();

        db = kikimr.GetTableClient();
        InsertIntoSchema1(db, "/Root/t1", 4);

        const std::vector<std::string> queries = {
            R"(
                SELECT count(t1.a), t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, grouping(t1.b) as gb, grouping(t1.c) as gc,
                       grouping(t1.b, t1.c) as gbc
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by t1.b
                order by b;
            )",
            R"(
                SELECT sum(t1.a), t1.b as b, t1.c as c, grouping(t1.b) + grouping(t1.c) as loch
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by loch desc, b, c;
            )",
            R"(
                SELECT t1.b as b, t1.c as c, grouping(t1.c) as gc,
                       rank() over (partition by grouping(t1.c) order by sum(t1.a) desc) as rnk
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by gc, rnk;
            )",
            R"(
                SELECT t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
        };

        const std::vector<std::string> results = {
            R"([[4u;#;1u];[1u;[1];0u];[1u;[2];0u];[1u;[3];0u];[1u;[4];0u]])",
            R"([[4u;#;#;1u;1u;3u];[1u;[1];#;0u;1u;1u];[1u;[1];[2];0u;0u;0u];[1u;[2];#;0u;1u;1u];[1u;[2];[3];0u;0u;0u];)"
            R"([1u;[3];#;0u;1u;1u];[1u;[3];[4];0u;0u;0u];[1u;[4];#;0u;1u;1u];[1u;[4];[5];0u;0u;0u]])",
            R"([[1u;[1];0u];[1u;[2];0u];[1u;[3];0u];[1u;[4];0u]])",
            R"([[[6];#;#;2u];[[0];[1];#;1u];[[1];[2];#;1u];[[2];[3];#;1u];[[3];[4];#;1u];)"
            R"([[0];[1];[2];0u];[[1];[2];[3];0u];[[2];[3];[4];0u];[[3];[4];[5];0u]])",
            R"([[[4];[5];0u;1u];[[3];[4];0u;2u];[[2];[3];0u;3u];[[1];[2];0u;4u];)"
            R"([#;#;1u;1u];[[4];#;1u;2u];[[3];#;1u;3u];[[2];#;1u;4u];[[1];#;1u;5u]])",
            R"([[#;1u];[[1];0u];[[2];0u];[[3];0u];[[4];0u]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "Query " << i << ": " << result.GetIssues().ToString());

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "Query " << i << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), results[i], "Query " << i);
        }
    }

}

} // namespace NKqp
} // namespace NKikimr
