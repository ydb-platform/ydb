#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp::NYqlTests {

using namespace NYdb;
using namespace NYdb::NTable;

inline TString GetStringField(const NJson::TJsonValue& node, const TString& fieldName) {
    const auto& map = node.GetMapSafe();
    const auto field = map.find(fieldName);
    UNIT_ASSERT_C(field != map.end() && field->second.IsString(), fieldName);
    return field->second.GetStringSafe();
}

inline bool StringArrayFieldContains(const NJson::TJsonValue& node, const TString& fieldName, const TString& value) {
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

inline const NJson::TJsonValue* FindOperatorByStringField(const NJson::TJsonValue& planNode, const TString& fieldName, const TString& fieldValue) {
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

// The subtree of skipNode is not searched.
inline const NJson::TJsonValue* FindOperatorByStringFieldContaining(const NJson::TJsonValue& planNode, const TString& fieldName, const TString& fieldValue,
    const NJson::TJsonValue* skipNode = nullptr) {
    if (!planNode.IsMap() || &planNode == skipNode) {
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
            if (const auto* op = FindOperatorByStringFieldContaining(child, fieldName, fieldValue, skipNode)) {
                return op;
            }
        }
    }

    return nullptr;
}

inline const NJson::TJsonValue* FindConnectionNode(const NJson::TJsonValue& node, const TString& connectionName) {
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

inline void PrintPlan(const TString& plan, bool analyzeMode) {
    NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
        NYdb::NConsoleClient::EDataFormat::PrettyTable,
        analyzeMode, Cout, /*maxWidth=*/0
    );
    queryPlanPrinter.Print(plan);
}

inline TString ExecuteExplain(NYdb::NQuery::TSession& session, const TString& query) {
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

inline NJson::TJsonValue GetSimplifiedPlan(const TString& plan) {
    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);

    const auto& planMap = planJson.GetMapSafe();
    const auto simplifiedPlan = planMap.find("SimplifiedPlan");
    UNIT_ASSERT_C(simplifiedPlan != planMap.end(), plan);
    return simplifiedPlan->second;
}

inline NKikimrConfig::TAppConfig CreateExplainPlanTestAppConfig(bool inlineJoinFiltersAfterCBO = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
    appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(inlineJoinFiltersAfterCBO);
    return appConfig;
}

inline void CreateExplainPlanTestTables(TKikimrRunner& kikimr) {
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

inline NYdb::NQuery::TSession CreateQuerySession(TKikimrRunner& kikimr) {
    auto db = kikimr.GetQueryClient();
    auto res = db.GetSession().GetValueSync();
    NStatusHelpers::ThrowOnError(res);
    return res.GetSession();
}

class TExplainPlanTestContext {
public:
    explicit TExplainPlanTestContext(bool inlineJoinFiltersAfterCBO = false)
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

} // namespace NKikimr::NKqp::NYqlTests
