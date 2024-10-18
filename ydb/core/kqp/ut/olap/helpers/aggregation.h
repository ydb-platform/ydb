#pragma once
#include "writer.h"
#include "local.h"

namespace NKikimr::NKqp {

class TExpectedLimitChecker {
private:
    std::optional<ui32> ExpectedLimit;
    std::optional<ui32> ExpectedResultCount;
    ui32 CheckScanData = 0;
    ui32 CheckScanTask = 0;
public:
    TExpectedLimitChecker& SetExpectedLimit(const ui32 value) {
        ExpectedLimit = value;
        ExpectedResultCount = value;
        return *this;
    }
    TExpectedLimitChecker& SetExpectedResultCount(const ui32 value) {
        ExpectedResultCount = value;
        return *this;
    }
    bool CheckExpectedLimitOnScanData(const ui32 resultCount) {
        if (!ExpectedResultCount) {
            return true;
        }
        ++CheckScanData;
        UNIT_ASSERT_LE(resultCount, *ExpectedResultCount);
        return true;
    }
    bool CheckExpectedLimitOnScanTask(const ui32 taskLimit) {
        if (!ExpectedLimit) {
            return true;
        }
        ++CheckScanTask;
        UNIT_ASSERT_EQUAL(taskLimit, *ExpectedLimit);
        return true;
    }
    bool CheckFinish() const {
        if (!ExpectedLimit) {
            return true;
        }
        return CheckScanData && CheckScanTask;
    }
};

class TExpectedRecordChecker {
private:
    std::optional<ui32> ExpectedColumnsCount;
    ui32 CheckScanData = 0;
public:
    TExpectedRecordChecker& SetExpectedColumnsCount(const ui32 value) {
        ExpectedColumnsCount = value;
        return *this;
    }
    bool CheckExpectedOnScanData(const ui32 columnsCount) {
        if (!ExpectedColumnsCount) {
            return true;
        }
        ++CheckScanData;
        UNIT_ASSERT_EQUAL(columnsCount, *ExpectedColumnsCount);
        return true;
    }
    bool CheckFinish() const {
        if (!ExpectedColumnsCount) {
            return true;
        }
        return CheckScanData;
    }
};

class TAggregationTestCase {
private:
    TString Query;
    TString ExpectedReply;
    std::vector<std::string> ExpectedPlanOptions;
    bool Pushdown = true;
    std::string ExpectedReadNodeType;
    TExpectedLimitChecker LimitChecker;
    TExpectedRecordChecker RecordChecker;
    bool UseLlvm = true;
public:
    void FillExpectedAggregationGroupByPlanOptions() {
        AddExpectedPlanOptions("WideCombiner");
    }
    TString GetFixedQuery() const {
        TStringBuilder queryFixed;
        queryFixed << "--!syntax_v1" << Endl;
        if (!Pushdown) {
            queryFixed << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
        }
        if (!UseLlvm) {
            queryFixed << "PRAGMA Kikimr.UseLlvm = \"false\";" << Endl;
        }
        queryFixed << "PRAGMA Kikimr.OptUseFinalizeByKey;" << Endl;

        queryFixed << Query << Endl;
        Cerr << "REQUEST:\n" << queryFixed << Endl;
        return queryFixed;
    }
    TAggregationTestCase() = default;
    TExpectedLimitChecker& MutableLimitChecker() {
        return LimitChecker;
    }
    TExpectedRecordChecker& MutableRecordChecker() {
        return RecordChecker;
    }
    bool GetPushdown() const {
        return Pushdown;
    }
    TAggregationTestCase& SetPushdown(const bool value = true) {
        Pushdown = value;
        return *this;
    }
    bool CheckFinished() const {
        return LimitChecker.CheckFinish();
    }

    const TString& GetQuery() const {
        return Query;
    }
    TAggregationTestCase& SetQuery(const TString& value) {
        Query = value;
        return *this;
    }
    TAggregationTestCase& SetUseLlvm(const bool value) {
        UseLlvm = value;
        return *this;
    }
    const TString& GetExpectedReply() const {
        return ExpectedReply;
    }
    TAggregationTestCase& SetExpectedReply(const TString& value) {
        ExpectedReply = value;
        return *this;
    }

    TAggregationTestCase& AddExpectedPlanOptions(const std::string& value) {
        ExpectedPlanOptions.emplace_back(value);
        return *this;
    }

    const std::vector<std::string>& GetExpectedPlanOptions() const {
        return ExpectedPlanOptions;
    }

    TAggregationTestCase& SetExpectedReadNodeType(const std::string& value) {
        ExpectedReadNodeType = value;
        return *this;
    }

    const std::string& GetExpectedReadNodeType() const {
        return ExpectedReadNodeType;
    }
};

template <typename TClient>
auto StreamExplainQuery(const TString& query, TClient& client) {
    if constexpr (std::is_same_v<NYdb::NTable::TTableClient, TClient>) {
        NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        return client.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
    } else {
        NYdb::NQuery::TExecuteQuerySettings scanSettings;
        scanSettings.ExecMode(NYdb::NQuery::EExecMode::Explain);
        return client.StreamExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), scanSettings).GetValueSync();
    }
}

template <typename TClient>
void CheckPlanForAggregatePushdown(
    const TString& query,
    TClient& client,
    const std::vector<std::string>& expectedPlanNodes,
    const std::string& readNodeType)
{
    auto res = StreamExplainQuery(query, client);
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    auto planRes = CollectStreamResult(res);
    auto ast = planRes.QueryStats->Getquery_ast();
    Cerr << "JSON Plan:" << Endl;
    Cerr << planRes.PlanJson.GetOrElse("NO_PLAN") << Endl;
    Cerr << "AST:" << Endl;
    Cerr << ast << Endl;
    for (auto planNode : expectedPlanNodes) {
        UNIT_ASSERT_C(ast.find(planNode) != std::string::npos,
            TStringBuilder() << planNode << " was not found. Query: " << query);
    }
    UNIT_ASSERT_C(ast.find("SqueezeToDict") == std::string::npos, TStringBuilder() << "SqueezeToDict denied for aggregation requests. Query: " << query);

    if (!readNodeType.empty()) {
        NJson::TJsonValue planJson;
        NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true);
        auto readNode = FindPlanNodeByKv(planJson, "Node Type", readNodeType.c_str());
        UNIT_ASSERT(readNode.IsDefined());

        auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
        for (auto& op : operators) {
            if (op.GetMapSafe().at("Name") == "TableFullScan") {
                auto ssaProgram = op.GetMapSafe().at("SsaProgram");
                UNIT_ASSERT(ssaProgram.IsDefined());
                UNIT_ASSERT(FindPlanNodes(ssaProgram, "Projection").size());
                break;
            }
        }
    }
}

void TestAggregationsBase(const std::vector<TAggregationTestCase>& cases);

void TestAggregationsInternal(const std::vector<TAggregationTestCase>& cases);

void TestAggregations(const std::vector<TAggregationTestCase>& cases);

template <typename TClient>
auto StreamExecuteQuery(const TAggregationTestCase& testCase, TClient& client) {
    if constexpr (std::is_same_v<NYdb::NTable::TTableClient, TClient>) {
        return client.StreamExecuteScanQuery(testCase.GetFixedQuery()).GetValueSync();
    } else {
        return client.StreamExecuteQuery(
            testCase.GetFixedQuery(),
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    }
}

template <typename TClient>
void RunTestCaseWithClient(const TAggregationTestCase& testCase, TClient& client) {
    auto it = StreamExecuteQuery(testCase, client);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    TString result = StreamResultToYson(it);
    if (!testCase.GetExpectedReply().empty()) {
        CompareYson(result, testCase.GetExpectedReply());
    }
}

void WriteTestDataForTableWithNulls(TKikimrRunner& kikimr, TString testTable);

void TestTableWithNulls(const std::vector<TAggregationTestCase>& cases, const bool genericQuery = false);

}
