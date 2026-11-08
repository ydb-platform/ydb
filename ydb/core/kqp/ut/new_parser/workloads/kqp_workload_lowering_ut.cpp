#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/host/new_parser/rbo/kqp_rbo_lowering.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/resource/resource.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <regex>
#include <sstream>

namespace NKikimr::NKqp {
namespace {

// The schema is the original ClickBench fixture. Fail if it acquires syntax
// this small metadata loader cannot represent, rather than silently omitting
// columns. Bytes is the SQL alias for the catalog's String type.
NNewYql::TTableMetadataPtr ClickBenchTable() {
    auto table = std::make_shared<NNewYql::TTableMetadata>();
    table->Cluster = "ut";
    table->Path = "/Root/hits";
    const auto schema = NResource::Find("clickbench/schema.sql");
    std::istringstream lines(std::string(schema.data(), schema.size()));
    const std::regex column(R"(^\s*(\w+)\s+(\w+)\s+(NOT NULL)\s*,\s*$)");
    std::string line;
    while (std::getline(lines, line)) {
        if (line.find("CREATE TABLE") != std::string::npos || line.find("PRIMARY KEY") != std::string::npos
            || line.find(");") != std::string::npos || line.find_first_not_of(" \t\r") == std::string::npos) {
            continue;
        }
        std::smatch match;
        UNIT_ASSERT_C(std::regex_match(line, match, column), "Unsupported schema line: " << line);
        const TString type = match[2].str();
        table->Columns.push_back({TString(match[1].str()), ui32(table->Columns.size() + 1),
            type == "Bytes" ? TString("String") : type, true});
    }
    UNIT_ASSERT(!table->Columns.empty());
    return table;
}

void CheckClickBench(ui32 queryId, bool columnStore, const TVector<TString>& outputs,
    const TVector<TString>& sortKeys, TStringBuf predicateColumn)
{
    NTests::TIdTestContext f;
    const auto sql = NResource::Find(TStringBuilder() << "clickbench/q" << queryId << ".yql");
    auto parsed = ParseQueryWithNewParser(sql, {}, false);
    UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(parsed.Ast->Statements.size(), 1);
    const auto metadata = ClickBenchTable();
    auto names = NNewYql::ResolveNames(*parsed.Ast, {"ut", "/Root", {}},
        [&](const TString& cluster, const TString& path) {
            UNIT_ASSERT_VALUES_EQUAL(cluster, metadata->Cluster);
            UNIT_ASSERT_VALUES_EQUAL(path, metadata->Path);
            return metadata;
        });
    UNIT_ASSERT_C(names.IsOk(), names.Issues.ToString());
    auto annotations = NNewYql::AnnotateAst(*parsed.Ast, *names.Names, f.ExprCtx, f.TypeCtx);
    UNIT_ASSERT_C(annotations, f.ExprCtx.IssueManager.GetIssues().ToString());
    const auto& query = *std::get<NNewYql::TQueryStatement>(parsed.Ast->Statements.front().Value).Query;
    const auto storage = columnStore ? NYql::EStorageType::ColumnStorage : NYql::EStorageType::RowStorage;
    // There is no host or legacy translator in this test. Any fallback thrown
    // by parsing, resolution, annotation or operator lowering fails the case.
    auto root = NNewYql::LowerToRbo(query, *parsed.Ast, *names.Names, *annotations, f.ExprCtx,
        [&](const NNewYql::TTableMetadata& table) {
            const auto node = NNodes::Build<NNodes::TKqpTable>(f.ExprCtx, f.Pos)
                .Path().Build(table.Path).PathId().Build("1:1").SysView().Build("").Version().Build("1").Done().Ptr();
            return NNewYql::TRboTable{node, storage};
        });
    NTests::AssertIdInvariants(*root, root->PlanProps);
    TVector<TString> actualOutputs;
    for (const auto& [id, name] : root->GetColumns().Items()) {
        Y_UNUSED(id);
        actualOutputs.push_back(name);
    }
    UNIT_ASSERT_VALUES_EQUAL(actualOutputs, outputs);
    auto op = root->GetInput();
    if (!sortKeys.empty()) {
        UNIT_ASSERT(op->Kind == EOperator::Limit);
        const auto limit = CastOperator<TOpLimit>(op);
        UNIT_ASSERT(!limit->HasOffset());
        const auto count = limit->GetLimitCond().GetExpressionBody();
        UNIT_ASSERT(count->IsCallable("Uint64"));
        UNIT_ASSERT_VALUES_EQUAL(count->Head().Content(), "10");
        op = limit->GetInput();
        UNIT_ASSERT(op->Kind == EOperator::Sort);
        const auto sort = CastOperator<TOpSort>(op);
        TVector<TString> actualKeys;
        for (const auto& [id, order] : sort->GetSortElements().Items()) {
            actualKeys.push_back(root->PlanProps.InfoUnitRegistry.Get(id).GetColumnName());
            UNIT_ASSERT(order.Ascending);
        }
        UNIT_ASSERT_VALUES_EQUAL(actualKeys, sortKeys);
        op = sort->GetInput();
    }
    UNIT_ASSERT(op->Kind == EOperator::Map);
    op = CastOperator<TOpMap>(op)->GetInput();
    UNIT_ASSERT(op->Kind == EOperator::Filter);
    const auto filter = CastOperator<TOpFilter>(op);
    const auto inputs = filter->GetFilterExpression().GetRawInputIUs();
    UNIT_ASSERT_VALUES_EQUAL(inputs.Size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(root->PlanProps.InfoUnitRegistry.Get(*inputs.begin()).GetColumnName(), predicateColumn);
    op = filter->GetInput();
    UNIT_ASSERT(op->Kind == EOperator::Source);
    const auto read = CastOperator<TOpRead>(op);
    UNIT_ASSERT(read->GetTableStorageType() == storage);
    UNIT_ASSERT_VALUES_EQUAL(NNodes::TKqpTable(read->GetTable()).Path().StringValue(), "/Root/hits");
    UNIT_ASSERT_VALUES_EQUAL(read->GetColumns().Size(), metadata->Columns.size());
    for (const auto& item : *root) {
        for (const auto& expression : item.Current->GetExpressions()) {
            NYql::CheckArguments(*expression.get().GetLambda());
            UNIT_ASSERT_EQUAL(expression.get().PlanProps, &root->PlanProps);
        }
    }
}

} // namespace

// These original workload queries exercise the currently supported relational
// subset. Aggregation-heavy TPCH/TPCDS queries are not native lowering coverage;
// their separate fallback compatibility tests must not be counted as such.
Y_UNIT_TEST_SUITE(KqpNewYqlWorkloadLowering) {
    Y_UNIT_TEST_TWIN(ClickBench_Q20, ColumnStore) {
        CheckClickBench(20, ColumnStore, {"UserID"}, {}, "UserID");
    }
    Y_UNIT_TEST_TWIN(ClickBench_Q25, ColumnStore) {
        CheckClickBench(25, ColumnStore, {"SearchPhrase", "EventTime"}, {"EventTime"}, "SearchPhrase");
    }
    Y_UNIT_TEST_TWIN(ClickBench_Q26, ColumnStore) {
        CheckClickBench(26, ColumnStore, {"SearchPhrase"}, {"SearchPhrase"}, "SearchPhrase");
    }
    Y_UNIT_TEST_TWIN(ClickBench_Q27, ColumnStore) {
        CheckClickBench(27, ColumnStore, {"SearchPhrase", "EventTime"}, {"EventTime", "SearchPhrase"}, "SearchPhrase");
    }
}

} // namespace NKikimr::NKqp
