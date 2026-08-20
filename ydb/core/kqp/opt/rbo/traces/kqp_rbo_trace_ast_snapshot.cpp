#include "kqp_rbo_trace_ast_snapshot.h"

#include "kqp_rbo_trace.h"
#include "kqp_rbo_trace_output.h"
#include "kqp_rbo_yql_ast_trace.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

#include <yql/essentials/utils/log/log.h>

#include <cctype>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

using TStatus = IGraphTransformer::TStatus;

std::string ToStdString(TStringBuf value) {
    return std::string(value.data(), value.size());
}

std::string TraceRootId(const std::string& title) {
    std::string id = "ast-snapshot";
    for (const char ch : title) {
        id += (std::isalnum(static_cast<unsigned char>(ch)) ? ch : '-');
    }
    return id;
}

std::string FormatExprForTrace(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!node) {
        return {};
    }

    return ToStdString(KqpExprToPrettyString(*node, ctx));
}

std::string FormatExprListForTrace(
    const TVector<TExprNode::TPtr>& items,
    const std::string& itemLabel,
    TExprContext& ctx)
{
    std::ostringstream out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) {
            out << "\n";
        }
        out << "----- " << itemLabel << " " << i << " -----\n"
            << FormatExprForTrace(items[i], ctx)
            << "\n";
    }

    return out.str();
}

void AddAstInfoTabs(
    optimizer_trace::Trace::Tile& tile,
    const std::optional<optimizer_trace::Widget>& linkGraph,
    const std::string& text)
{
    if (linkGraph) {
        tile.info().tab("dag-links", "DAG links")
            .widget(*linkGraph);
    }
    if (!text.empty()) {
        tile.info().tab("yql-ast-text", "YQL AST text")
            .widget(optimizer_trace::Widget::unwrappedText("Regular YQL AST", text, true));
    }
}

void AddQueryTextInfo(optimizer_trace::Trace::Tile& tile, const std::optional<std::string>& queryText) {
    if (!queryText || queryText->empty()) {
        return;
    }

    tile.info().tab("query-text", "Query text")
        .widget(optimizer_trace::Widget::unwrappedText("Original YQL query", *queryText, true));
}

} // anonymous namespace

class TRboTraceAstSnapshotLog {
public:
    explicit TRboTraceAstSnapshotLog(TString stage)
        : StageName(ToStdString(stage))
    {
    }

    bool IsOpen() {
        return EnsureOutput();
    }

    void Submit(
        const std::string& title,
        NYqlAstTrace::TBuildResult astTrace,
        const std::string& text)
    {
        if (!EnsureOutput()) {
            return;
        }

        auto& tile = Stage().tree(title, astTrace.Root);

        AddAstInfoTabs(tile, astTrace.LinkGraph, text);
        AddQueryTextInfo(tile, Output->QueryText());

        auto result = Output->Submit(tile);
        if (!result) {
            YQL_CLOG(WARN, CoreDq) << "Failed to submit RBO HTML trace AST snapshot to "
                                   << Output->OutputPath() << ": " << result.error;
            return;
        }

        Output->Flush();
    }

private:
    bool EnsureOutput() {
        if (!Output) {
            Output = std::make_unique<TRBOTraceOutputHandle>("RBO HTML trace AST snapshot");
        }
        return Output->IsOpen();
    }

    optimizer_trace::Trace::Stage& Stage() {
        if (!Stage_) {
            DefineHtmlTraceFields(Output->Trace());
            Stage_ = &Output->Trace().stage(StageName);
        }
        return *Stage_;
    }

private:
    std::string StageName;
    std::unique_ptr<TRBOTraceOutputHandle> Output;
    optimizer_trace::Trace::Stage* Stage_ = nullptr;
};

namespace {

class TRboTraceAstSnapshotTransformer final : public TSyncTransformerBase {
public:
    TRboTraceAstSnapshotTransformer(std::shared_ptr<TRboTraceAstSnapshotLog> traceLog, TString title)
        : TraceLog(std::move(traceLog))
        , Title(ToStdString(title))
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (!input || !TraceLog || !TraceLog->IsOpen()) {
            return TStatus::Ok;
        }

        TraceLog->Submit(
            Title,
            NYqlAstTrace::BuildExprTreeWithInfo(input, TraceRootId(Title)),
            FormatExprForTrace(input, ctx));
        return TStatus::Ok;
    }

    void Rewind() override {
    }

private:
    std::shared_ptr<TRboTraceAstSnapshotLog> TraceLog;
    std::string Title;
};

class TRboTraceBuildQuerySnapshotTransformer final : public TSyncTransformerBase {
public:
    TRboTraceBuildQuerySnapshotTransformer(
        std::shared_ptr<TRboTraceAstSnapshotLog> traceLog,
        TString title,
        TIntrusivePtr<NOpt::TKqpBuildQueryContext> buildCtx)
        : TraceLog(std::move(traceLog))
        , Title(ToStdString(title))
        , BuildCtx(std::move(buildCtx))
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (!TraceLog || !TraceLog->IsOpen() || !BuildCtx || BuildCtx->PhysicalTxs.empty()) {
            return TStatus::Ok;
        }

        TVector<TExprNode::TPtr> txs;
        txs.reserve(BuildCtx->PhysicalTxs.size());
        for (const auto& tx : BuildCtx->PhysicalTxs) {
            txs.push_back(tx.Ptr());
        }

        TraceLog->Submit(
            Title,
            NYqlAstTrace::BuildExprListTreeWithInfo(txs, TraceRootId(Title), "Physical transactions", "Tx"),
            FormatExprListForTrace(txs, "Tx", ctx));
        return TStatus::Ok;
    }

    void Rewind() override {
    }

private:
    std::shared_ptr<TRboTraceAstSnapshotLog> TraceLog;
    std::string Title;
    TIntrusivePtr<NOpt::TKqpBuildQueryContext> BuildCtx;
};

} // anonymous namespace

std::shared_ptr<TRboTraceAstSnapshotLog> CreateKqpRboTraceAstSnapshotLog(TString stage) {
    return std::make_shared<TRboTraceAstSnapshotLog>(std::move(stage));
}

TAutoPtr<IGraphTransformer> CreateKqpRboTraceAstSnapshotTransformer(
    std::shared_ptr<TRboTraceAstSnapshotLog> traceLog,
    TString title)
{
    return new TRboTraceAstSnapshotTransformer(std::move(traceLog), std::move(title));
}

TAutoPtr<IGraphTransformer> CreateKqpRboTraceBuildQuerySnapshotTransformer(
    std::shared_ptr<TRboTraceAstSnapshotLog> traceLog,
    TString title,
    TIntrusivePtr<NOpt::TKqpBuildQueryContext> buildCtx)
{
    return new TRboTraceBuildQuerySnapshotTransformer(
        std::move(traceLog),
        std::move(title),
        std::move(buildCtx));
}

} // namespace NKikimr::NKqp
