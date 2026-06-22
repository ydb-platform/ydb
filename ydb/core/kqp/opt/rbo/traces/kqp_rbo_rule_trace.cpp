#include "kqp_rbo_rule_trace.h"

#include "kqp_rbo_trace.h"
#include "kqp_rbo_yql_ast_trace.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <util/string/builder.h>

#include <optional>
#include <string>

namespace NKikimr::NKqp {

namespace {

constexpr ui32 HtmlTraceProps =
    ERuleProperties::RequireParents |
    ERuleProperties::RequireTypes |
    ERuleProperties::RequireMetadata |
    ERuleProperties::RequireStatistics |
    ERuleProperties::RequireLiveness |
    ERuleProperties::RequireNameConstraints |
    ERuleProperties::RequireAliases;

constexpr ui32 HtmlPlanOpts =
    EPrintPlanOptions::PrintFullMetadata |
    EPrintPlanOptions::PrintBasicStatistics;

std::string ToStdString(TStringBuf value) {
    return std::string(value.data(), value.size());
}

void ComputeHtmlTraceProps(TOpRoot& root, TRBOContext& ctx, TStringBuf stageName) {
    ComputeRequiredProps(root, HtmlTraceProps, ctx, TStringBuilder() << stageName << " HTML trace");
}

void SubmitTextTile(TRBOContext& ctx, optimizer_trace::Trace::Stage& stage, const std::string& title, const std::string& body) {
    if (body.empty()) {
        return;
    }

    auto& tile = stage.text(title, body);
    ctx.TraceLog.Submit(tile);
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

void SubmitAstTile(TRBOContext& ctx, optimizer_trace::Trace::Stage& stage, const std::string& title, const NYql::TExprNode::TPtr& ast, const std::string& rootId) {
    if (!ast) {
        return;
    }

    auto astTrace = NYqlAstTrace::BuildExprTreeWithInfo(ast, rootId);
    auto& tile = stage.tree(title, astTrace.Root);
    AddAstInfoTabs(
        tile,
        astTrace.LinkGraph,
        ToStdString(KqpExprToPrettyString(*ast, ctx.ExprCtx)));
    ctx.TraceLog.Submit(tile);
}

void AddQueryTextInfo(optimizer_trace::Trace::Tile& tile, const std::optional<std::string>& queryText) {
    if (!queryText || queryText->empty()) {
        return;
    }

    tile.info().tab("query-text", "Query text")
        .widget(optimizer_trace::Widget::unwrappedText("Original YQL query", *queryText, true));
}

} // anonymous namespace

TRuleTraceAttempt::TRuleTraceAttempt(TRBOContext& ctx, TStringBuf ruleName)
    : Ctx(ctx)
{
    if (Ctx.NeedToLog()) {
        Tile.emplace(ToStdString(ruleName), std::string());
        Ctx.TraceLog.SetCurrentRuleTile(&*Tile);
    }
}

void TRuleTraceAttempt::CloseRule() {
    if (!Closed) {
        Ctx.TraceLog.SetCurrentRuleTile(nullptr);
        Closed = true;
    }
}

void TRuleTraceAttempt::SubmitIfHasInfo(TOpRoot& root, TStringBuf stageName) {
    CloseRule();
    if (Tile && !Tile->info().empty()) {
        Submit(root, stageName);
    }
    Ctx.TraceLog.ClearPostBuildEnrichers();
    Tile.reset();
}

void TRuleTraceAttempt::SubmitApplied(TOpRoot& root, TStringBuf stageName) {
    CloseRule();
    if (Tile) {
        Submit(root, stageName);
    }
    Ctx.TraceLog.ClearPostBuildEnrichers();
    Tile.reset();
}

void TRuleTraceAttempt::Submit(TOpRoot& root, TStringBuf stageName) {
    ComputeHtmlTraceProps(root, Ctx, stageName);

    TTraceBuildState traceBuildState;
    Tile->setTree(BuildPlanNodeFromRoot(root, Ctx.ExprCtx, HtmlPlanOpts, &traceBuildState));
    Ctx.TraceLog.ApplyPostBuildEnrichers(*Tile, traceBuildState);

    auto& submittedTile = Ctx.TraceLog.AddTile(std::move(*Tile));
    AddPlanWidgets(submittedTile, root, traceBuildState);
    Ctx.TraceLog.Submit(submittedTile);
}

void SubmitInitialPlanTrace(TOpRoot& root, TRBOContext& ctx) {
    if (!ctx.NeedToLog()) {
        return;
    }

    auto& htmlStage = ctx.TraceLog.stage("Plan input");
    if (const auto& queryText = ctx.TraceLog.QueryText()) {
        SubmitTextTile(ctx, htmlStage, "Query text", *queryText);
    }
    if (ctx.KqpCtx.RboTraceAstBeforeRewriteSelect) {
        SubmitAstTile(ctx, htmlStage, "AST before RewriteSelect", ctx.KqpCtx.RboTraceAstBeforeRewriteSelect, "ast-before-rewrite-select");
    }
    if (ctx.KqpCtx.RboTraceAstAfterRewriteSelect) {
        SubmitAstTile(ctx, htmlStage, "AST after RewriteSelect", ctx.KqpCtx.RboTraceAstAfterRewriteSelect, "ast-after-rewrite-select");
    }

    DefineHtmlTraceFields(ctx.TraceLog.trace());
    ComputeHtmlTraceProps(root, ctx, "Plan input");

    TTraceBuildState traceBuildState;
    auto& tile = htmlStage.tree(
        "Original plan",
        BuildPlanNodeFromRoot(root, ctx.ExprCtx, HtmlPlanOpts, &traceBuildState));
    AddQueryTextInfo(tile, ctx.TraceLog.QueryText());
    AddPlanWidgets(tile, root, traceBuildState);
    ctx.TraceLog.Submit(tile);
}

} // namespace NKikimr::NKqp
