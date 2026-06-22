#include "kqp_rbo_rule_trace.h"

#include "kqp_rbo_trace.h"

#include <util/string/builder.h>

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
        SubmitTextTile(ctx, htmlStage, "AST before RewriteSelect", ToStdString(*ctx.KqpCtx.RboTraceAstBeforeRewriteSelect));
    }
    if (ctx.KqpCtx.RboTraceAstAfterRewriteSelect) {
        SubmitTextTile(ctx, htmlStage, "AST after RewriteSelect", ToStdString(*ctx.KqpCtx.RboTraceAstAfterRewriteSelect));
    }

    DefineHtmlTraceFields(ctx.TraceLog.trace());
    ComputeHtmlTraceProps(root, ctx, "Plan input");

    TTraceBuildState traceBuildState;
    auto& tile = htmlStage.tree(
        "Original plan",
        BuildPlanNodeFromRoot(root, ctx.ExprCtx, HtmlPlanOpts, &traceBuildState));
    AddPlanWidgets(tile, root, traceBuildState);
    ctx.TraceLog.Submit(tile);
}

} // namespace NKikimr::NKqp
