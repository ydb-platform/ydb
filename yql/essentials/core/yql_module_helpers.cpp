#include "yql_module_helpers.h"
#include "yql_expr_optimize.h"

#include <library/cpp/iterator/enumerate.h>

namespace NYql {

namespace {

const TExprNode::TPtr* ImportFreezed(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types,
    TExprContext** src)
{
    const TExportTable* exports = types.Modules->GetModule(path);
    if (!exports) {
        ctx.AddError(TIssue(
            position,
            TStringBuilder()
                << "Module '" << path << "' not found"));
        return nullptr;
    }

    const TExprNode::TPtr* symbol = exports->Symbols().FindPtr(name);
    if (!symbol) {
        ctx.AddError(TIssue(
            position,
            TStringBuilder()
                << "Symbol '" << name << "' not found "
                << "in module '" << path << "'"));
        return nullptr;
    }

    if (src) {
        *src = &exports->ExprCtx();
    }

    return symbol;
}

} // namespace

const TExprNode::TPtr* ImportFreezed(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types)
{
    return ImportFreezed(
        position, path, name, ctx, types, /*src=*/nullptr);
}

TExprNode::TPtr ImportDeeplyCopied(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types)
{
    TExprContext* src = nullptr;
    const TExprNode::TPtr* symbol = ImportFreezed(
        position, path, name, ctx, types, &src);
    if (!symbol) {
        return nullptr;
    }

    TNodeOnNodeOwnedMap clones;
    return ctx.DeepCopy(
        /*node=*/*(*symbol),
        /*nodeContext=*/*src,
        /*deepClones=*/clones,
        /*internStrings=*/true,
        /*copyTypes=*/false);
}

TExprNode::TPtr ImportCached(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types)
{
    TExprContext* source = nullptr;
    const auto* symbol = ImportFreezed(position, path, name, ctx, types, &source);
    if (!symbol) {
        return nullptr;
    }

    if (const auto cached = ctx.LazyBindings.find(symbol->Get()); cached != ctx.LazyBindings.end()) {
        return cached->second;
    }
    auto result = ctx.DeepCopy(**symbol, *source, ctx.LazyBindingClones,
                               /*internStrings=*/true, /*copyTypes=*/false);
    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    const auto status = ExpandApplyNoRepeat(result, result, ctx, types);
    if (status == IGraphTransformer::TStatus::Error) {
        return nullptr;
    }
    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
    ctx.LazyBindings.emplace(symbol->Get(), result);
    return result;
}

} // namespace NYql
