#include "yql_module_helpers.h"

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

} // namespace NYql
