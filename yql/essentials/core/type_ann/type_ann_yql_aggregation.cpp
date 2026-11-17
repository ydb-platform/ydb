#include "type_ann_yql.h"

#include <yql/essentials/core/yql_module_helpers.h>

namespace NYql::NTypeAnnImpl {
namespace {

bool ValidateYqlTraitsFactoryArity(
    const TExprNode& input, const TExprNode& factory, TExprContext& ctx)
{
    YQL_ENSURE(factory.IsLambda());
    const size_t expectedArgsCount = factory.Head().ChildrenSize();
    YQL_ENSURE(expectedArgsCount >= 2);
    const size_t actualArgsCount = input.ChildrenSize() + 1;

    if (expectedArgsCount != actualArgsCount) {
        ctx.AddError(TIssue(
            input.Head().Pos(ctx),
            TStringBuilder() << "Expected " << expectedArgsCount << " arguments, "
                             << "but got " << actualArgsCount));
        return false;
    }

    return true;
}

IGraphTransformer::TStatus AnnotateYqlTraitsFactory(
    const TExprNode::TPtr& input, const TString& modulePath, TExtContext& ctx)
{
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isUniversal;
    if (!EnsureAtomOrUniversal(input->Head(), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal || !ctx.Types.Modules) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    const auto* factory = ImportFreezed(
        input->Head().Pos(ctx.Expr), modulePath,
        TString(input->Head().Content()) + "_traits_factory", ctx.Expr, ctx.Types);
    if (!factory || !ValidateYqlTraitsFactoryArity(*input, **factory, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

} // namespace

IGraphTransformer::TStatus YqlAggFactoryWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    Y_UNUSED(output);

    return AnnotateYqlTraitsFactory(input, "/lib/yql/aggregate.yqls", ctx);
}

IGraphTransformer::TStatus YqlWinFactoryWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    Y_UNUSED(output);

    return AnnotateYqlTraitsFactory(input, "/lib/yql/window.yqls", ctx);
}

} // namespace NYql::NTypeAnnImpl
