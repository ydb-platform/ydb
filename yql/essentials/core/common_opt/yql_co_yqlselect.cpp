#include "yql_co_yqlselect.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>

namespace NYql {

TExprNode::TPtr BuildYqlAggregationTraits(
    const TExprNode::TPtr& node,
    TExprNode::TPtr type,
    TExprNode::TPtr extractor,
    TExprContext& ctx,
    TOptimizeContext& optCtx)
{
    YQL_ENSURE(node->IsCallable("YqlAgg"));

    TExprNode::TPtr traitsFactory = ImportDeeplyCopied(
        node->Child(0)->Child(0)->Pos(ctx),
        "/lib/yql/aggregate.yqls",
        TString(node->Child(0)->Child(0)->Content()) + "_traits_factory",
        ctx,
        *optCtx.Types);
    YQL_ENSURE(traitsFactory);

    // clang-format off
    type = ctx.Builder(node->Pos())
        .Callable("ListType")
            .Add(0, std::move(type))
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    TExprNode::TPtr traits = ctx.Builder(node->Pos())
        .Apply(std::move(traitsFactory))
            .With(0, std::move(type))
            .With(1, std::move(extractor))
        .Seal()
        .Build();
    // clang-format on

    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApplyNoRepeat(traits, traits, ctx);
    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

    return traits;
}

} // namespace NYql
