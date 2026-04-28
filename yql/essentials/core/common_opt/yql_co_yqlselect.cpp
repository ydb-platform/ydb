#include "yql_co_yqlselect.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/yql_yqlselect.h>

namespace NYql {

TExprNode::TPtr BuildYqlAggregationTraits(
    const TExprNode::TPtr& node,
    TExprNode::TPtr type,
    TExprNode::TPtr extractor,
    TExprContext& ctx,
    TOptimizeContext& optCtx)
{
    // clang-format off
    type = ctx.Builder(node->Pos())
        .Callable("ListType")
            .Add(0, std::move(type))
        .Seal()
        .Build();
    // clang-format on

    return NYql::ExpandYqlTraitsFactory(
        node->Child(0), std::move(type), std::move(extractor), ctx, *optCtx.Types);
}

} // namespace NYql
