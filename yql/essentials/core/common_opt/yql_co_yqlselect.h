#include "yql_co.h"

namespace NYql {

TExprNode::TPtr ExpandYqlFlatten(
    TExprNode::TPtr list,
    const TExprNode& from,
    const TExprNode::TPtr& renameLambda,
    TExprContext& ctx,
    TOptimizeContext& optCtx);

TExprNode::TPtr BuildYqlAggregationTraits(
    const TExprNode::TPtr& node,
    TExprNode::TPtr type,
    TExprNode::TPtr extractor,
    TExprContext& ctx,
    TOptimizeContext& optCtx);

} // namespace NYql
