#include "yql_co.h"

namespace NYql {

TExprNode::TPtr BuildYqlAggregationTraits(
    const TExprNode::TPtr& node,
    TExprNode::TPtr type,
    TExprNode::TPtr extractor,
    TExprContext& ctx,
    TOptimizeContext& optCtx);

} // namespace NYql
