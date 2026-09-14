#pragma once

#include "yql_co.h"

namespace NYql {

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgBetween(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

} // namespace NYql
