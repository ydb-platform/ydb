#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandAggregate(const TExprNode::TPtr& node, TExprContext& ctx, bool forceCompact = false);
inline TExprNode::TPtr ExpandAggregateCompact(const TExprNode::TPtr& node, TExprContext& ctx) {
    return ExpandAggregate(node, ctx, true);
}

}

