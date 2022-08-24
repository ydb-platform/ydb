#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include "yql_type_annotation.h"

namespace NYql {

TExprNode::TPtr ExpandAggregate(bool allowPickle, const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool forceCompact = false, bool compactForDistinct = false);
inline TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    return ExpandAggregate(false, node, ctx, typesCtx, true);
}

}

