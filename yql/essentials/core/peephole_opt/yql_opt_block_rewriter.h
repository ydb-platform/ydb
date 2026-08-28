#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

bool CollectBlockRewrites(TExprContext& ctx, TTypeAnnotationContext& types,
    TTypeAnnotationNode::TConstSpanType inputTypes, bool keepInputColumns,
    const TExprNode::TPtr& lambda, ui32& newNodes, TNodeMap<size_t>& rewritePositions,
    TExprNode::TPtr& blockLambda, TExprNode::TPtr& restLambda);

} // namespace NYql
