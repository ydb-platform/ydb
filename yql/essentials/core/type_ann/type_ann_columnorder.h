#pragma once

#include "type_ann_impl.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>


namespace NYql::NTypeAnnImpl {

IGraphTransformer::TStatus OrderForPgSetItem(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForAssumeColumnOrder(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForSqlProject(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForMergeExtend(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForUnionAll(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForEquiJoin(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderForCalcOverWindow(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus OrderFromFirst(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus OrderFromFirstAndOutputType(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx);
} // namespace NYql::NTypeAnnImpl

