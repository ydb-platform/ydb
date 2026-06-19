#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

class TVisitorTransformerBase;

namespace NDq {

IGraphTransformer::TStatus ConstraintDqStage(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus ConstraintDqOutput(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus ConstraintDqConnection(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus ConstraintDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus ConstraintDqReplicate(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus ConstraintDqJoin(const TExprNode::TPtr& input, TExprContext& ctx);

std::unique_ptr<TVisitorTransformerBase> CreateDqConstraintsTransformer(bool disableCheck = false);

} // namespace NDq

} // namespace NYql
