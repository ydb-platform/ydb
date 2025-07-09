#pragma once

#include "type_ann_impl.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql::NTypeAnnImpl {

IGraphTransformer::TStatus MatchRecognizeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeMeasuresCallablesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeMeasuresCallableWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeParamsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeMeasuresWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizePatternWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeDefinesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus MatchRecognizeCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

} // namespace NYql::NTypeAnnImpl
