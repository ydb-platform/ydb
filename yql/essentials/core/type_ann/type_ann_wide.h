#pragma once

#include "type_ann_impl.h"
#include "type_ann_impl.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_expr_types.h>


namespace NYql::NTypeAnnImpl {
    IGraphTransformer::TStatus ExpandMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    bool ValidateWideTopKeys(TExprNode& keys, const TTypeAnnotationNode::TListType& types, TExprContext& ctx);

    IGraphTransformer::TStatus WideMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideFilterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus WideWhileWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideCombinerWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideCondense1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideChopperWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideChain1MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus NarrowMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus NarrowFlatMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus NarrowMultiMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus WideTopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
} // namespace NYql::NTypeAnnImpl

