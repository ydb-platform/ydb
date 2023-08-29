#pragma once

#include "type_ann_core.h"
#include "type_ann_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>


namespace NYql {
    namespace NTypeAnnImpl {
        IGraphTransformer::TStatus MatchRecognizeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
        IGraphTransformer::TStatus MatchRecognizeParamsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
        IGraphTransformer::TStatus MatchRecognizeMeasuresWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
        IGraphTransformer::TStatus MatchRecognizePatternWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
        IGraphTransformer::TStatus MatchRecognizeDefinesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
        IGraphTransformer::TStatus MatchRecognizeCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    }
}
