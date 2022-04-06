#pragma once

#include "type_ann_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {

IGraphTransformer::TStatus PgStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus FromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus ToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgWindowCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAggWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgQualifiedStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgColumnRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgResultItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgWhereWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAnonWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgConstWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgInternal0Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAggregationTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgSetItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

} // namespace NTypeAnnImpl
} // namespace NYql
