#pragma once

#include "type_ann_impl.h"
#include "type_ann_sql.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_expr_types.h>


namespace NYql::NTypeAnnImpl {

bool AdjustPgUnknownType(TVector<const TItemExprType*>& outputItems, TExprContext& ctx);
IGraphTransformer::TStatus InferPgCommonType(
    TPositionHandle pos,
    const TExprNode* setItems,
    const TExprNode* setOps,
    TColumnOrder& resultColumnOrder,
    const TStructExprType*& resultStructType,
    TExtContext& ctx,
    bool& isUniversal);
TExprNodePtr WrapWithPgCast(TExprNodePtr&& node, ui32 targetTypeId, TExprContext& ctx);
const TTypeAnnotationNode* ToPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx, bool& isUniversal);
const TTypeAnnotationNode* FromPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx, bool& isUniversal);

IGraphTransformer::TStatus PgSelfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgBoolOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus FromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus ToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCloneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgArrayOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgWindowCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgNullIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAggWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgQualifiedStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAnonWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgConstWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgInternal0Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAggregationTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgArrayWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgTypeModWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgLikeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgInWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgBetweenWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgToRecordWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgIterateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

} // namespace NYql::NTypeAnnImpl
