#pragma once

#include "type_ann_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {

TExprNodePtr WrapWithPgCast(TExprNodePtr&& node, ui32 targetTypeId, TExprContext& ctx);
TString MakeAliasedColumn(TStringBuf alias, TStringBuf column);
const TItemExprType* AddAlias(const TString& alias, const TItemExprType* item, TExprContext& ctx);
TStringBuf RemoveAlias(TStringBuf column);
TStringBuf RemoveAlias(TStringBuf column, TStringBuf& alias);
const TItemExprType* RemoveAlias(const TItemExprType* item, TExprContext& ctx);
TMap<TString, ui32> ExtractExternalColumns(const TExprNode& select);
bool IsPlainMemberOverArg(const TExprNode& expr, TStringBuf& memberName);
const TTypeAnnotationNode* ToPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx);
const TTypeAnnotationNode* FromPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx);

IGraphTransformer::TStatus PgSelfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgBoolOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus FromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus ToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgCloneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgArrayOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgWindowCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgNullIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgAggWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgQualifiedStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgColumnRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgResultItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgReplaceUnknownWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
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
IGraphTransformer::TStatus PgValuesListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus PgArrayWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgTypeModWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgLikeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgInWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgBetweenWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgSubLinkWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgGroupRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgGroupingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgGroupingSetWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgToRecordWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
IGraphTransformer::TStatus PgIterateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

} // namespace NTypeAnnImpl
} // namespace NYql
