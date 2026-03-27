#pragma once

#include "type_ann_impl.h"

namespace NYql::NTypeAnnImpl {

////////////////////////////////////////////////////////////////////////////////

bool IsPlainMemberOverArg(const TExprNode& expr, TStringBuf& memberName);

////////////////////////////////////////////////////////////////////////////////

TString MakeAliasedColumn(TStringBuf alias, TStringBuf column);

const TItemExprType* AddAlias(const TString& alias, const TItemExprType* item, TExprContext& ctx);

TStringBuf RemoveAlias(TStringBuf column);

TStringBuf RemoveAlias(TStringBuf column, TString& alias);

const TItemExprType* RemoveAlias(const TItemExprType* item, TExprContext& ctx);

////////////////////////////////////////////////////////////////////////////////

TMap<TString, ui32> ExtractExternalColumns(const TExprNode& select);

////////////////////////////////////////////////////////////////////////////////

IGraphTransformer::TStatus SqlStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlColumnRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlResultItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlReplaceUnknownWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlWhereWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlSetItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus SqlValuesListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus SqlSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus SqlSubLinkWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlGroupRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlGroupingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

IGraphTransformer::TStatus SqlGroupingSetWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

} // namespace NYql::NTypeAnnImpl
