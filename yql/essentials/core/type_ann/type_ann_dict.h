#pragma once

#include "type_ann_core.h"
#include "type_ann_impl.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_expr_types.h>


namespace NYql::NTypeAnnImpl {

const TDictExprType* GetCachedMutDictType(const TStringBuf& resourceTag, const TExprContext& typeCtx);
IGraphTransformer::TStatus MutDictCreateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus ToMutDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus FromMutDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

template <bool WithPayload>
IGraphTransformer::TStatus MutDictBlindOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictPopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictContainsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictHasItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictLengthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictKeysWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
IGraphTransformer::TStatus MutDictPayloadsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

template <bool WithPayload>
IGraphTransformer::TStatus DictBlindOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

} // namespace NYql::NTypeAnnImpl

