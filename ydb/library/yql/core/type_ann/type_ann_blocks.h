#pragma once

#include "type_ann_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {

    IGraphTransformer::TStatus AsScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus ReplicateScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus ReplicateScalarsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockCompressWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockExistsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockExpandChunkedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockCoalesceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockLogicalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockJustWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockAsStructWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockAsTupleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockNthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockFromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockFuncWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus BlockBitCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus BlockCombineAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus BlockCombineHashedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus BlockMergeFinalizeHashedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus WideToBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus WideFromBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideSkipTakeBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideTopBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus WideSortBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockPgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockPgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockExtendWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

} // namespace NTypeAnnImpl
} // namespace NYql
