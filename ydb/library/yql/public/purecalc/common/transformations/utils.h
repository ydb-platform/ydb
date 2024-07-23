#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * A transformer which wraps the given input node with the pipeline
         * converting the input type to the block one.
         *
         * @param pos the position of the given node to be rewritten.
         * @param structType the item type of the container provided by the node.
         * @param ctx the context to make ExprNode rewrites.
         * @return the resulting ExprNode.
         */
        TExprNode::TPtr NodeFromBlocks(
            const TPositionHandle& pos,
            const TStructExprType* structType,
            TExprContext& ctx
        );
    }
}
