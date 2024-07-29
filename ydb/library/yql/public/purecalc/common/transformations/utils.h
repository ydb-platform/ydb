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

        /**
         * A transformer which wraps the given root node with the pipeline
         * converting the output type to the block one.
         *
         * @param pos the position of the given node to be rewritten.
         * @param structType the item type of the container provided by the node.
         * @param ctx the context to make ExprNode rewrites.
         * @return the resulting ExprNode.
         */
        TExprNode::TPtr NodeToBlocks(
            const TPositionHandle& pos,
            const TStructExprType* structType,
            TExprContext& ctx
        );

        /**
         * A transformer to apply the given lambda to the given iterable (either
         * list or stream). If the iterable is list, the lambda should be passed
         * to the <LMap> callable; if the iterable is stream, the lambda should
         * be applied right to the iterable.
         *
         * @param pos the position of the given node to be rewritten.
         * @param iterable the node, that provides the iterable to be processed.
         * @param lambda the node, that provides lambda to be applied.
         * @param wrapLMap indicator to wrap the result with LMap callable.
         * @oaram ctx the context to make ExprNode rewrites.
         */
        TExprNode::TPtr ApplyToIterable(
            const TPositionHandle& pos,
            const TExprNode::TPtr iterable,
            const TExprNode::TPtr lambda,
            bool wrapLMap,
            TExprContext& ctx
        );

        /**
         * A helper which wraps the items of the given struct with the block
         * type container and appends the new item for _yql_block_length column.
         *
         * @param structType original struct to be wrapped.
         * @param ctx the context to make ExprType rewrite.
         * @return the new struct with block items.
         */
        const TStructExprType* WrapBlockStruct(
            const TStructExprType* structType,
            TExprContext& ctx
        );

        /**
         * A helper which unwraps the block container from the items of the
         * given struct and removes the item for _yql_block_length column.
         *
         * @param structType original struct to be unwrapped.
         * @param ctx the context to make ExprType rewrite.
         * @return the new struct without block items.
         */
        const TStructExprType* UnwrapBlockStruct(
            const TStructExprType* structType,
            TExprContext& ctx
        );
    }
}
