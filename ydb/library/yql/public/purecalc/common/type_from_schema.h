#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/yson/node/node.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Load struct type from yson. Use methods below to check returned type for correctness.
         */
        const TTypeAnnotationNode* MakeTypeFromSchema(const NYT::TNode&, TExprContext&);

        /**
         * Extend struct type with additional columns. Type of each extra column is loaded from yson.
         */
        const TStructExprType* ExtendStructType(const TStructExprType*, const THashMap<TString, NYT::TNode>&, TExprContext&);

        /**
         * Check if the given type can be used as an input schema, i.e. it is a struct.
         */
        bool ValidateInputSchema(const TTypeAnnotationNode* type, TExprContext& ctx);

        /**
         * Check if the given type can be used as an output schema, i.e. it is a struct or a variant of structs.
         */
        bool ValidateOutputSchema(const TTypeAnnotationNode* type, TExprContext& ctx);

        /**
         * Check if output type can be silently converted to the expected type.
         */
        bool ValidateOutputType(const TTypeAnnotationNode* type, const TTypeAnnotationNode* expected, TExprContext& ctx);
    }
}
