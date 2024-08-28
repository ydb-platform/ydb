#pragma once

#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/public/purecalc/common/processor_mode.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Build type annotation transformer that is aware of type of the input rows.
         *
         * @param typeAnnotationContext current context.
         * @param inputStructs types of each input.
         * @param rawInputStructs container to store the resulting input item type.
         * @param processorMode current processor mode. This will affect generated input type,
         *                      e.g. list node or struct node.
         * @param nodeName name of the callable used to get input data, e.g. `Self`.
         * @return a graph transformer for type annotation.
         */
        TAutoPtr<IGraphTransformer> MakeTypeAnnotationTransformer(
            TTypeAnnotationContextPtr typeAnnotationContext,
            const TVector<const TStructExprType*>& inputStructs,
            TVector<const TStructExprType*>& rawInputStructs,
            EProcessorMode processorMode,
            const TString& nodeName = TString{PurecalcInputCallableName}
        );
    }
}
