#pragma once

#include <ydb/library/yql/public/purecalc/common/processor_mode.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * A transformer which converts an output type of the expression to the given type or reports an error.
         *
         * @param outputStruct destination output struct type.
         * @return a graph transformer for type alignment.
         */
        TAutoPtr<IGraphTransformer> MakeOutputAligner(
            const TTypeAnnotationNode* outputStruct,
            EProcessorMode processorMode
        );
    }
}
