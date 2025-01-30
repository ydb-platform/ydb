#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

// Expands callable ComputeRangeFor (use for debug purposes)
THolder<IGraphTransformer> MakeExpandRangeComputeForTransformer(const TIntrusivePtr<TTypeAnnotationContext>& types);

}
