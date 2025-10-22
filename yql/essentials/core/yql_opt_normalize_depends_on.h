#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreateNormalizeDependsOnTransformer(const TTypeAnnotationContext& types);

}
