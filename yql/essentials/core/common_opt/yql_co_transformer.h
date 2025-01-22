#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>

namespace NYql {

TAutoPtr<IGraphTransformer> CreateCommonOptTransformer(TTypeAnnotationContext* typeCtx, bool ignorePgRules = false);
TAutoPtr<IGraphTransformer> CreateCommonOptFinalTransformer(TTypeAnnotationContext* typeCtx);

}
