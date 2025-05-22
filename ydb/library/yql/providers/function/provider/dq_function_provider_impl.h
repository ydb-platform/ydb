#pragma once

#include "dq_function_provider.h"

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql::NDqFunction {

THolder<TVisitorTransformerBase> CreateDqFunctionIntentTransformer(TDqFunctionState::TPtr state);
THolder<IGraphTransformer> CreateDqFunctionMetaLoader(TDqFunctionState::TPtr state);
THolder<IGraphTransformer> CreateDqFunctionPhysicalOptTransformer(TDqFunctionState::TPtr state);
THolder<IDqIntegration> CreateDqFunctionDqIntegration(TDqFunctionState::TPtr state);
THolder<TVisitorTransformerBase> CreateDqFunctionTypeAnnotation(TDqFunctionState::TPtr state);

}
