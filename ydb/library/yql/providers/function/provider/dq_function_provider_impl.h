#pragma once

#include "dq_function_provider.h"

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql::NDqFunction {

std::unique_ptr<TVisitorTransformerBase> CreateDqFunctionIntentTransformer(TDqFunctionState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateDqFunctionMetaLoader(TDqFunctionState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateDqFunctionPhysicalOptTransformer(TDqFunctionState::TPtr state);
std::unique_ptr<IDqIntegration> CreateDqFunctionDqIntegration(TDqFunctionState::TPtr state);
std::unique_ptr<TVisitorTransformerBase> CreateDqFunctionTypeAnnotation(TDqFunctionState::TPtr state);

}
