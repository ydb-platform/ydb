#pragma once

#include "dq_function_provider.h"

#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>

namespace NYql::NDqFunction {

THolder<TVisitorTransformerBase> CreateDqFunctionIntentTransformer(TDqFunctionState::TPtr state);
THolder<IGraphTransformer> CreateDqFunctionMetaLoader(TDqFunctionState::TPtr state);
THolder<IGraphTransformer> CreateDqFunctionPhysicalOptTransformer(TDqFunctionState::TPtr state);
THolder<IDqIntegration> CreateDqFunctionDqIntegration(TDqFunctionState::TPtr state);
THolder<TVisitorTransformerBase> CreateDqFunctionTypeAnnotation(TDqFunctionState::TPtr state);

}
