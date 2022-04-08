#pragma once

#include "dq_function_provider.h"

#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

THolder<TVisitorTransformerBase> CreateDqFunctionIntentTransformer(TDqFunctionState::TPtr state);
THolder<IGraphTransformer> CreateDqFunctionMetaLoader(TDqFunctionState::TPtr state);

}