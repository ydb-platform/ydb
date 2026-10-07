#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

std::unique_ptr<IGraphTransformer> CreateGenericDescribeTableTransformer(TGenericState::TPtr state);

} // namespace NYql
