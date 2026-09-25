#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

std::unique_ptr<TGraphTransformerBase> CreateGenericListSplitTransformer(TGenericState::TPtr state);

} // NYql
