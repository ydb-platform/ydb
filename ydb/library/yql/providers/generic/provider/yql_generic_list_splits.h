#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

THolder<TGraphTransformerBase> CreateGenericListSplitTransformer(TGenericState::TPtr state);

} // NYql
