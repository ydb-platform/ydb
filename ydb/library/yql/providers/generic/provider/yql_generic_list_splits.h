#pragma once

#include "yql_generic_state.h"

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

THolder<TGraphTransformerBase> CreateGenericListSplitTransformer(TGenericState::TPtr state);

}   // namespace NYql
