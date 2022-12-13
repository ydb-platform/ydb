#pragma once

#include "yql_dq_state.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

THolder<IGraphTransformer> CreateDqDataSinkConstraintTransformer(TDqState::TPtr state);

} // NYql
