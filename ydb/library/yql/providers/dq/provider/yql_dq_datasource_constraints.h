#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

THolder<IGraphTransformer> CreateDqDataSourceConstraintTransformer();

} // NYql
