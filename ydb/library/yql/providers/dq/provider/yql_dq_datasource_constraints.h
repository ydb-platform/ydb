#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

std::unique_ptr<IGraphTransformer> CreateDqDataSourceConstraintTransformer();

} // NYql
