#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

THolder<IGraphTransformer> CreateDqDataSourceConstraintTransformer();

} // NYql
