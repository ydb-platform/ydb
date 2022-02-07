#pragma once

#include "yql_clickhouse_provider.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>

namespace NYql {

void RegisterDqClickHouseMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TClickHouseState::TPtr& state);

}
