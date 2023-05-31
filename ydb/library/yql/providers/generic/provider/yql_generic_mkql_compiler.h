#pragma once

#include "yql_generic_provider.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql {

    void RegisterDqGenericMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TGenericState::TPtr& state);

}
