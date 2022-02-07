#pragma once

#include "yql_pq_provider.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqPqMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);

}
