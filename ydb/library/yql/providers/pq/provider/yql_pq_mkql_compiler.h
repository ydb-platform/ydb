#pragma once

#include "yql_pq_provider.h"

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqPqMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);

}
