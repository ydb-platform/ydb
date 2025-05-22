#pragma once

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqSolomonMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);

}
