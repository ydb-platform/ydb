#pragma once

#include "yql_ydb_provider.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqYdbMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TYdbState::TPtr& state);

}
