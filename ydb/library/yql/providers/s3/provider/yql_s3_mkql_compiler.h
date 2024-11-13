#pragma once

#include "yql_s3_provider.h"

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqS3MkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TS3State::TPtr& state);

}
