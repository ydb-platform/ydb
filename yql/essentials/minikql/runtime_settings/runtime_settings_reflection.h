#pragma once

#include "runtime_settings.h"

#include <yql/essentials/utils/meta/reflection.h>

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NPrivate::TRuntimeHostSettings,
                      (DatumValidation)(TestHostSetting)(UdfProfileEnable)(UdfProfileMinTimeUs)(UdfProfileGraceCount)(UdfProfileExcludeModules)(UdfProfileHLLPrecision));

} // namespace NYql::NReflection
