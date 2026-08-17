#pragma once

#include "runtime_settings.h"

#include <yql/essentials/utils/strong_alias.h>

#include <util/system/types.h>

namespace NYql {

using TRuntimeSettingsStableHash = TStrongAlias<class TRuntimeSettingsStableHashTag, TString>;

TRuntimeSettingsStableHash StableHashRuntimeSettings(const TRuntimeSettings& config);

} // namespace NYql
