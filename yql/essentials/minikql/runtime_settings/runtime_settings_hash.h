#pragma once

#include "runtime_settings.h"

#include <util/system/types.h>

#include <array>

namespace NYql {

using TRuntimeSettingsStableHash = TString;

TRuntimeSettingsStableHash StableHashRuntimeSettings(const TRuntimeSettings& config);

} // namespace NYql
