#pragma once
#include "defs.h"

namespace NKikimr::NConsole {

/**
 * Log Settings Configurator is used to track log config changes and adjust current
 * log setting appropriately.
 */
IActor *CreateLogSettingsConfigurator();

IActor *CreateLogSettingsConfigurator(const TString &pathToConfigCacheFile);

} // namespace NKikimr::NConsole
