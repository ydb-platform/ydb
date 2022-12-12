#pragma once
#include "defs.h"

namespace NKikimr::NConsole {

/**
 * Shared Cache Configurator tracks and applies changes to shared cache configuration.
 */
IActor *CreateSharedCacheConfigurator();

} // namespace NKikimr::NConsole
