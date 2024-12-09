#pragma once
#include "defs.h"

namespace NKikimr::NConsole {

    /**
     * Feature flags configurator tracks feature flags config changes and
     * applies them to the runtime feature flags in AppData.
     */
    IActor* CreateFeatureFlagsConfigurator();

} // namespace NKikimr::NConsole
