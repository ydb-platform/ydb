#include "helpers.h"
#include "config.h"

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    TSingletonManager::Configure(config);

    // TODO(babenko): move to server program base
    NLogging::TLogManager::Get()->EnableReopenOnSighup();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
