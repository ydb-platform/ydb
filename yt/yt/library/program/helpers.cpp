#include "helpers.h"
#include "config.h"

#include <yt/yt/library/profiling/perf/event_counter_profiler.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    TSingletonManager::Configure(config);

    // TODO(babenko): move to server program base
    NLogging::TLogManager::Get()->EnableReopenOnSighup();

    // By default, server components must have a reasonable FQDN.
    // Failure to do so may result in issues like YT-4561.
    // TODO(babenko): move to server program base
    NNet::TAddressResolver::Get()->EnsureLocalHostName();

    // TODO(babenko): move to server program base
    NProfiling::EnablePerfEventCounterProfiling();
}

void ReconfigureSingletons(const TSingletonsDynamicConfigPtr& dynamicConfig)
{
    TSingletonManager::Reconfigure(dynamicConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
