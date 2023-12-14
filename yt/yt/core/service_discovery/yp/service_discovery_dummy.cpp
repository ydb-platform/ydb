#include "service_discovery.h"

#include "config.h"

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

IServiceDiscoveryPtr CreateServiceDiscovery(TServiceDiscoveryConfigPtr config)
{
    // In opensource, we do not have any service discovery implementation,
    // so it must be disabled.
    YT_VERIFY(!config->Enable);

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

bool GetServiceDiscoveryEnableDefault()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
