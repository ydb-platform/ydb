#include "service_discovery.h"

#include "config.h"

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

IServiceDiscoveryPtr CreateServiceDiscovery(TServiceDiscoveryConfigPtr config)
{
    if (!config->Enable) {
        return nullptr;
    }

    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

bool GetServiceDiscoveryEnableDefault()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
