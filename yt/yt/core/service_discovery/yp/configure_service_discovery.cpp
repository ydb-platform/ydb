#include "service_discovery.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NServiceDiscovery::NYP {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TServiceDiscoveryConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TServiceDiscoveryConfigPtr& config)
{
    NRpc::TDispatcher::Get()->SetServiceDiscovery(CreateServiceDiscovery(config));
}

YT_DEFINE_CONFIGURABLE_SINGLETON(
    "yp_service_discovery",
    TServiceDiscoveryConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
