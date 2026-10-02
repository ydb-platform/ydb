#include "registry.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TSolomonRegistryConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TSolomonRegistryDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TSolomonRegistryConfigPtr& config)
{
    TSolomonRegistry::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TSolomonRegistryConfigPtr& config,
    const TSolomonRegistryDynamicConfigPtr& dynamicConfig)
{
    TSolomonRegistry::Get()->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "solomon_registry",
    TSolomonRegistryConfig,
    TSolomonRegistryDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
