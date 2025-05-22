#include "fiber_manager.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NConcurrency {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TFiberManagerConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TFiberManagerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TFiberManagerConfigPtr& config)
{
    TFiberManager::Configure(config);
}

void ReconfigureSingleton(
    const TFiberManagerConfigPtr& config,
    const TFiberManagerDynamicConfigPtr& dynamicConfig)
{
    TFiberManager::Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "fiber_manager",
    TFiberManagerConfig,
    TFiberManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
