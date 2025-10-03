#include "baggage_manager.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NTracing {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TBaggageManagerConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TBaggageManagerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TBaggageManagerConfigPtr& config)
{
    TBaggageManager::Configure(config);
}

void ReconfigureSingleton(
    const TBaggageManagerConfigPtr& config,
    const TBaggageManagerDynamicConfigPtr& dynamicConfig)
{
    TBaggageManager::Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "trace_baggage",
    TBaggageManagerConfig,
    TBaggageManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
