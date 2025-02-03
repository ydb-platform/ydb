#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NRpc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TDispatcherConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TDispatcherDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TDispatcherConfigPtr& config)
{
    TDispatcher::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TDispatcherConfigPtr& config,
    const TDispatcherDynamicConfigPtr& dynamicConfig)
{
    TDispatcher::Get()->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "rpc_dispatcher",
    TDispatcherConfig,
    TDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
