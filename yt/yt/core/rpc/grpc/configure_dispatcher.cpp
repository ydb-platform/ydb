#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NRpc::NGrpc {

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
    TDispatcher::Get()->Reconfigure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "grpc_dispatcher",
    TDispatcherConfig,
    TDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
