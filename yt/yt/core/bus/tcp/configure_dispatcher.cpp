#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NBus {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TTcpDispatcherConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TTcpDispatcherDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TTcpDispatcherConfigPtr& config)
{
    NBus::TTcpDispatcher::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TTcpDispatcherConfigPtr& config,
    const TTcpDispatcherDynamicConfigPtr& dynamicConfig)
{
    TTcpDispatcher::Get()->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "tcp_dispatcher",
    TTcpDispatcherConfig,
    TTcpDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
