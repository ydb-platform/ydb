#include "io_dispatcher.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NPipes {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TIODispatcherConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TIODispatcherDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TIODispatcherConfigPtr& config)
{
    TIODispatcher::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TIODispatcherConfigPtr& config,
    const TIODispatcherDynamicConfigPtr& dynamicConfig)
{
    TIODispatcher::Get()->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "io_dispatcher",
    TIODispatcherConfig,
    TIODispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
