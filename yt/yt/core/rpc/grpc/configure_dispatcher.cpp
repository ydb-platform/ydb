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

void ConfigureSingleton(const TDispatcherConfigPtr& config)
{
    TDispatcher::Get()->Configure(config);
}

YT_DEFINE_CONFIGURABLE_SINGLETON(
    "grpc_dispatcher",
    TDispatcherConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
