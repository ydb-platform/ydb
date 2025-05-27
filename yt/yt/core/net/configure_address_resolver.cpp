#include "address.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NNet {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TAddressResolverConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TAddressResolverConfigPtr& config)
{
    TAddressResolver::Get()->Configure(config);
}

YT_DEFINE_CONFIGURABLE_SINGLETON(
    "address_resolver",
    TAddressResolverConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
