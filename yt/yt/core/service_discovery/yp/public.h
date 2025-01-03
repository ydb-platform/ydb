#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/configurable_singleton_decl.h>

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServiceDiscoveryConfig)

YT_DECLARE_CONFIGURABLE_SINGLETON(TServiceDiscoveryConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
