#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

struct TServiceDiscoveryConfig
    : public NRpc::TRetryingChannelConfig
    , public TAsyncExpiringCacheConfig
{
    bool Enable;

    //! Provider endpoint.
    TString Fqdn;
    int GrpcPort;

    //! Provider throttles requests based on this string.
    TString Client;

    REGISTER_YSON_STRUCT(TServiceDiscoveryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
