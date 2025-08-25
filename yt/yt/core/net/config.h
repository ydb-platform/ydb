#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/dns/config.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

struct TDialerConfig
    : public virtual NYTree::TYsonStruct
{
    bool EnableNoDelay;
    bool EnableAggressiveReconnect;

    TDuration MinRto;
    TDuration MaxRto;
    double RtoScale;
    TDuration ConnectTimeout;

    REGISTER_YSON_STRUCT(TDialerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDialerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Configuration for TAddressResolver singleton.
struct TAddressResolverConfig
    : public TAsyncExpiringCacheConfig
    , public NDns::TAresDnsResolverConfig
{
    bool EnableIPv4;
    bool EnableIPv6;

    //! If true, when determining local host name, it will additionally be resolved
    //! into FQDN by calling |getaddrinfo|. Setting this option to false may be
    //! useful in MTN environment, in which hostnames are barely resolvable.
    //! NB: Set this option to false only if you are sure that process is not being
    //! exposed under localhost name to anyone; in particular, any kind of discovery
    //! should be done using some other kind of addresses.
    bool ResolveHostNameIntoFqdn;

    //! If set, localhost name will be forcefully set to the given value rather
    //! than retrieved from the system.
    std::optional<std::string> LocalHostNameOverride;

    REGISTER_YSON_STRUCT(TAddressResolverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAddressResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
