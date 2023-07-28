#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

class TAresDnsResolverConfig
    : public virtual NYTree::TYsonStruct
{
public:
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
    //! than retrieved via |NYT::NNet::UpdateLocalHostName|.
    std::optional<TString> LocalHostNameOverride;
    int Retries;
    TDuration RetryDelay;
    TDuration ResolveTimeout;
    TDuration MaxResolveTimeout;
    std::optional<double> Jitter;
    TDuration WarningTimeout;
    //! Used to check that bootstrap is being initialized from a correct container.
    std::optional<TString> ExpectedLocalHostName;

    REGISTER_YSON_STRUCT(TAresDnsResolverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAresDnsResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
