#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NServiceDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    TString Id;
    TString Protocol;
    TString Fqdn;
    TString IP4Address;
    TString IP6Address;
    int Port;

    //! Identifies whether this endpoint is ready to serve traffic according to the provider.
    /*!
     * Must not be used in runtime systems due to the following:
     * - provider downtime (which is considered as a normal state by design) causes flag staleness;
     * - difference in a network connectivity of (client <> endpoint) and (provider <> endpoint) makes flag useless for the client.
     *
     * Better use client-specific probes to identify ready and alive endpoints.
     *
     * See https://st.yandex-team.ru/YT-16705 for details.
     */
    bool Ready;
};

struct TEndpointSet
{
    TString Id;

    std::vector<TEndpoint> Endpoints;
};

////////////////////////////////////////////////////////////////////////////////

struct IServiceDiscovery
    : public virtual TRefCounted
{
    virtual TFuture<TEndpointSet> ResolveEndpoints(
        const TString& cluster,
        const TString& endpointSetId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery
