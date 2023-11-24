#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TPeerDiscoveryResponse
{
    bool IsUp;
    std::vector<TString> Addresses;
};

struct IPeerDiscovery
    : public TRefCounted
{
    virtual TFuture<TPeerDiscoveryResponse> Discover(
        IChannelPtr channel,
        TDuration timeout,
        TDuration replyDelay,
        const std::string& serviceName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPeerDiscovery)

////////////////////////////////////////////////////////////////////////////////

IPeerDiscoveryPtr CreateDefaultPeerDiscovery(TDiscoverRequestHook hook);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
