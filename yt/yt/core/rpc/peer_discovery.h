#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IDiscoverRequestHook
    : public TRefCounted
{
    virtual void EnrichRequest(NProto::TReqDiscover* request) const = 0;
    virtual void HandleResponse(NProto::TRspDiscover* response) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscoverRequestHook);

////////////////////////////////////////////////////////////////////////////////

struct TPeerDiscoveryResponse
{
    bool IsUp;
    std::vector<std::string> Addresses;
};

struct IPeerDiscovery
    : public TRefCounted
{
    virtual TFuture<TPeerDiscoveryResponse> Discover(
        IChannelPtr channel,
        const std::string& address,
        TDuration timeout,
        TDuration replyDelay,
        const std::string& serviceName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPeerDiscovery)

////////////////////////////////////////////////////////////////////////////////

IPeerDiscoveryPtr CreateDefaultPeerDiscovery(IDiscoverRequestHookPtr hook = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
