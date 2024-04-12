#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IDiscoverRequestHook
    : public TRefCounted
{
    virtual void EnrichRequest(NProto::TReqDiscover* request) const = 0;
    virtual void OnResponse(NProto::TRspDiscover* response) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscoverRequestHook);

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
        const TString& address,
        TDuration timeout,
        TDuration replyDelay,
        const std::string& serviceName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPeerDiscovery)

////////////////////////////////////////////////////////////////////////////////

IPeerDiscoveryPtr CreateDefaultPeerDiscovery(IDiscoverRequestHookPtr hook = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
