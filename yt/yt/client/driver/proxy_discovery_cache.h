#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/api/rpc_proxy/address_helpers.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TProxyDiscoveryRequest
{
    NApi::EProxyType Type;
    std::string Role = NApi::DefaultRpcProxyRole;
    NApi::NRpcProxy::EAddressType AddressType = NApi::NRpcProxy::DefaultAddressType;
    std::string NetworkName = NApi::NRpcProxy::DefaultNetworkName;
    bool IgnoreBalancers = false;

    bool operator==(const TProxyDiscoveryRequest& other) const = default;

    operator size_t() const;
};

void FormatValue(TStringBuilderBase* builder, const TProxyDiscoveryRequest& request, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TProxyDiscoveryResponse
{
    std::vector<std::string> Addresses;
};

////////////////////////////////////////////////////////////////////////////////

struct IProxyDiscoveryCache
    : public virtual TRefCounted
{
    virtual TFuture<TProxyDiscoveryResponse> Discover(
        const TProxyDiscoveryRequest& request) = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyDiscoveryCache)

////////////////////////////////////////////////////////////////////////////////

IProxyDiscoveryCachePtr CreateProxyDiscoveryCache(
    TAsyncExpiringCacheConfigPtr config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
