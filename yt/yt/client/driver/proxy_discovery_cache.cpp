#include "proxy_discovery_cache.h"

#include "private.h"

#include <iterator>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/public.h>

#include <yt/yt/client/api/rpc_proxy/address_helpers.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <util/digest/multi.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NApi;
using namespace NConcurrency;
using namespace NApi::NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

TProxyDiscoveryRequest::operator size_t() const
{
    return MultiHash(
        Type,
        Role,
        AddressType,
        NetworkName,
        IgnoreBalancers);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TProxyDiscoveryRequest& request, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Type: %v, Role: %v, AddressType: %v, NetworkName: %v, IgnoreBalancers: %v}",
        request.Type,
        request.Role,
        request.AddressType,
        request.NetworkName,
        request.IgnoreBalancers);
}

////////////////////////////////////////////////////////////////////////////////

class TProxyDiscoveryCache
    : public TAsyncExpiringCache<TProxyDiscoveryRequest, TProxyDiscoveryResponse>
    , public IProxyDiscoveryCache
{
public:
    TProxyDiscoveryCache(
        TAsyncExpiringCacheConfigPtr config,
        IClientPtr client)
        : TAsyncExpiringCache(
            std::move(config),
            DriverLogger().WithTag("Cache: ProxyDiscovery"))
        , Client_(std::move(client))
    { }

    TFuture<TProxyDiscoveryResponse> Discover(
        const TProxyDiscoveryRequest& request) override
    {
        return Get(request);
    }

private:
    const IClientPtr Client_;

    TFuture<TProxyDiscoveryResponse> DoGet(
        const TProxyDiscoveryRequest& request,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return GetResponseByBalancers(request).Apply(
            BIND([=, this, this_ = MakeStrong(this)] (const std::optional<TProxyDiscoveryResponse>& response) {
                if (response) {
                    return MakeFuture<TProxyDiscoveryResponse>(std::move(*response));
                }
                return GetResponseByAddresses(request);
            }).AsyncVia(Client_->GetConnection()->GetInvoker()));
    }

    TFuture<std::optional<TProxyDiscoveryResponse>> GetResponseByBalancers(const TProxyDiscoveryRequest& request)
    {
        if (request.IgnoreBalancers) {
            return MakeFuture<std::optional<TProxyDiscoveryResponse>>(std::nullopt);
        }

        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::LocalCache;
        options.Attributes = {BalancersAttributeName};

        auto path = GetProxyRegistryPath(request.Type) + "/@";
        return Client_->GetNode(path, options).Apply(
            BIND([=] (const TYsonString& yson) -> std::optional<TProxyDiscoveryResponse> {
                auto attributes = ConvertTo<IMapNodePtr>(yson);

                auto balancers = attributes->GetChildValueOrDefault<TBalancersMap>(BalancersAttributeName, {});

                auto responseBalancers = GetBalancersOrNull(balancers, request.Role, request.AddressType, request.NetworkName);

                if (!responseBalancers) {
                    return std::nullopt;
                }

                TProxyDiscoveryResponse response;
                std::move(responseBalancers->begin(), responseBalancers->end(), std::back_inserter(response.Addresses));
                return response;
            }).AsyncVia(Client_->GetConnection()->GetInvoker()));
    }

    TFuture<TProxyDiscoveryResponse> GetResponseByAddresses(const TProxyDiscoveryRequest& request)
    {
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::LocalCache;
        options.SuppressUpstreamSync = true;
        options.SuppressTransactionCoordinatorSync = true;
        options.Attributes = {BannedAttributeName, RoleAttributeName, AddressesAttributeName};

        auto path = GetProxyRegistryPath(request.Type);
        return Client_->GetNode(path, options).Apply(BIND([=] (const TYsonString& yson) {
            TProxyDiscoveryResponse response;

            for (const auto& [proxyAddress, proxyNode] : ConvertTo<THashMap<TString, IMapNodePtr>>(yson)) {
                if (!proxyNode->FindChild(AliveNodeName)) {
                    continue;
                }

                if (proxyNode->Attributes().Get(BannedAttributeName, false)) {
                    continue;
                }

                if (proxyNode->Attributes().Get<TString>(RoleAttributeName, DefaultRpcProxyRole) != request.Role) {
                    continue;
                }

                auto addresses = proxyNode->Attributes().Get<TProxyAddressMap>(AddressesAttributeName, {});
                auto address = GetAddressOrNull(addresses, request.AddressType, request.NetworkName);

                if (address) {
                    response.Addresses.push_back(*address);
                } else {
                    // COMPAT(verytable): Drop it after all rpc proxies migrate to 22.3.
                    if (!proxyNode->Attributes().Contains(AddressesAttributeName)) {
                        response.Addresses.push_back(proxyAddress);
                    }
                }
            }
            return response;
        }).AsyncVia(Client_->GetConnection()->GetInvoker()));
    }


    static TYPath GetProxyRegistryPath(EProxyType type)
    {
        switch (type) {
            case EProxyType::Rpc:
                return RpcProxiesPath;
            case EProxyType::Grpc:
                return GrpcProxiesPath;
            default:
                THROW_ERROR_EXCEPTION("Proxy type %Qlv is not supported",
                    type);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IProxyDiscoveryCachePtr CreateProxyDiscoveryCache(
    TAsyncExpiringCacheConfigPtr config,
    IClientPtr client)
{
    return New<TProxyDiscoveryCache>(
        std::move(config),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
