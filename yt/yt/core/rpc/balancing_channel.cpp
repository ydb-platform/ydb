#include "balancing_channel.h"
#include "channel.h"
#include "caching_channel_factory.h"
#include "config.h"
#include "roaming_channel.h"
#include "dynamic_channel_pool.h"
#include "dispatcher.h"
#include "hedging_channel.h"

#include <yt/yt/core/service_discovery/service_discovery.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/hedging_manager.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NRpc {

using namespace NYTree;
using namespace NConcurrency;
using namespace NServiceDiscovery;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBalancingChannelSubprovider)
DECLARE_REFCOUNTED_CLASS(TBalancingChannelProvider)

class TBalancingChannelSubprovider
    : public TRefCounted
{
public:
    TBalancingChannelSubprovider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        IAttributeDictionaryPtr endpointAttributes,
        std::string serviceName,
        IPeerDiscoveryPtr peerDiscovery)
        : Config_(std::move(config))
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(*endpointAttributes)
                .Item("service").Value(serviceName)
            .EndMap()))
        , ServiceName_(std::move(serviceName))
        , Pool_(New<TDynamicChannelPool>(
            Config_,
            std::move(channelFactory),
            EndpointDescription_,
            EndpointAttributes_,
            ServiceName_,
            peerDiscovery))
    {
        if (Config_->Addresses) {
            ConfigureFromAddresses();
        } else if (Config_->Endpoints) {
            ConfigureFromEndpoints();
        } else {
            // Must not happen for a properly validated configuration.
            Pool_->SetPeerDiscoveryError(TError("No endpoints configured"));
        }
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request)
    {
        auto hedgingOptions = Config_->HedgingDelay
            ? std::make_optional(
                THedgingChannelOptions{
                    .HedgingManager = CreateSimpleHedgingManager(*Config_->HedgingDelay),
                    .CancelPrimaryOnHedging = Config_->CancelPrimaryRequestOnHedging,
                })
            : std::nullopt;
        return Pool_->GetChannel(request, hedgingOptions);
    }

    TFuture<IChannelPtr> GetChannel()
    {
        return Pool_->GetRandomChannel();
    }

    void Terminate(const TError& error)
    {
        Pool_->Terminate(error);
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const TString ServiceName_;

    const TDynamicChannelPoolPtr Pool_;

    IServiceDiscoveryPtr ServiceDiscovery_;
    TPeriodicExecutorPtr EndpointsUpdateExecutor_;

    void ConfigureFromAddresses()
    {
        Pool_->SetPeers(*Config_->Addresses);
    }

    void ConfigureFromEndpoints()
    {
        ServiceDiscovery_ = TDispatcher::Get()->GetServiceDiscovery();
        if (!ServiceDiscovery_) {
            Pool_->SetPeerDiscoveryError(TError("No Service Discovery is configured"));
            return;
        }

        EndpointsUpdateExecutor_ = New<TPeriodicExecutor>(
            TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TBalancingChannelSubprovider::UpdateEndpoints, MakeWeak(this)),
            Config_->Endpoints->UpdatePeriod);
        EndpointsUpdateExecutor_->Start();
    }

    void UpdateEndpoints()
    {
        std::vector<TFuture<TEndpointSet>> asyncEndpointSets;
        for (const auto& cluster : Config_->Endpoints->Clusters) {
            asyncEndpointSets.push_back(ServiceDiscovery_->ResolveEndpoints(
                cluster,
                Config_->Endpoints->EndpointSetId));
        }

        AllSet(asyncEndpointSets)
            .Subscribe(BIND(&TBalancingChannelSubprovider::OnEndpointsResolved, MakeWeak(this)));
    }

    void OnEndpointsResolved(const TErrorOr<std::vector<TErrorOr<TEndpointSet>>>& endpointSetsOrError)
    {
        const auto& endpointSets = endpointSetsOrError.ValueOrThrow();

        std::vector<TString> allAddresses;
        std::vector<TError> errors;
        for (const auto& endpointSetOrError : endpointSets) {
            if (!endpointSetOrError.IsOK()) {
                errors.push_back(endpointSetOrError);
                YT_LOG_WARNING(endpointSetOrError, "Could not resolve endpoints from cluster");
                continue;
            }

            auto addresses = AddressesFromEndpointSet(endpointSetOrError.Value());
            allAddresses.insert(allAddresses.end(), addresses.begin(), addresses.end());
        }

        if (errors.size() == endpointSets.size()) {
            Pool_->SetPeerDiscoveryError(
                TError("Endpoints could not be resolved in any cluster") << errors);
            return;
        }

        Pool_->SetPeers(allAddresses);
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelSubprovider)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelProvider
    : public IRoamingChannelProvider
{
public:
    TBalancingChannelProvider(
        TBalancingChannelConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        IAttributeDictionaryPtr endpointAttributes,
        IPeerDiscoveryPtr peerDiscovery)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , EndpointDescription_(Format("%v%v",
            endpointDescription,
            Config_->Addresses))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("addresses").Value(Config_->Addresses)
                .Items(*endpointAttributes)
            .EndMap()))
        , PeerDiscovery_(std::move(peerDiscovery))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request) override
    {
        if (Config_->DisableBalancingOnSingleAddress &&
            Config_->Addresses &&
            Config_->Addresses->size() == 1)
        {
            return MakeFuture(ChannelFactory_->CreateChannel((*Config_->Addresses)[0]));
        } else {
            return GetSubprovider(request->GetService())->GetChannel(request);
        }
    }

    TFuture<IChannelPtr> GetChannel(std::string serviceName) override
    {
        return GetSubprovider(std::move(serviceName))->GetChannel();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        YT_UNIMPLEMENTED();
    }

    void Terminate(const TError& error) override
    {
        std::vector<TBalancingChannelSubproviderPtr> subproviders;
        {
            auto guard = ReaderGuard(SpinLock_);
            for (const auto& [_, subprovider] : SubproviderMap_) {
                subproviders.push_back(subprovider);
            }
        }

        for (const auto& subprovider : subproviders) {
            subprovider->Terminate(error);
        }
    }

private:
    const TBalancingChannelConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const IPeerDiscoveryPtr PeerDiscovery_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<std::string, TBalancingChannelSubproviderPtr> SubproviderMap_;


    TBalancingChannelSubproviderPtr GetSubprovider(std::string serviceName)
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }
        }

        {
            auto guard = WriterGuard(SpinLock_);
            auto it = SubproviderMap_.find(serviceName);
            if (it != SubproviderMap_.end()) {
                return it->second;
            }

            auto subprovider = New<TBalancingChannelSubprovider>(
                Config_,
                ChannelFactory_,
                EndpointDescription_,
                EndpointAttributes_,
                serviceName,
                PeerDiscovery_);
            EmplaceOrCrash(SubproviderMap_, serviceName, subprovider);
            return subprovider;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelProvider)

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    IAttributeDictionaryPtr endpointAttributes,
    IPeerDiscoveryPtr peerDiscovery)
{
    auto channelProvider = CreateBalancingChannelProvider(
        std::move(config),
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        std::move(peerDiscovery));

    return CreateRoamingChannel(channelProvider);
}

IRoamingChannelProviderPtr CreateBalancingChannelProvider(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    IAttributeDictionaryPtr endpointAttributes,
    IPeerDiscoveryPtr peerDiscovery)
{
    YT_VERIFY(config);
    YT_VERIFY(channelFactory);
    YT_VERIFY(peerDiscovery);

    return New<TBalancingChannelProvider>(
        std::move(config),
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        std::move(peerDiscovery));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
