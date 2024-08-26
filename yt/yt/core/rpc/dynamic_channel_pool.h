#pragma once

#include "public.h"
#include "peer_discovery.h"
#include "hedging_channel.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

// NB: The internal mechanism performs different kinds of polling on the provided addresses.
// This includes sequentially sending Discover requests to potentially all of them.
// Thus, you should avoid passing an aggressively-caching channel factory to the pool if the amount of peers is large
// and your system's file descriptor limit is low.
// In any case, since this itself is a caching-like structure, adding another caching layer is pointless (and even dangerous).
class TDynamicChannelPool
    : public TRefCounted
{
public:
    TDynamicChannelPool(
        TDynamicChannelPoolConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        NYTree::IAttributeDictionaryPtr endpointAttributes,
        std::string serviceName,
        IPeerDiscoveryPtr peerDiscovery);
    ~TDynamicChannelPool();

    TFuture<IChannelPtr> GetRandomChannel();
    TFuture<IChannelPtr> GetChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions = std::nullopt);

    void SetPeers(const std::vector<std::string>& addresses);
    void SetPeerDiscoveryError(const TError& error);

    void Terminate(const TError& error);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
