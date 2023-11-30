#pragma once

#include "public.h"
#include "peer_discovery.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes,
    IPeerDiscoveryPtr peerDiscovery = CreateDefaultPeerDiscovery());

IRoamingChannelProviderPtr CreateBalancingChannelProvider(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes,
    IPeerDiscoveryPtr peerDiscovery = CreateDefaultPeerDiscovery());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
