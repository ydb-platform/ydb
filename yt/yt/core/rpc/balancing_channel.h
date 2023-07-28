#pragma once

#include "public.h"

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
    TDiscoverRequestHook discoverRequestHook = {});

IRoamingChannelProviderPtr CreateBalancingChannelProvider(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes,
    TDiscoverRequestHook discoverRequestHook = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
