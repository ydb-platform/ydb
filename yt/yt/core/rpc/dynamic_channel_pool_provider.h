#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

IRoamingChannelProviderPtr CreateDynamicChannelPoolProvider(
    TDynamicChannelPoolPtr pool,
    std::string endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes);

IRoamingChannelProviderPtr CreateStickyDynamicChannelPoolProvider(
    TDynamicChannelPoolPtr pool,
    std::string endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
