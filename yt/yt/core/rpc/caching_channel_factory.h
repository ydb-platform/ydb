#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel factory that wraps another channel factory
//! and caches its channels by address.
//! These channels expire after some time of inactivity.
IChannelFactoryPtr CreateCachingChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TDuration idleChannelTtl = TDuration::Minutes(5));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
