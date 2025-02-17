#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/public.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NRpc::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via Bus.
IChannelPtr CreateBusChannel(
    NYT::NBus::IBusClientPtr client,
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

//! Creates a factory for creating TCP Bus channels.
IChannelFactoryPtr CreateTcpBusChannelFactory(
    NYT::NBus::TBusConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

//! Creates a factory for creating Unix domain socket (UDS) Bus channels.
IChannelFactoryPtr CreateUdsBusChannelFactory(
    NYT::NBus::TBusConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NBus
