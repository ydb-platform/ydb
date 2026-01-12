#pragma once

#include "public.h"

#include "packet.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TCertProfiler
{
    //! Profiler to output certificate data.
    NProfiling::TProfiler Profiler;
    //! Invoker to read certificate data periodically.
    IInvokerPtr Invoker;
};

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreatePublicTcpBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<TCertProfiler> certProfiler = std::nullopt);

IBusServerPtr CreateLocalTcpBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<TCertProfiler> certProfiler = std::nullopt);

IBusServerPtr CreateBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<TCertProfiler> certProfiler = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
