#pragma once

#include "public.h"

#include "packet.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreatePublicTcpBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NProfiling::TProfiler> profiler = std::nullopt,
    IInvokerPtr invoker = nullptr);

IBusServerPtr CreateLocalTcpBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NProfiling::TProfiler> profiler = std::nullopt,
    IInvokerPtr invoker = nullptr);

IBusServerPtr CreateBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NProfiling::TProfiler> profiler = std::nullopt,
    IInvokerPtr invoker = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
