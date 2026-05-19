#pragma once

#include "public.h"

#include "packet.h"

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus::NTcp {

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateRemoteBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NCrypto::TCertProfiler> certProfiler = std::nullopt);

IBusServerPtr CreateLocalBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NCrypto::TCertProfiler> certProfiler = std::nullopt);

IBusServerPtr CreateBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    std::optional<NCrypto::TCertProfiler> certProfiler = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp
