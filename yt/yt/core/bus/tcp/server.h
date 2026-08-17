#pragma once

#include "public.h"
#include "packet.h"

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus::NTcp {

////////////////////////////////////////////////////////////////////////////////

//! A TCP-backed server.
struct IBusServer
    : public NBus::IBusServer
{
    //! Apply new dynamic config.
    virtual void Reconfigure(const TBusServerDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBusServer)

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
