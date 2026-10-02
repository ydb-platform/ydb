#pragma once

#include "public.h"
#include "packet.h"

#include <yt/yt/core/bus/client.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus::NTcp {

////////////////////////////////////////////////////////////////////////////////

//! A TCP-backed client.
struct IBusClient
    : public NBus::IBusClient
{
    //! Apply new dynamic config.
    virtual void Reconfigure(const TBusClientDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBusClient)

////////////////////////////////////////////////////////////////////////////////

//! Initializes a new client for communicating with a given address.
IBusClientPtr CreateBusClient(
    TBusClientConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp
