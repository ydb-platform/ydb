#pragma once

#include "public.h"

#include "packet.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateBusServer(
    TBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
