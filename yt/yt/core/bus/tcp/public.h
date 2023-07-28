#pragma once

#include <yt/yt/core/bus/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusNetworkCounters;
using TBusNetworkCountersPtr = TIntrusivePtr<TBusNetworkCounters>;

DECLARE_REFCOUNTED_CLASS(TMultiplexingBandConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TBusConfig)
DECLARE_REFCOUNTED_CLASS(TBusServerConfig)
DECLARE_REFCOUNTED_CLASS(TBusClientConfig)

struct IPacketTranscoderFactory;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

