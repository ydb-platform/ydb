#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <yt/yt/core/bus/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusNetworkCounters;
using TBusNetworkCountersPtr = TIntrusivePtr<TBusNetworkCounters>;

DECLARE_REFCOUNTED_CLASS(TMultiplexingBandConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TBusConfig)
DECLARE_REFCOUNTED_CLASS(TBusDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TBusServerConfig)
DECLARE_REFCOUNTED_CLASS(TBusServerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TBusClientConfig)
DECLARE_REFCOUNTED_CLASS(TBusClientDynamicConfig)

struct IPacketTranscoderFactory;

YT_DECLARE_RECONFIGURABLE_SINGLETON(TTcpDispatcherConfig, TTcpDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

