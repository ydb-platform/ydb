#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <yt/yt/core/bus/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusNetworkCounters;
using TBusNetworkCountersPtr = TIntrusivePtr<TBusNetworkCounters>;

DECLARE_REFCOUNTED_STRUCT(TMultiplexingBandConfig)
DECLARE_REFCOUNTED_STRUCT(TTcpDispatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TTcpDispatcherDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TBusConfig)
DECLARE_REFCOUNTED_STRUCT(TBusDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TBusServerConfig)
DECLARE_REFCOUNTED_STRUCT(TBusServerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TBusClientConfig)
DECLARE_REFCOUNTED_STRUCT(TBusClientDynamicConfig)

struct IPacketTranscoderFactory;

YT_DECLARE_RECONFIGURABLE_SINGLETON(TTcpDispatcherConfig, TTcpDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

