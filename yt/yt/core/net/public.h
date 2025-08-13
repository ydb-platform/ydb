#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/configurable_singleton_decl.h>
#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <library/cpp/yt/misc/guid.h>

#ifdef _linux_
    #include <sys/signalfd.h>
#endif

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetworkAddress;
class TIP6Address;
class TIP6Network;

using TConnectionId = TGuid;

DEFINE_ENUM(EDeliveryFencedMode,
    (None)
    (New)
    // COMPAT(pogorelov)
    (Old)
);

#ifdef _linux_
static inline const auto DeliveryFencedWriteSignal = SIGRTMIN;
#endif // _linux

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IPacketConnection)
DECLARE_REFCOUNTED_STRUCT(IConnectionReader)
DECLARE_REFCOUNTED_STRUCT(IConnectionWriter)
DECLARE_REFCOUNTED_STRUCT(IListener)
DECLARE_REFCOUNTED_STRUCT(TDialerContext)
DECLARE_REFCOUNTED_STRUCT(IDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialerSession)

DECLARE_REFCOUNTED_STRUCT(TDialerConfig)
DECLARE_REFCOUNTED_STRUCT(TAddressResolverConfig)

YT_DEFINE_ERROR_ENUM(
    ((Aborted)         (1500))
    ((ResolveTimedOut) (1501))
);

YT_DECLARE_CONFIGURABLE_SINGLETON(TAddressResolverConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
