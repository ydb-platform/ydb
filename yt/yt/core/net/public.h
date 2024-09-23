#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetworkAddress;
class TIP6Address;
class TIP6Network;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IPacketConnection)
DECLARE_REFCOUNTED_STRUCT(IConnectionReader)
DECLARE_REFCOUNTED_STRUCT(IConnectionWriter)
DECLARE_REFCOUNTED_STRUCT(IListener)
DECLARE_REFCOUNTED_STRUCT(TDialerContext)
DECLARE_REFCOUNTED_STRUCT(IDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialerSession)

DECLARE_REFCOUNTED_CLASS(TDialerConfig)
DECLARE_REFCOUNTED_CLASS(TAddressResolverConfig)

YT_DEFINE_ERROR_ENUM(
    ((Aborted)         (1500))
    ((ResolveTimedOut) (1501))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
