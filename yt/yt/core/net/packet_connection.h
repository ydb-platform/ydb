#pragma once

#include "public.h"

#include <yt/yt/core/net/address.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

struct IPacketConnection
    : public virtual TRefCounted
{
    virtual TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(const TSharedMutableRef& buffer) = 0;

    // NOTE: This method is synchronous
    virtual void SendTo(const TSharedRef& buffer, const TNetworkAddress& address) = 0;

    virtual TFuture<void> Abort() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPacketConnection)

////////////////////////////////////////////////////////////////////////////////

IPacketConnectionPtr CreatePacketConnection(
    const TNetworkAddress& at,
    NConcurrency::IPollerPtr poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
