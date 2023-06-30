#pragma once

#include "public.h"

#include <yt/yt/core/net/address.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

struct IListener
    : public virtual TRefCounted
{
    virtual const TNetworkAddress& GetAddress() const = 0;
    virtual TFuture<IConnectionPtr> Accept() = 0;
    virtual void Shutdown() = 0;
};

DEFINE_REFCOUNTED_TYPE(IListener)

IListenerPtr CreateListener(
    const TNetworkAddress& address,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor,
    int maxBacklogSize = 8192);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
