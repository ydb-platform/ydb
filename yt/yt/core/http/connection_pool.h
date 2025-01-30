#pragma once

#include "config.h"
#include "stream.h"

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/dialer.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class TConnectionPool
    : public TRefCounted
{
public:
    TConnectionPool(
        NNet::IDialerPtr dialer,
        TClientConfigPtr config,
        IInvokerPtr invoker);

    ~TConnectionPool();

    TFuture<NNet::IConnectionPtr> Connect(
        const NNet::TNetworkAddress& address,
        NNet::TDialerContextPtr context = nullptr);

    void Release(const NNet::IConnectionPtr& connection);

private:
    const NNet::IDialerPtr Dialer_;
    const TClientConfigPtr Config_;

    struct TPooledConnection
    {
        NNet::IConnectionPtr Connection;
        TInstant InsertionTime;

        TDuration GetIdleTime() const;
        bool IsValid() const;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TMultiLruCache<NNet::TNetworkAddress, TPooledConnection> Cache_;
    NConcurrency::TPeriodicExecutorPtr ExpirationExecutor_;

    bool CheckPooledConnection(const TPooledConnection& pooledConnection);
    void DropExpiredConnections();
};

DEFINE_REFCOUNTED_TYPE(TConnectionPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
