#include "connection_pool.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TDuration TIdleConnection::GetIdleTime() const
{
    return TInstant::Now() - InsertionTime;
}

bool TIdleConnection::IsOK() const
{
    return Connection->IsIdle();
}

////////////////////////////////////////////////////////////////////////////////

TConnectionPool::TConnectionPool(
    IDialerPtr dialer,
    TClientConfigPtr config,
    IInvokerPtr invoker)
    : Dialer_(std::move(dialer))
    , Config_(std::move(config))
    , Connections_(Config_->MaxIdleConnections)
    , ExpiredConnectionsCollector_(
        New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TConnectionPool::DropExpiredConnections, MakeWeak(this)),
            TPeriodicExecutorOptions::WithJitter(
                Config_->ConnectionIdleTimeout)))
{
    if (Config_->MaxIdleConnections > 0) {
        ExpiredConnectionsCollector_->Start();
    }
}

TConnectionPool::~TConnectionPool()
{
    YT_UNUSED_FUTURE(ExpiredConnectionsCollector_->Stop());
}

TFuture<IConnectionPtr> TConnectionPool::Connect(
    const TNetworkAddress& address,
    TDialerContextPtr context)
{
    {
        auto guard = Guard(SpinLock_);

        while (auto item = Connections_.Extract(address)) {
            if (item->GetIdleTime() < Config_->ConnectionIdleTimeout && item->IsOK()) {
                return MakeFuture<IConnectionPtr>(std::move(item->Connection));
            }
        }
    }

    return Dialer_->Dial(address, std::move(context));
}

void TConnectionPool::Release(const IConnectionPtr& connection)
{
    auto guard = Guard(SpinLock_);
    Connections_.Insert(connection->RemoteAddress(), {connection, TInstant::Now()});
}

void TConnectionPool::DropExpiredConnections()
{
    auto guard = Guard(SpinLock_);

    TMultiLruCache<TNetworkAddress, TIdleConnection> validConnections(
        Config_->MaxIdleConnections);

    while (Connections_.GetSize() > 0) {
        auto idleConnection = Connections_.Pop();
        if (idleConnection.GetIdleTime() < Config_->ConnectionIdleTimeout && idleConnection.IsOK()) {
            validConnections.Insert(idleConnection.Connection->RemoteAddress(), idleConnection);
        }
    }

    Connections_ = std::move(validConnections);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
