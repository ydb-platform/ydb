#include "connection_pool.h"

#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HttpLogger;

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
                auto&& connection = item->Connection;
                YT_LOG_DEBUG("Connection is extracted from cache (Address: %v, ConnectionId: %v)",
                    address,
                    connection->GetId());
                return MakeFuture<IConnectionPtr>(std::move(connection));
            }
        }
    }

    return Dialer_->Dial(address, std::move(context));
}

void TConnectionPool::Release(const IConnectionPtr& connection)
{
    YT_LOG_DEBUG("Connection is put to cache (Address: %v, ConnectionId: %v)",
        connection->GetRemoteAddress(),
        connection->GetId());

    auto guard = Guard(SpinLock_);
    Connections_.Insert(connection->GetRemoteAddress(), {connection, TInstant::Now()});
}

void TConnectionPool::DropExpiredConnections()
{
    auto guard = Guard(SpinLock_);

    decltype(Connections_) validConnections(Config_->MaxIdleConnections);

    while (Connections_.GetSize() > 0) {
        auto idleConnection = Connections_.Pop();
        if (idleConnection.GetIdleTime() < Config_->ConnectionIdleTimeout && idleConnection.IsOK()) {
            validConnections.Insert(idleConnection.Connection->GetRemoteAddress(), idleConnection);
        } else {
            YT_LOG_DEBUG("Connection expired from cache (Address: %v, ConnectionId: %v)",
                idleConnection.Connection->GetRemoteAddress(),
                idleConnection.Connection->GetId());
        }
    }

    Connections_ = std::move(validConnections);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
