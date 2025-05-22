#include "connection_pool.h"

#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = HttpLogger;

////////////////////////////////////////////////////////////////////////////////

TDuration TConnectionPool::TPooledConnection::GetIdleTime() const
{
    return TInstant::Now() - InsertionTime;
}

bool TConnectionPool::TPooledConnection::IsValid() const
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
    , Cache_(Config_->MaxIdleConnections)
    , ExpirationExecutor_(
        New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TConnectionPool::DropExpiredConnections, MakeWeak(this)),
            TPeriodicExecutorOptions::WithJitter(
                Config_->ConnectionIdleTimeout)))
{
    if (Config_->MaxIdleConnections > 0) {
        ExpirationExecutor_->Start();
    }
}

TConnectionPool::~TConnectionPool()
{
    YT_UNUSED_FUTURE(ExpirationExecutor_->Stop());
}

TFuture<IConnectionPtr> TConnectionPool::Connect(
    const TNetworkAddress& address,
    TDialerContextPtr context)
{
    {
        auto guard = Guard(SpinLock_);
        while (auto pooledConnection = Cache_.TryExtract(address)) {
            if (CheckPooledConnection(*pooledConnection)) {
                auto connection = std::move(pooledConnection->Connection);
                YT_LOG_DEBUG("Connection is extracted from cache (ConnectionId: %v)",
                    connection->GetId());
                return MakeFuture<IConnectionPtr>(std::move(connection));
            }
        }
    }

    return Dialer_->Dial(address, std::move(context));
}

void TConnectionPool::Release(const IConnectionPtr& connection)
{
    YT_LOG_DEBUG("Connection is put to cache (ConnectionId: %v)",
        connection->GetId());

    {
        auto guard = Guard(SpinLock_);
        Cache_.Insert(connection->GetRemoteAddress(), {connection, TInstant::Now()});
    }
}

bool TConnectionPool::CheckPooledConnection(const TPooledConnection& pooledConnection)
{
    auto idleTime = pooledConnection.GetIdleTime();
    if (idleTime > Config_->ConnectionIdleTimeout) {
        YT_LOG_DEBUG("Connection evicted from cache due to idle timeout (ConnectionId: %v)",
            pooledConnection.Connection->GetId());
        return false;
    }

    if (!pooledConnection.IsValid()) {
        YT_LOG_DEBUG("Connection evicted from cache due to invalid state (ConnectionId: %v)",
            pooledConnection.Connection->GetId());
        return false;
    }

    return true;
}

void TConnectionPool::DropExpiredConnections()
{
    auto guard = Guard(SpinLock_);

    decltype(Cache_) newCache(Config_->MaxIdleConnections);

    while (Cache_.GetSize() > 0) {
        auto pooledConnection = Cache_.Pop();
        if (CheckPooledConnection(pooledConnection)) {
            newCache.Insert(pooledConnection.Connection->GetRemoteAddress(), pooledConnection);
        }
    }

    Cache_ = std::move(newCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
