#pragma once

#include "connection.h"

#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

// TODO(prime@): Create HTTP endpoint for discovery that works without authentication.
#include <yt/yt/core/service_discovery/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public NApi::IConnection
{
public:
    TConnection(TConnectionConfigPtr config, TConnectionOptions options);
    ~TConnection();

    NRpc::IChannelPtr CreateChannel(bool sticky);
    NRpc::IChannelPtr CreateChannelByAddress(const std::string& address);

    const TConnectionConfigPtr& GetConfig();

    // IConnection implementation.
    TClusterTag GetClusterTag() const override;
    const std::string& GetLoggingTag() const override;
    const std::string& GetClusterId() const override;
    const std::optional<std::string>& GetClusterName() const override;

    bool IsSameCluster(const IConnectionPtr& other) const override;

    IInvokerPtr GetInvoker() override;

    NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        NHiveClient::TCellId cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    void ClearMetadataCaches() override;

    void Terminate() override;
    bool IsTerminated() const override;

    NYson::TYsonString GetConfigYson() const override;

private:
    friend class TClient;
    friend class TTransaction;
    friend class TTimestampProvider;

    const TConnectionConfigPtr Config_;

    std::atomic<bool> Terminated_ = false;
    std::atomic<bool> ProxyListUpdateStarted_ = false;

    const TGuid ConnectionId_;
    const std::string LoggingTag_;
    const std::string ClusterId_;
    const NLogging::TLogger Logger;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NRpc::IChannelFactoryPtr CachingChannelFactory_;
    const NRpc::TDynamicChannelPoolPtr ChannelPool_;

    const NConcurrency::TActionQueuePtr ActionQueue_;
    const IInvokerPtr ConnectionInvoker_;

    TBackoffStrategy UpdateProxyListBackoffStrategy_;

    const NServiceDiscovery::IServiceDiscoveryPtr ServiceDiscovery_;

    // TODO(prime@): Create HTTP endpoint for discovery that works without authentication.
    NThreading::TAtomicObject<std::string> DiscoveryToken_;

    std::vector<std::string> DiscoverProxiesViaHttp();
    std::vector<std::string> DiscoverProxiesViaServiceDiscovery();

    void ScheduleProxyListUpdate(TDuration delay);
    void OnProxyListUpdate();
};

DEFINE_REFCOUNTED_TYPE(TConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
