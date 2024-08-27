#pragma once

#include "connection.h"

#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

// TODO(prime@): Create HTTP endpoint for discovery that works without authentication.
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/service_discovery/public.h>

#include <yt/yt/core/logging/log.h>

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
    const TString& GetLoggingTag() const override;
    const TString& GetClusterId() const override;
    const std::optional<std::string>& GetClusterName() const override;

    bool IsSameCluster(const IConnectionPtr& other) const override;

    IInvokerPtr GetInvoker() override;

    NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        NHiveClient::TCellId cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    void ClearMetadataCaches() override;

    void Terminate() override;

    NYson::TYsonString GetConfigYson() const override;

private:
    friend class TClient;
    friend class TTransaction;
    friend class TTimestampProvider;

    const TConnectionConfigPtr Config_;

    const TGuid ConnectionId_;
    const TString LoggingTag_;
    const TString ClusterId_;
    const NLogging::TLogger Logger;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NRpc::IChannelFactoryPtr CachingChannelFactory_;
    const NRpc::TDynamicChannelPoolPtr ChannelPool_;

    NConcurrency::TActionQueuePtr ActionQueue_;
    IInvokerPtr ConnectionInvoker_;

    NConcurrency::TPeriodicExecutorPtr UpdateProxyListExecutor_;

    // TODO(prime@): Create HTTP endpoint for discovery that works without authentication.
    TAtomicObject<TString> DiscoveryToken_;

    NServiceDiscovery::IServiceDiscoveryPtr ServiceDiscovery_;

    std::vector<std::string> DiscoverProxiesViaHttp();
    std::vector<std::string> DiscoverProxiesViaServiceDiscovery();

    void OnProxyListUpdate();
};

DEFINE_REFCOUNTED_TYPE(TConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
