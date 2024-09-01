#include "connection.h"

#include "config.h"
#include "client.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/client/misc/method_helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/string/guid.h>
#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString MakeConnectionLoggingTag(const std::vector<NApi::IConnectionPtr>& connections, TGuid connectionId)
{
    TStringBuilder builder;
    builder.AppendString("Clusters: (");
    TDelimitedStringBuilderWrapper delimitedBuilder(&builder, "; ");
    for (const auto& connection : connections) {
        delimitedBuilder->AppendString(connection->GetLoggingTag());
    }
    builder.AppendString("), ");
    builder.AppendFormat("ConnectionId: %v", connectionId);
    return builder.Flush();
}

class TConnection
    : public NApi::IConnection
{
public:
    TConnection(
        std::vector<NApi::IConnectionPtr> connections,
        NConcurrency::TActionQueuePtr actionQueue,
        TFederationConfigPtr config)
        : Config_(std::move(config))
        , Connections_(std::move(connections))
        , ActionQueue_(std::move(actionQueue))
        , ConnectionId_(TGuid::Create())
        , LoggingTag_(MakeConnectionLoggingTag(Connections_, ConnectionId_))
    {
        YT_VERIFY(!Connections_.empty());
    }

    const TString& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    IInvokerPtr GetInvoker() override
    {
        return Connections_[0]->GetInvoker();
    }

    NApi::IClientPtr CreateClient(const NApi::TClientOptions& options = {}) override
    {
        std::vector<NApi::IClientPtr> clients;
        clients.reserve(Connections_.size());
        for (auto& connection : Connections_) {
            clients.push_back(connection->CreateClient(options));
        }
        return NFederated::CreateClient(clients, Config_);
    }

    void ClearMetadataCaches() override
    {
        // TODO(bulatman) What about exceptions?
        for (auto& connection : Connections_) {
            connection->ClearMetadataCaches();
        }
    }

    void Terminate() override
    {
        // TODO(bulatman) What about exceptions?
        for (auto& connection : Connections_) {
            connection->Terminate();
        }
    }

    //! Returns a YSON-serialized connection config.
    NYson::TYsonString GetConfigYson() const override
    {
        return NYson::ConvertToYsonString(Config_);
    }

    UNIMPLEMENTED_CONST_METHOD(NApi::TClusterTag, GetClusterTag, ());
    UNIMPLEMENTED_CONST_METHOD(const TString&, GetClusterId, ());
    UNIMPLEMENTED_CONST_METHOD(const std::optional<std::string>&, GetClusterName, ());
    UNIMPLEMENTED_CONST_METHOD(bool, IsSameCluster, (const NApi::IConnectionPtr&));
    UNIMPLEMENTED_METHOD(
        NHiveClient::ITransactionParticipantPtr,
        CreateTransactionParticipant,
        (NHiveClient::TCellId, const NApi::TTransactionParticipantOptions&));

private:
    const TFederationConfigPtr Config_;
    const std::vector<NApi::IConnectionPtr> Connections_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const TGuid ConnectionId_;
    const TString LoggingTag_;
};

} // namespace

NApi::IConnectionPtr CreateConnection(std::vector<NApi::IConnectionPtr> connections, TFederationConfigPtr config)
{
    return New<TConnection>(std::move(connections), nullptr, std::move(config));
}

NApi::IConnectionPtr CreateConnection(TConnectionConfigPtr config, NApi::NRpcProxy::TConnectionOptions options)
{
    NConcurrency::TActionQueuePtr actionQueue;
    if (!options.ConnectionInvoker) {
        actionQueue = New<NConcurrency::TActionQueue>("FederatedConn");
        options.ConnectionInvoker = actionQueue->GetInvoker();
    }
    std::vector<NApi::IConnectionPtr> connections;
    connections.reserve(config->RpcProxyConnections.size());
    for (const auto& rpcProxyConfig : config->RpcProxyConnections) {
        connections.push_back(NApi::NRpcProxy::CreateConnection(rpcProxyConfig, options));
    }

    return New<TConnection>(std::move(connections), std::move(actionQueue), std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
