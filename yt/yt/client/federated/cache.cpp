#include "cache.h"
#include "connection.h"

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/cache/cache_base.h>
#include <yt/yt/client/cache/rpc.h>

#include <util/string/split.h>

namespace NYT::NClient::NFederated {

using namespace NYT::NClient::NCache;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public NCache::TClientsCacheBase
{
public:
    TClientsCache(
        TClientsCacheConfigPtr clientsCacheConfig,
        NApi::TClientOptions options,
        TConnectionConfigPtr federationConfig,
        TString clusterSeparator)
        : ClientsCacheConfig_(std::move(clientsCacheConfig))
        , Options_(std::move(options))
        , FederationConfig_(std::move(federationConfig))
        , ClusterSeparator_(std::move(clusterSeparator))
    { }
protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        std::vector<std::string> clusters;
        NApi::IClientPtr client;
        StringSplitter(clusterUrl).SplitByString(ClusterSeparator_).SkipEmpty().Collect(&clusters);
        switch (clusters.size()) {
            case 0:
                THROW_ERROR_EXCEPTION("Cannot create client without cluster");
            case 1:
                return NCache::CreateClient(NCache::GetConnectionConfig(ClientsCacheConfig_, clusterUrl), Options_);
            default:
                return CreateFederatedClient(clusters);
        }
    }

private:
    NApi::IClientPtr CreateFederatedClient(const std::vector<std::string>& clusters)
    {
        THashSet<std::string> seenClusters;
        for (const auto& connectionConfig : FederationConfig_->RpcProxyConnections) {
            THROW_ERROR_EXCEPTION_UNLESS(
                connectionConfig->ClusterUrl,
                "Cluster URL is mandatory for federated client connection config");
            seenClusters.insert(connectionConfig->ClusterUrl.value());
        }

        THROW_ERROR_EXCEPTION_UNLESS(
            clusters.size() == seenClusters.size(),
            "Numbers of desired (%Qv) and configured (%Qv) clusters do not match",
            clusters,
            seenClusters);

        for (const auto& cluster : clusters) {
            THROW_ERROR_EXCEPTION_UNLESS(
                seenClusters.contains(cluster),
                "No federated client configuration for cluster %Qv", cluster);
        }

        if (!FederatedConnection_) {
            // TODO(ashishkin): use proper invoker here?
            NApi::NRpcProxy::TConnectionOptions options;
            FederatedConnection_ = CreateConnection(FederationConfig_, std::move(options));
        }
        return FederatedConnection_->CreateClient(Options_);
    }

private:
    const TClientsCacheConfigPtr ClientsCacheConfig_;
    const NApi::TClientOptions Options_;
    const NFederated::TConnectionConfigPtr FederationConfig_;
    const TString ClusterSeparator_;
    NApi::IConnectionPtr FederatedConnection_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const TClientsCacheConfigPtr& clientsCacheConfig,
    const NApi::TClientOptions& options,
    TString clusterSeparator)
{
    return NYT::New<TClientsCache>(
        clientsCacheConfig,
        options,
        std::move(federatedConfig),
        std::move(clusterSeparator));
}

IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const NApi::NRpcProxy::TConnectionConfigPtr& connectionConfig,
    const NApi::TClientOptions& options,
    TString clusterSeparator)
{
    auto clientsCacheConfig = New<TClientsCacheConfig>();
    clientsCacheConfig->DefaultConnection = CloneYsonStruct(
        connectionConfig,
        /*postprocess*/ false,
        /*setDefaults*/ false);

    return NYT::New<TClientsCache>(
        std::move(clientsCacheConfig),
        options,
        std::move(federatedConfig),
        std::move(clusterSeparator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
