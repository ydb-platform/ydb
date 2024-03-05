#include "cache.h"
#include "client.h"
#include "connection.h"

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/cache/cache_base.h>
#include <yt/yt/client/cache/rpc.h>

#include <util/string/split.h>

namespace NYT::NClient::NFederated {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public NCache::TClientsCacheBase
{
public:
    TClientsCache(
        TClustersConfig clustersConfig,
        NApi::TClientOptions options,
        TConnectionConfigPtr federationConfig,
        TString clusterSeparator)
        : ClustersConfig_(std::move(clustersConfig))
        , Options_(std::move(options))
        , FederationConfig_(std::move(federationConfig))
        , ClusterSeparator_(std::move(clusterSeparator))
    {}
protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        std::vector<TString> clusters;
        NYT::NApi::IClientPtr client;
        StringSplitter(clusterUrl).SplitByString(ClusterSeparator_).SkipEmpty().Collect(&clusters);
        switch (clusters.size()) {
            case 0:
                THROW_ERROR_EXCEPTION("Cannot create client without cluster");
            case 1:
                return NCache::CreateClient(NCache::MakeClusterConfig(ClustersConfig_, clusterUrl), Options_);
            default:
                return CreateFederatedClient(clusters);
        }
    }

private:
    NApi::IClientPtr CreateFederatedClient(const std::vector<TString>& clusters)
    {
        THashSet<TString> seenClusters;
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
            NYT::NApi::NRpcProxy::TConnectionOptions options;
            FederatedConnection_ = CreateConnection(FederationConfig_, std::move(options));
        }
        return FederatedConnection_->CreateClient(Options_);
    }

private:
    const TClustersConfig ClustersConfig_;
    const NApi::TClientOptions Options_;
    const NFederated::TConnectionConfigPtr FederationConfig_;
    const TString ClusterSeparator_;
    NApi::IConnectionPtr FederatedConnection_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const TClustersConfig& clustersConfig,
    const NYT::NApi::TClientOptions& options,
    TString clusterSeparator)
{
    return NYT::New<TClientsCache>(
        clustersConfig,
        options,
        std::move(federatedConfig),
        std::move(clusterSeparator));
}

IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const TConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString clusterSeparator)
{
    TClustersConfig clustersConfig;
    *clustersConfig.MutableDefaultConfig() = config;

    return NYT::New<TClientsCache>(
        std::move(clustersConfig),
        options,
        std::move(federatedConfig),
        std::move(clusterSeparator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
