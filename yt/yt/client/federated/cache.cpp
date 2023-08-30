#include "cache.h"
#include "client.h"

#include <yt/yt/client/cache/cache_base.h>
#include <yt/yt/client/cache/options.h>
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
        TFederationConfigPtr federationConfig,
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
        if (clusters.size() == 1) {
            return NCache::CreateClient(NCache::MakeClusterConfig(ClustersConfig_, clusterUrl), Options_);
        } else {
            std::vector<NYT::NApi::IClientPtr> clients;
            clients.reserve(clusters.size());
            for (auto& cluster : clusters) {
                clients.push_back(GetClient(cluster));
            }
            return NFederated::CreateClient(std::move(clients), FederationConfig_);
        }
    }

private:
    const TClustersConfig ClustersConfig_;
    const NApi::TClientOptions Options_;
    const TFederationConfigPtr FederationConfig_;
    const TString ClusterSeparator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IClientsCachePtr CreateFederatedClientsCache(
    const TClustersConfig& config,
    const NYT::NApi::TClientOptions& options,
    TFederationConfigPtr federatedConfig,
    TString clusterSeparator)
{
    return NYT::New<TClientsCache>(
        std::move(config),
        std::move(options),
        std::move(federatedConfig),
        std::move(clusterSeparator));
}

IClientsCachePtr CreateFederatedClientsCache(
    const TClustersConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString chaosBundleName,
    TString clusterSeparator)
{
    auto federationConfig = NYT::New<NYT::NClient::NFederated::TFederationConfig>();
    if (!chaosBundleName.empty()) {
        federationConfig->BundleName = std::move(chaosBundleName);
    }
    return CreateFederatedClientsCache(
        std::move(config),
        std::move(options),
        std::move(federationConfig),
        std::move(clusterSeparator));
}

IClientsCachePtr CreateFederatedClientsCache(
    const TConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString chaosBundleName,
    TString clusterSeparator)
{
    TClustersConfig clustersConfig;
    *clustersConfig.MutableDefaultConfig() = config;

    return CreateFederatedClientsCache(
        std::move(clustersConfig),
        std::move(options),
        std::move(chaosBundleName),
        std::move(clusterSeparator));
}

IClientsCachePtr CreateFederatedClientsCache(
    TString chaosBundleName,
    TString clusterSeparator)
{
    return CreateFederatedClientsCache(
        TClustersConfig{},
        NCache::GetClientOpsFromEnvStatic(),
        std::move(chaosBundleName),
        std::move(clusterSeparator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
