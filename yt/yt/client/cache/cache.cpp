#include "cache_base.h"
#include "options.h"
#include "rpc.h"

#include <yt/yt_proto/yt/client/cache/proto/config.pb.h>

#include <util/stream/str.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

TConfig MakeClusterConfig(const TClustersConfig& clustersConfig, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    auto it = clustersConfig.GetClusterConfigs().find(cluster);
    TConfig config = (it != clustersConfig.GetClusterConfigs().end()) ? it->second : clustersConfig.GetDefaultConfig();
    config.SetClusterName(ToString(cluster));
    if (!proxyRole.empty()) {
        config.SetProxyRole(ToString(proxyRole));
    }
    return config;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public TClientsCacheBase
{
public:
    TClientsCache(const TClustersConfig& config, const NApi::TClientOptions& options)
        : ClustersConfig_(config)
        , Options_(options)
    {}

protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        return NCache::CreateClient(MakeClusterConfig(ClustersConfig_, clusterUrl), Options_);
    }

private:
    const TClustersConfig ClustersConfig_;
    const NApi::TClientOptions Options_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IClientsCachePtr CreateClientsCache(const TClustersConfig& config, const NApi::TClientOptions& options)
{
    return New<TClientsCache>(config, options);
}

IClientsCachePtr CreateClientsCache(const TConfig& config, const NApi::TClientOptions& options)
{
    TClustersConfig clustersConfig;
    *clustersConfig.MutableDefaultConfig() = config;
    return CreateClientsCache(clustersConfig, options);
}

IClientsCachePtr CreateClientsCache(const TConfig& config)
{
    return CreateClientsCache(config, GetClientOpsFromEnvStatic());
}

IClientsCachePtr CreateClientsCache(const NApi::TClientOptions& options)
{
    return CreateClientsCache(TClustersConfig{}, options);
}

IClientsCachePtr CreateClientsCache()
{
    return CreateClientsCache(GetClientOpsFromEnvStatic());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
