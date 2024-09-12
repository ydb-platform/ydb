#include "cache_base.h"
#include "rpc.h"
#include "config.h"

#include <yt/yt/client/api/options.h>

#include <yt/yt/core/net/address.h>

#include <util/stream/str.h>

namespace NYT::NClient::NCache {

using namespace NNet;
using NApi::NRpcProxy::TConnectionConfig;
using NApi::NRpcProxy::TConnectionConfigPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetNormalClusterName(TStringBuf clusterName)
{
    return NNet::InferYTClusterFromClusterUrlRaw(clusterName).value_or(clusterName);
}

// TODO(ignat): move this logic to ads/bsyeti/libs/ytex/client/
TClientsCacheConfigPtr GetClustersConfigWithNormalClusterName(const TClientsCacheConfigPtr& config)
{
    auto newConfig = New<TClientsCacheConfig>();

    newConfig->DefaultConfig = CloneYsonStruct(config->DefaultConfig, /*postprocess*/ false, /*setDefaults*/ false);
    for (const auto& [clusterName, clusterConfig] : config->ClusterConfigs) {
        newConfig->ClusterConfigs[ToString(GetNormalClusterName(clusterName))] =
            CloneYsonStruct(clusterConfig, /*postprocess*/ false, /*setDefaults*/ false);
    }
    return newConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TConnectionConfigPtr MakeClusterConfig(
    const TClientsCacheConfigPtr& clustersConfig,
    TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    auto it = clustersConfig->ClusterConfigs.find(GetNormalClusterName(cluster));
    auto config = (it != clustersConfig->ClusterConfigs.end()) ? it->second : clustersConfig->DefaultConfig;

    auto newConfig = CloneYsonStruct(config, /*postprocess*/ false, /*setDefaults*/ false);
    newConfig->ClusterUrl = ToString(cluster);
    newConfig->ClusterName = InferYTClusterFromClusterUrl(*newConfig->ClusterUrl);
    if (!proxyRole.empty()) {
        newConfig->ProxyRole = ToString(proxyRole);
    }
    return newConfig;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public TClientsCacheBase
{
public:
    TClientsCache(const TClientsCacheConfigPtr& config, const NApi::TClientOptions& options)
        : ClustersConfig_(GetClustersConfigWithNormalClusterName(config))
        , Options_(options)
    { }

protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        return NCache::CreateClient(MakeClusterConfig(ClustersConfig_, clusterUrl), Options_);
    }

private:
    const TClientsCacheConfigPtr ClustersConfig_;
    const NApi::TClientOptions Options_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IClientsCachePtr CreateClientsCache(const TClientsCacheConfigPtr& config, const NApi::TClientOptions& options)
{
    return New<TClientsCache>(config, options);
}

IClientsCachePtr CreateClientsCache(
    const TConnectionConfigPtr& config,
    const NApi::TClientOptions& options)
{
    auto clustersConfig = New<TClientsCacheConfig>();
    clustersConfig->DefaultConfig = CloneYsonStruct(config, /*postprocess*/ false, /*setDefaults*/ false);
    return CreateClientsCache(clustersConfig, options);
}

IClientsCachePtr CreateClientsCache(const TConnectionConfigPtr& config)
{
    return CreateClientsCache(config, NApi::GetClientOptionsFromEnvStatic());
}

IClientsCachePtr CreateClientsCache(const NApi::TClientOptions& options)
{
    auto config = New<TClientsCacheConfig>();
    config->SetDefaults();
    return CreateClientsCache(config, options);
}

IClientsCachePtr CreateClientsCache()
{
    return CreateClientsCache(NApi::GetClientOptionsFromEnvStatic());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
