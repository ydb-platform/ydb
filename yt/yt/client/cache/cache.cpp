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
    YT_VERIFY(config);
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
    const bool useDefaultConfig = (it == clustersConfig->ClusterConfigs.end());
    const auto& config = useDefaultConfig ? clustersConfig->DefaultConfig : it->second;

    auto newConfig = config ? CloneYsonStruct(config, /*postprocess*/ false, /*setDefaults*/ false) : New<NApi::NRpcProxy::TConnectionConfig>();
    // Ignore cluster url from DefaultConfig, but use it from ClusterConfigs[_] if it is set.
    if (useDefaultConfig || !newConfig->ClusterUrl.has_value() || newConfig->ClusterUrl->empty()) {
        newConfig->ClusterUrl = ToString(cluster);
    }
    newConfig->ClusterName = InferYTClusterFromClusterUrl(*newConfig->ClusterUrl);
    if (!proxyRole.empty()) {
        newConfig->ProxyRole = ToString(proxyRole);
    }
    newConfig->Postprocess();
    return newConfig;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public TClientsCacheBase
{
public:
    TClientsCache(const TClientsCacheConfigPtr& config, const TClientsCacheAuthentificationOptionsPtr& clientsOptions)
        : ClustersConfig_(GetClustersConfigWithNormalClusterName(config))
        , ClientsOptions_(clientsOptions)
    { }

protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        auto& options = ClientsOptions_->ClusterOptions.ValueRef(clusterUrl, ClientsOptions_->DefaultOptions);
        return NCache::CreateClient(MakeClusterConfig(ClustersConfig_, clusterUrl), options);
    }

private:
    const TClientsCacheConfigPtr ClustersConfig_;
    const TClientsCacheAuthentificationOptionsPtr ClientsOptions_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IClientsCachePtr CreateClientsCache(const TClientsCacheConfigPtr& config, const TClientsCacheAuthentificationOptionsPtr& options)
{
    return New<TClientsCache>(config, options);
}

IClientsCachePtr CreateClientsCache(const TClientsCacheConfigPtr& config, const NApi::TClientOptions& defaultOptions)
{
    auto options = New<TClientsCacheAuthentificationOptions>();
    options->DefaultOptions = defaultOptions;
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
