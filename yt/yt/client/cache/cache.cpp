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

std::string GetNormalClusterName(TStringBuf clusterName)
{
    return std::string(NNet::InferYTClusterFromClusterUrlRaw(clusterName).value_or(clusterName));
}

// TODO(ignat): move this logic to ads/bsyeti/libs/ytex/client/
TClientsCacheConfigPtr GetClustersConfigWithNormalClusterName(const TClientsCacheConfigPtr& config)
{
    YT_VERIFY(config);
    auto newConfig = New<TClientsCacheConfig>();

    newConfig->DefaultConnection = CloneYsonStruct(
        config->DefaultConnection,
        /*postprocess*/ false,
        /*setDefaults*/ false);
    for (const auto& [cluster, connection] : config->PerClusterConnection) {
        newConfig->PerClusterConnection[ToString(GetNormalClusterName(cluster))] =
            CloneYsonStruct(connection, /*postprocess*/ false, /*setDefaults*/ false);
    }
    return newConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TClientsCache
    : public TClientsCacheBase
{
public:
    TClientsCache(const TClientsCacheConfigPtr& config, const TClientsCacheAuthentificationOptionsPtr& clientsOptions)
        : Config_(GetClustersConfigWithNormalClusterName(config))
        , ClientsOptions_(clientsOptions)
    { }

protected:
    NApi::IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        auto& options = ClientsOptions_->ClusterOptions.ValueRef(clusterUrl, ClientsOptions_->DefaultOptions);
        return NCache::CreateClient(GetConnectionConfig(Config_, clusterUrl), options);
    }

private:
    const TClientsCacheConfigPtr Config_;
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
    const TConnectionConfigPtr& connectionConfig,
    const NApi::TClientOptions& options)
{
    auto config = New<TClientsCacheConfig>();
    config->DefaultConnection = CloneYsonStruct(connectionConfig, /*postprocess*/ false, /*setDefaults*/ false);
    if (config->DefaultConnection->ClusterName) {
        config->PerClusterConnection[*config->DefaultConnection->ClusterName] = config->DefaultConnection;
    }
    return CreateClientsCache(config, options);
}

IClientsCachePtr CreateClientsCache(const TConnectionConfigPtr& connectionConfig)
{
    return CreateClientsCache(connectionConfig, NApi::GetClientOptionsFromEnvStatic());
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

NApi::NRpcProxy::TConnectionConfigPtr GetConnectionConfig(const TClientsCacheConfigPtr& config, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);

    const auto& connectionConfig = config->PerClusterConnection.ValueRef(
        GetNormalClusterName(cluster),
        config->DefaultConnection);
    YT_VERIFY(connectionConfig);
    auto connectionConfigCopy = CloneYsonStruct(connectionConfig, /*postprocess*/ false, /*setDefaults*/ false);

    // Ignore cluster url from DefaultConfig, but use it from ClusterConfigs[_] if it is set.
    if (connectionConfig == config->DefaultConnection ||
        !connectionConfigCopy->ClusterUrl || connectionConfigCopy->ClusterUrl->empty())
    {
        connectionConfigCopy->ClusterUrl = ToString(cluster);
        connectionConfigCopy->ClusterName = InferYTClusterFromClusterUrl(*connectionConfigCopy->ClusterUrl);
    }
    if (!proxyRole.empty()) {
        connectionConfigCopy->ProxyRole = ToString(proxyRole);
    }
    connectionConfigCopy->Postprocess();
    return connectionConfigCopy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
