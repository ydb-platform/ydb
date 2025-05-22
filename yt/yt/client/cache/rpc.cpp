#include "rpc.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NClient::NCache {

using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

std::pair<TStringBuf, TStringBuf> ExtractClusterAndProxyRole(TStringBuf clusterUrl)
{
    static const TStringBuf schemeDelim = "://";

    auto startPos = clusterUrl.find(schemeDelim);
    if (startPos != TStringBuf::npos) {
        startPos += schemeDelim.size();
    } else {
        startPos = 0;
    }

    auto endPos = clusterUrl.rfind('/');
    if (endPos != TStringBuf::npos && endPos > startPos) {
        return {clusterUrl.Head(endPos), clusterUrl.Tail(endPos + 1)};
    } else {
        return {clusterUrl, ""};
    }
}

void SetClusterUrl(const NApi::NRpcProxy::TConnectionConfigPtr& config, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    if (!proxyRole.empty()) {
        // TODO(ignat): avoid using Y_ENSURE
        Y_ENSURE(!config->ProxyRole || config->ProxyRole.value().empty(), "ProxyRole specified in both: config and url");
        config->ProxyRole = ToString(proxyRole);
    }
    config->ClusterUrl = ToString(cluster);
    config->ClusterName = InferYTClusterFromClusterUrl(*config->ClusterUrl);
}

NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config, const NApi::TClientOptions& options)
{
    if (config->ClusterName && *(config->ClusterName) == "" && config->ClusterUrl != "") {
        THROW_ERROR_EXCEPTION("Connection config has empty cluster name but non-empty cluster URL, it usually means misconfiguration")
            << TErrorAttribute("cluster_name", config->ClusterName)
            << TErrorAttribute("cluster_url", config->ClusterUrl);
    }
    return NApi::NRpcProxy::CreateConnection(config)->CreateClient(options);
}

NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config)
{
    return CreateClient(config, NApi::GetClientOptionsFromEnvStatic());
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl)
{
    return CreateClient(clusterUrl, NApi::GetClientOptionsFromEnvStatic());
}

NApi::IClientPtr CreateClient(TStringBuf cluster, std::optional<TStringBuf> proxyRole)
{
    auto config = New<NApi::NRpcProxy::TConnectionConfig>();
    config->ClusterUrl = ToString(cluster);
    if (proxyRole && !proxyRole->empty()) {
        config->ProxyRole = ToString(*proxyRole);
    }
    config->Postprocess();
    return CreateClient(config);
}

NApi::IClientPtr CreateClient()
{
    return CreateClient(Strip(GetEnv("YT_PROXY")));
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl, const NApi::TClientOptions& options)
{
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();

    SetClusterUrl(connectionConfig, clusterUrl);

    return CreateClient(connectionConfig, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
