#pragma once

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt_proto/yt/client/cache/proto/config.pb.h>

#include <util/generic/strbuf.h>


namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

NApi::NRpcProxy::TConnectionConfigPtr GetConnectionConfig(const TConfig& config);

////////////////////////////////////////////////////////////////////////////////

//! Helper function to extract ClusterName and ProxyRole from clusterUrl.
std::pair<TStringBuf, TStringBuf> ExtractClusterAndProxyRole(TStringBuf clusterUrl);

//! Helper function to properly set ClusterName and ProxyRole from clusterUrl.
//  Expected that clusterUrl will be in format cluster[/proxyRole].
void SetClusterUrl(const NApi::NRpcProxy::TConnectionConfigPtr& config, TStringBuf clusterUrl);
void SetClusterUrl(TConfig& config, TStringBuf clusterUrl);

NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config, const NApi::TClientOptions& options);
NApi::IClientPtr CreateClient(const TConfig& config, const NApi::TClientOptions& options);

//! Shortcut to create rpc client with options from env variables
NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config);
NApi::IClientPtr CreateClient(const TConfig& config);

//! Shortcut to create client to specified cluster.
//  `clusterUrl` may have format cluster[/proxyRole],
//  where cluster may be short name of yt cluster (markov, hahn) or ip address + port.
NApi::IClientPtr CreateClient(TStringBuf clusterUrl);

//! Allows to specify proxyRole as dedicated option.
NApi::IClientPtr CreateClient(TStringBuf cluster, TStringBuf proxyRole);

//! Shortcut to create client with default config and options from env variables (use env:YT_PROXY as serverName).
NApi::IClientPtr CreateClient();

//! Shortcut to create client to cluster with custom options.
NApi::IClientPtr CreateClient(TStringBuf clusterUrl, const NApi::TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
