#include "rpc.h"
#include "options.h"

#include <yt/yt_proto/yt/client/cache/proto/config.pb.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NClient::NCache {

NCompression::ECodec GetResponseCodecFromProto(const ECompressionCodec& protoCodec)
{
    switch (protoCodec) {
        case ECompressionCodec::None:
            return NCompression::ECodec::None;
        case ECompressionCodec::Lz4:
            return NCompression::ECodec::Lz4;
    }
    Y_UNREACHABLE();
}

NApi::NRpcProxy::TConnectionConfigPtr GetConnectionConfig(const TConfig& config)
{
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();

    connectionConfig->ClusterUrl = config.GetClusterName();
    if (!config.GetProxyRole().empty()) {
        connectionConfig->ProxyRole = config.GetProxyRole();
    }
    if (0 != config.GetChannelPoolSize()) {
        connectionConfig->DynamicChannelPool->MaxPeerCount = config.GetChannelPoolSize();
    }
    if (0 != config.GetChannelPoolRebalanceIntervalSeconds()) {
        connectionConfig->DynamicChannelPool->RandomPeerEvictionPeriod = TDuration::Seconds(config.GetChannelPoolRebalanceIntervalSeconds());
    }
    if (0 != config.GetModifyRowsBatchCapacity()) {
        connectionConfig->ModifyRowsBatchCapacity = config.GetModifyRowsBatchCapacity();
    }

#define SET_TIMEOUT_OPTION(name) \
    if (0 != config.Get##name()) connectionConfig->name = TDuration::MilliSeconds(config.Get ## name())

    SET_TIMEOUT_OPTION(DefaultTransactionTimeout);
    SET_TIMEOUT_OPTION(DefaultSelectRowsTimeout);
    SET_TIMEOUT_OPTION(DefaultLookupRowsTimeout);
    SET_TIMEOUT_OPTION(DefaultTotalStreamingTimeout);
    SET_TIMEOUT_OPTION(DefaultStreamingStallTimeout);
    SET_TIMEOUT_OPTION(DefaultPingPeriod);

#undef SET_TIMEOUT_OPTION

    connectionConfig->ResponseCodec = GetResponseCodecFromProto(config.GetResponseCodec());
    connectionConfig->EnableRetries = config.GetEnableRetries();

    if (config.HasRetryBackoffTime()) {
        connectionConfig->RetryingChannel->RetryBackoffTime = TDuration::MilliSeconds(config.GetRetryBackoffTime());
    }
    if (config.HasRetryAttempts()) {
        connectionConfig->RetryingChannel->RetryAttempts = config.GetRetryAttempts();
    }
    if (config.HasRetryTimeout()) {
        connectionConfig->RetryingChannel->RetryTimeout = TDuration::MilliSeconds(config.GetRetryTimeout());
    }

    connectionConfig->Postprocess();

    return connectionConfig;
}

////////////////////////////////////////////////////////////////////////////////

std::pair<TStringBuf, TStringBuf> ExtractClusterAndProxyRole(TStringBuf clusterUrl)
{
    TStringBuf cluster;
    TStringBuf proxyRole;
    clusterUrl.Split('/', cluster, proxyRole);
    return {cluster, proxyRole};
}

void SetClusterUrl(const NApi::NRpcProxy::TConnectionConfigPtr& config, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    if (!proxyRole.empty()) {
        Y_ENSURE(!config->ProxyRole || config->ProxyRole.value().empty(), "ProxyRole specified in both: config and url");
        config->ProxyRole = ToString(proxyRole);
    }
    config->ClusterUrl = ToString(cluster);
}

void SetClusterUrl(TConfig& config, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    if (!proxyRole.empty()) {
        Y_ENSURE(config.GetProxyRole().empty(), "ProxyRole specified in both: config and url");
        config.SetProxyRole(ToString(proxyRole));
    }
    config.SetClusterName(ToString(cluster));
}

NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config, const NApi::TClientOptions& options)
{
    return NApi::NRpcProxy::CreateConnection(config)->CreateClient(options);
}

NApi::IClientPtr CreateClient(const TConfig& config, const NApi::TClientOptions& options)
{
    return CreateClient(GetConnectionConfig(config), options);
}

NApi::IClientPtr CreateClient(const NApi::NRpcProxy::TConnectionConfigPtr& config)
{
    return CreateClient(config, GetClientOpsFromEnvStatic());
}

NApi::IClientPtr CreateClient(const TConfig& config)
{
    return CreateClient(GetConnectionConfig(config));
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl)
{
    return CreateClient(clusterUrl, GetClientOpsFromEnvStatic());
}

NApi::IClientPtr CreateClient(TStringBuf cluster, TStringBuf proxyRole)
{
    auto config = New<NApi::NRpcProxy::TConnectionConfig>();
    config->ClusterUrl = ToString(cluster);
    if (!proxyRole.empty()) {
        config->ProxyRole = ToString(proxyRole);
    }
    return CreateClient(config);
}

NApi::IClientPtr CreateClient()
{
    return CreateClient(Strip(GetEnv("YT_PROXY")));
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl, const NApi::TClientOptions& options)
{
    TConfig config;
    SetClusterUrl(config, clusterUrl);
    return CreateClient(config, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
