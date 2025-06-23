#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>

#include <util/generic/hash.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionConfig
    : public NApi::TConnectionConfig
{
    static TConnectionConfigPtr CreateFromClusterUrl(
        const std::string& clusterUrl,
        const std::optional<std::string>& proxyRole = {});

    std::optional<std::string> ClusterUrl;
    std::optional<TClusterTag> ClusterTag;
    std::optional<std::string> ProxyRole;
    std::optional<EAddressType> ProxyAddressType;
    std::optional<std::string> ProxyNetworkName;
    std::optional<std::vector<std::string>> ProxyAddresses;
    NRpc::TServiceDiscoveryEndpointsConfigPtr ProxyEndpoints;
    std::optional<std::string> ProxyUnixDomainSocket;
    bool EnableProxyDiscovery;
    THashMap<std::string, std::string> ProxyUrlAliasingRules;

    NRpc::TDynamicChannelPoolConfigPtr DynamicChannelPool;

    TDuration PingPeriod;
    TDuration ProxyListUpdatePeriod;
    TDuration ProxyListRetryPeriod;
    TDuration MaxProxyListRetryPeriod;
    int MaxProxyListUpdateAttempts;

    TDuration RpcTimeout;
    std::optional<TDuration> RpcAcknowledgementTimeout;

    TDuration TimestampProviderLatestTimestampUpdatePeriod;

    TDuration DefaultTransactionTimeout;
    TDuration DefaultLookupRowsTimeout;
    TDuration DefaultSelectRowsTimeout;
    TDuration DefaultTotalStreamingTimeout;
    TDuration DefaultStreamingStallTimeout;
    TDuration DefaultPingPeriod;
    TDuration DefaultChaosLeaseTimeout;

    NBus::TBusConfigPtr BusClient;
    TDuration IdleChannelTtl;

    NHttp::TClientConfigPtr HttpClient;
    NHttps::TClientConfigPtr HttpsClient;

    NCompression::ECodec RequestCodec;
    NCompression::ECodec ResponseCodec;

    bool EnableLegacyRpcCodecs;

    bool EnableRetries;
    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    i64 ModifyRowsBatchCapacity;

    NObjectClient::TCellTag ClockClusterTag;

    //! Path in Cypress with UDFs.
    std::optional<NYPath::TYPath> UdfRegistryPath;

    //! If |true| select query will be added to tracing tags of SelectRows span.
    bool EnableSelectQueryTracingTag;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

void ValidateConnectionConfig(const TConnectionConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
