#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
{
public:
    static TConnectionConfigPtr CreateFromClusterUrl(
        TString clusterUrl,
        std::optional<TString> proxyRole = {});

    std::optional<TString> ClusterUrl;
    std::optional<TClusterTag> ClusterTag;
    std::optional<TString> ProxyRole;
    std::optional<EAddressType> ProxyAddressType;
    std::optional<TString> ProxyNetworkName;
    std::optional<std::vector<TString>> ProxyAddresses;
    NRpc::TServiceDiscoveryEndpointsConfigPtr ProxyEndpoints;
    std::optional<TString> ProxyUnixDomainSocket;
    bool EnableProxyDiscovery;

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

} // namespace NYT::NApi::NRpcProxy
