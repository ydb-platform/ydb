#include "config.h"

#include "address_helpers.h"

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/https/config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NApi::NRpcProxy {

using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

TConnectionConfigPtr TConnectionConfig::CreateFromClusterUrl(
    TString clusterUrl,
    std::optional<TString> proxyRole)
{
    auto config = New<TConnectionConfig>();
    config->ClusterUrl = std::move(clusterUrl);
    config->ProxyRole = std::move(proxyRole);
    config->Postprocess();
    return config;
}

void TConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_url", &TThis::ClusterUrl)
        .Default();
    registrar.Parameter("cluster_tag", &TThis::ClusterTag)
        .Optional();
    registrar.Parameter("proxy_role", &TThis::ProxyRole)
        .Optional();
    registrar.Parameter("proxy_address_type", &TThis::ProxyAddressType)
        .Optional();
    registrar.Parameter("proxy_network_name", &TThis::ProxyNetworkName)
        .Optional();
    registrar.Parameter("proxy_addresses", &TThis::ProxyAddresses)
        .Alias("addresses")
        .Optional();
    registrar.Parameter("proxy_endpoints", &TThis::ProxyEndpoints)
        .Optional();
    registrar.Parameter("proxy_unix_domain_socket", &TThis::ProxyUnixDomainSocket)
        .Optional();
    registrar.Parameter("enable_proxy_discovery", &TThis::EnableProxyDiscovery)
        .Default(true);

    registrar.Parameter("dynamic_channel_pool", &TThis::DynamicChannelPool)
        .DefaultNew();

    registrar.Parameter("ping_period", &TThis::PingPeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("proxy_list_update_period", &TThis::ProxyListUpdatePeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("proxy_list_retry_period", &TThis::ProxyListRetryPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_proxy_list_retry_period", &TThis::MaxProxyListRetryPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("max_proxy_list_update_attempts", &TThis::MaxProxyListUpdateAttempts)
        .Default(3);

    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("rpc_acknowledgement_timeout", &TThis::RpcAcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("timestamp_provider_latest_timestamp_update_period", &TThis::TimestampProviderLatestTimestampUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("default_transaction_timeout", &TThis::DefaultTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("default_lookup_rows_timeout", &TThis::DefaultLookupRowsTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("default_select_rows_timeout", &TThis::DefaultSelectRowsTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("default_total_streaming_timeout", &TThis::DefaultTotalStreamingTimeout)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("default_streaming_stall_timeout", &TThis::DefaultStreamingStallTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("default_ping_period", &TThis::DefaultPingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("idle_channel_ttl", &TThis::IdleChannelTtl)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("http_client", &TThis::HttpClient)
        .DefaultNew();
    registrar.Parameter("https_client", &TThis::HttpsClient)
        .DefaultNew();

    registrar.Parameter("request_codec", &TThis::RequestCodec)
        .Default(NCompression::ECodec::None);
    registrar.Parameter("response_codec", &TThis::ResponseCodec)
        .Default(NCompression::ECodec::None);
    // COMPAT(danilalexeev  ): legacy RPC codecs
    registrar.Parameter("enable_legacy_rpc_codecs", &TThis::EnableLegacyRpcCodecs)
        .Default(true);

    registrar.Parameter("enable_retries", &TThis::EnableRetries)
        .Default(false);
    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultNew();

    registrar.Parameter("modify_rows_batch_capacity", &TThis::ModifyRowsBatchCapacity)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Preprocessor([] (TThis* config) {
        config->DynamicChannelPool->MaxPeerCount = 100;
    });

    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Default(NObjectClient::InvalidCellTag);

    registrar.Parameter("udf_registry_path", &TThis::UdfRegistryPath)
        .Optional();

    registrar.Parameter("enable_select_query_tracing_tag", &TThis::EnableSelectQueryTracingTag)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (!config->ProxyEndpoints && !config->ClusterUrl && !config->ProxyAddresses && !config->ProxyUnixDomainSocket) {
            THROW_ERROR_EXCEPTION("Either \"endpoints\" or \"cluster_url\" or \"proxy_addresses\" or \"proxy_unix_domain_socket\" must be specified");
        }
        if (config->ProxyEndpoints && config->ProxyRole) {
            THROW_ERROR_EXCEPTION("\"proxy_role\" is not supported by Service Discovery");
        }
        if (config->ProxyAddresses && config->ProxyAddresses->empty()) {
            THROW_ERROR_EXCEPTION("\"proxy_addresses\" must not be empty");
        }
        if (!config->EnableProxyDiscovery && !config->ProxyAddresses) {
            THROW_ERROR_EXCEPTION("If proxy discovery is disabled, \"proxy_addresses\" should be specified");
        }

        if (!config->ClusterName && config->ClusterUrl) {
            config->ClusterName = InferYTClusterFromClusterUrl(*config->ClusterUrl);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
