#include "config.h"

namespace NYT::NRpc {

using namespace NBus;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void THistogramExponentialBounds::Register(TRegistrar registrar)
{
    registrar.Parameter("min", &TThis::Min).Default(TDuration::Zero());
    registrar.Parameter("max", &TThis::Max).Default(TDuration::Seconds(2));
}

////////////////////////////////////////////////////////////////////////////////

void THistogramConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("exponential_bounds", &TThis::ExponentialBounds).Optional();
    registrar.Parameter("custom_bounds", &TThis::CustomBounds).Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TServiceCommonConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_per_user_profiling", &TThis::EnablePerUserProfiling)
        .Default(false);
    registrar.Parameter("histogram_timer_profiling", &TThis::HistogramTimerProfiling)
        .Default();
    registrar.Parameter("code_counting", &TThis::EnableErrorCodeCounting)
        .Default(false);
    registrar.Parameter("tracing_mode", &TThis::TracingMode)
        .Default(ERequestTracingMode::Enable);
}

////////////////////////////////////////////////////////////////////////////////

void TServiceCommonDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_per_user_profiling", &TThis::EnablePerUserProfiling)
        .Default();
    registrar.Parameter("histogram_timer_profiling", &TThis::HistogramTimerProfiling)
        .Default();
    registrar.Parameter("code_counting", &TThis::EnableErrorCodeCounting)
        .Default();
    registrar.Parameter("tracing_mode", &TThis::TracingMode)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("services", &TThis::Services)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TServerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("services", &TThis::Services)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_per_user_profiling", &TThis::EnablePerUserProfiling)
        .Optional();
    registrar.Parameter("code_counting", &TThis::EnableErrorCodeCounting)
        .Optional();
    registrar.Parameter("histogram_timer_profiling", &TThis::HistogramTimerProfiling)
        .Default();
    registrar.Parameter("tracing_mode", &TThis::TracingMode)
        .Optional();
    registrar.Parameter("methods", &TThis::Methods)
        .Optional();
    registrar.Parameter("authentication_queue_size_limit", &TThis::AuthenticationQueueSizeLimit)
        .Alias("max_authentication_queue_size")
        .Optional();
    registrar.Parameter("pending_payloads_timeout", &TThis::PendingPayloadsTimeout)
        .Optional();
    registrar.Parameter("pooled", &TThis::Pooled)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TMethodConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heavy", &TThis::Heavy)
        .Optional();
    registrar.Parameter("queue_size_limit", &TThis::QueueSizeLimit)
        .Alias("max_queue_size")
        .Optional();
    registrar.Parameter("queue_byte_size_limit", &TThis::QueueByteSizeLimit)
        .Alias("max_queue_byte_size")
        .Optional();
    registrar.Parameter("concurrency_limit", &TThis::ConcurrencyLimit)
        .Alias("max_concurrency")
        .Optional();
    registrar.Parameter("concurrency_byte_limit", &TThis::ConcurrencyByteLimit)
        .Alias("max_concurrency_byte")
        .Optional();
    registrar.Parameter("log_level", &TThis::LogLevel)
        .Optional();
    registrar.Parameter("request_bytes_throttler", &TThis::RequestBytesThrottler)
        .Default();
    registrar.Parameter("request_weight_throttler", &TThis::RequestWeightThrottler)
        .Default();
    registrar.Parameter("logging_suppression_timeout", &TThis::LoggingSuppressionTimeout)
        .Optional();
    registrar.Parameter("logging_suppression_failed_request_throttler", &TThis::LoggingSuppressionFailedRequestThrottler)
        .Optional();
    registrar.Parameter("tracing_mode", &TThis::TracingMode)
        .Optional();
    registrar.Parameter("pooled", &TThis::Pooled)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TRetryingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retry_backoff_time", &TThis::RetryBackoffTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("retry_attempts", &TThis::RetryAttempts)
        .GreaterThanOrEqual(1)
        .Default(10);
    registrar.Parameter("retry_timeout", &TThis::RetryTimeout)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBalancingChannelConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("discover_timeout", &TThis::DiscoverTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("acknowledgement_timeout", &TThis::AcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("rediscover_period", &TThis::RediscoverPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("rediscover_splay", &TThis::RediscoverSplay)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("hard_backoff_time", &TThis::HardBackoffTime)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("soft_backoff_time", &TThis::SoftBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TViablePeerRegistryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_peer_count", &TThis::MaxPeerCount)
        .GreaterThan(1)
        .Default(100);
    registrar.Parameter("hashes_per_peer", &TThis::HashesPerPeer)
        .GreaterThan(0)
        .Default(10);
    registrar.Parameter("peer_priority_strategy", &TThis::PeerPriorityStrategy)
        .Default(EPeerPriorityStrategy::None);
    registrar.Parameter("min_peer_count_for_priority_awareness", &TThis::MinPeerCountForPriorityAwareness)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("enable_power_of_two_choices_strategy", &TThis::EnablePowerOfTwoChoicesStrategy)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->MinPeerCountForPriorityAwareness > config->MaxPeerCount) {
            THROW_ERROR_EXCEPTION(
                "Value of \"min_peer_count_for_priority_awareness\" cannot be bigger than \"max_peer_count\": %v > %v; please read the corresponding comment",
                config->MinPeerCountForPriorityAwareness,
                config->MaxPeerCount);
        }
    });
}

void TDynamicChannelPoolConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_discover_requests", &TThis::MaxConcurrentDiscoverRequests)
        .GreaterThan(0)
        .Default(10);
    registrar.Parameter("random_peer_eviction_period", &TThis::RandomPeerEvictionPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_peer_polling", &TThis::EnablePeerPolling)
        .Default(false);
    registrar.Parameter("peer_polling_period", &TThis::PeerPollingPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("peer_polling_period_splay", &TThis::PeerPollingPeriodSplay)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("peer_polling_request_timeout", &TThis::PeerPollingRequestTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("discovery_session_timeout", &TThis::DiscoverySessionTimeout)
        .Default(TDuration::Minutes(5))
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TServiceDiscoveryEndpointsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster)
        .Default();
    registrar.Parameter("clusters", &TThis::Clusters)
        .Default();
    registrar.Parameter("endpoint_set_id", &TThis::EndpointSetId);
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Postprocessor([] (TThis* config) {
        if (config->Cluster.has_value() == !config->Clusters.empty()) {
            THROW_ERROR_EXCEPTION("Exactly one of \"cluster\" and \"clusters\" field must be set");
        }

        if (config->Clusters.empty()) {
            config->Clusters.push_back(*config->Cluster);
            config->Cluster.reset();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBalancingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("addresses", &TThis::Addresses)
        .Optional();
    registrar.Parameter("disable_balancing_on_single_address", &TThis::DisableBalancingOnSingleAddress)
        .Default(true);
    registrar.Parameter("endpoints", &TThis::Endpoints)
        .Optional();
    registrar.Parameter("hedging_delay", &TThis::HedgingDelay)
        .Optional();
    registrar.Parameter("cancel_primary_request_on_hedging", &TThis::CancelPrimaryRequestOnHedging)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        int endpointConfigCount = 0;
        if (config->Addresses) {
            ++endpointConfigCount;
        }
        if (config->Endpoints) {
            ++endpointConfigCount;
        }
        if (endpointConfigCount != 1) {
            THROW_ERROR_EXCEPTION("Exactly one of \"addresses\" and \"endpoints\" must be specified");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TThrottlingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rate_limit", &TThis::RateLimit)
        .GreaterThan(0)
        .Default(10);
}

////////////////////////////////////////////////////////////////////////////////

void TThrottlingChannelDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rate_limit", &TThis::RateLimit)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TResponseKeeperConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("eviction_period", &TThis::EvictionPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_eviction_tick_time", &TThis::MaxEvictionTickTime)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("eviction_tick_time_check_period", &TThis::EvictionTickTimeCheckPeriod)
        .Default(1024);
    registrar.Parameter("enable_warmup", &TThis::EnableWarmup)
        .Default(true);
    registrar.Parameter("warmup_time", &TThis::WarmupTime)
        .Default(TDuration::Minutes(6));
    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableWarmup && config->WarmupTime < config->ExpirationTime) {
            THROW_ERROR_EXCEPTION("\"warmup_time\" cannot be less than \"expiration_time\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heavy_pool_size", &TThis::HeavyPoolSize)
        .Default(DefaultHeavyPoolSize)
        .GreaterThan(0);
    registrar.Parameter("compression_pool_size", &TThis::CompressionPoolSize)
        .Default(DefaultCompressionPoolSize)
        .GreaterThan(0);
    registrar.Parameter("alert_on_missing_request_info", &TThis::AlertOnMissingRequestInfo)
        .Default(false);
}

TDispatcherConfigPtr TDispatcherConfig::ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->HeavyPoolSize, dynamicConfig->HeavyPoolSize);
    UpdateYsonStructField(mergedConfig->CompressionPoolSize, dynamicConfig->CompressionPoolSize);
    UpdateYsonStructField(mergedConfig->AlertOnMissingRequestInfo, dynamicConfig->AlertOnMissingRequestInfo);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TDispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heavy_pool_size", &TThis::HeavyPoolSize)
        .Optional()
        .GreaterThan(0);
    registrar.Parameter("compression_pool_size", &TThis::CompressionPoolSize)
        .Optional()
        .GreaterThan(0);
    registrar.Parameter("alert_on_missing_request_info", &TThis::AlertOnMissingRequestInfo)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
