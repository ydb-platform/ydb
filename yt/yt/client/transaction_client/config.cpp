#include "config.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

void TRemoteTimestampProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("latest_timestamp_update_period", &TThis::LatestTimestampUpdatePeriod)
        // COMPAT(babenko)
        .Alias("update_period")
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("batch_period", &TThis::BatchPeriod)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("enable_timestamp_provider_discovery", &TThis::EnableTimestampProviderDiscovery)
        .Default(false);
    registrar.Parameter("timestamp_provider_discovery_period", &TThis::TimestampProviderDiscoveryPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("timestamp_provider_discovery_period_splay", &TThis::TimestampProviderDiscoveryPeriodSplay)
        .Default(TDuration::Seconds(10));

    registrar.Preprocessor([] (TThis* config) {
        config->RetryAttempts = 100;
        config->RetryTimeout = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAlienTimestampProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag);
    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
