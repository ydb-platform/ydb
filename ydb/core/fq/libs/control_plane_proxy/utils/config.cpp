#include "config.h"

namespace NFq {

namespace {

NConfig::TControlPlaneProxyConfig FillDefaultParameters(NConfig::TControlPlaneProxyConfig config)
{
    if (!config.GetRequestTimeout()) {
        config.SetRequestTimeout("30s");
    }

    if (!config.GetMetricsTtl()) {
        config.SetMetricsTtl("1d");
    }

    if (!config.GetConfigRetryPeriod()) {
        config.SetConfigRetryPeriod("100ms");
    }

    return config;
}

}

TControlPlaneProxyConfig::TControlPlaneProxyConfig(
    const NConfig::TControlPlaneProxyConfig& config,
    const NConfig::TControlPlaneStorageConfig& storageConfig,
    const NConfig::TComputeConfig& computeConfig,
    const NConfig::TCommonConfig& commonConfig,
    const NYql::TS3GatewayConfig& s3Config)
    : Proto(FillDefaultParameters(config))
    , StorageConfig(TControlPlaneStorageConfig(storageConfig, s3Config, commonConfig, {}))
    , ComputeConfig(computeConfig)
    , CommonConfig(commonConfig)
    , RequestTimeout(GetDuration(Proto.GetRequestTimeout(), TDuration::Seconds(30)))
    , MetricsTtl(GetDuration(Proto.GetMetricsTtl(), TDuration::Days(1)))
    , ConfigRetryPeriod(
          GetDuration(Proto.GetConfigRetryPeriod(), TDuration::MilliSeconds(100))) { }

} // NFq
