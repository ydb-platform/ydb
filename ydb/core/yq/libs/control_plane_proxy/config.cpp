#include "config.h"

namespace NYq {

namespace {

TDuration GetDuration(const TString& value, const TDuration& defaultValue)
{
    TDuration result = defaultValue;
    TDuration::TryParse(value, result);
    return result;
}

NConfig::TControlPlaneProxyConfig FillDefaultParameters(NConfig::TControlPlaneProxyConfig config)
{
    if (!config.GetRequestTimeout()) {
        config.SetRequestTimeout("30s");
    }

    if (!config.GetMetricsTtl()) {
        config.SetMetricsTtl("1d");
    }

    return config;
}

}

TControlPlaneProxyConfig::TControlPlaneProxyConfig(const NConfig::TControlPlaneProxyConfig& config)
    : Proto(FillDefaultParameters(config))
    , RequestTimeout(GetDuration(Proto.GetRequestTimeout(), TDuration::Seconds(30)))
    , MetricsTtl(GetDuration(Proto.GetMetricsTtl(), TDuration::Days(1)))
{
}

} // NYq
