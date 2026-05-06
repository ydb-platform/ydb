#include "counter.h"

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

static const auto HedgingClientProfiler = NProfiling::TRegistry{"/hedging_client"}.WithHot();
static const auto LagPenaltyProviderProfiler = NProfiling::TRegistry{"/lag_penalty_provider"}.WithHot();

////////////////////////////////////////////////////////////////////////////////

TCounter::TCounter(
    const NProfiling::TRegistry& registry,
    TDuration requestDurationHistogramMin,
    TDuration requestDurationHistogramMax,
    std::optional<TDuration> requestDurationHistogramGranularity)
    : SuccessRequestCount(registry.Counter("/requests_success"))
    , CancelRequestCount(registry.Counter("/requests_cancel"))
    , ErrorRequestCount(registry.Counter("/requests_error"))
    , EffectivePenalty(registry.TimeGauge("/effective_penalty"))
    , ExternalPenalty(registry.TimeGauge("/external_penalty"))
    , RequestDuration(
        requestDurationHistogramGranularity
            ? registry.TimeHistogram(
                "/request_duration",
                requestDurationHistogramMin,
                requestDurationHistogramMax,
                requestDurationHistogramGranularity.value())
            : registry.TimeHistogram(
                "/request_duration",
                requestDurationHistogramMin,
                requestDurationHistogramMax))
{ }

TCounter::TCounter(
    const std::string& clusterName,
    TDuration requestDurationHistogramMin,
    TDuration requestDurationHistogramMax,
    std::optional<TDuration> requestDurationHistogramGranularity)
    : TCounter(
        HedgingClientProfiler.WithTag("yt_cluster", clusterName),
        requestDurationHistogramMin,
        requestDurationHistogramMax,
        requestDurationHistogramGranularity)
{ }

TCounter::TCounter(
    const NProfiling::TTagSet& tagSet,
    TDuration requestDurationHistogramMin,
    TDuration requestDurationHistogramMax,
    std::optional<TDuration> requestDurationHistogramGranularity)
    : TCounter(
        HedgingClientProfiler.WithTags(tagSet),
        requestDurationHistogramMin,
        requestDurationHistogramMax,
        requestDurationHistogramGranularity)
{ }

////////////////////////////////////////////////////////////////////////////////

TLagPenaltyProviderCounters::TLagPenaltyProviderCounters(
    const NProfiling::TRegistry& registry,
    [[maybe_unused]] const std::vector<std::string>& clusters)
    : SuccessRequestCount(registry.Counter("/update_success"))
    , ErrorRequestCount(registry.Counter("/update_error"))
{ }

TLagPenaltyProviderCounters::TLagPenaltyProviderCounters(
    const NYPath::TYPath& tablePath,
    const std::vector<std::string>& clusterNames)
    : TLagPenaltyProviderCounters(LagPenaltyProviderProfiler.WithTag("table", tablePath), clusterNames)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
