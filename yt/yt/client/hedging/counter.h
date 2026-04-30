#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <optional>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

// ! Counters which will be collected from yt-client.
struct TCounter final
{
    explicit TCounter(
        const std::string& clusterName,
        TDuration requestDurationHistogramMin = TDuration::MilliSeconds(1),
        TDuration requestDurationHistogramMax = TDuration::MilliSeconds(70),
        const std::optional<TDuration>& requestDurationHistogramGranularity = std::nullopt);
    explicit TCounter(
        const NProfiling::TTagSet& tagSet,
        TDuration requestDurationHistogramMin = TDuration::MilliSeconds(1),
        TDuration requestDurationHistogramMax = TDuration::MilliSeconds(70),
        const std::optional<TDuration>& requestDurationHistogramGranularity = std::nullopt);
    explicit TCounter(
        const NProfiling::TRegistry& registry,
        TDuration requestDurationHistogramMin = TDuration::MilliSeconds(1),
        TDuration requestDurationHistogramMax = TDuration::MilliSeconds(70),
        const std::optional<TDuration>& requestDurationHistogramGranularity = std::nullopt);

    NProfiling::TCounter SuccessRequestCount;
    NProfiling::TCounter CancelRequestCount;
    NProfiling::TCounter ErrorRequestCount;
    NProfiling::TTimeGauge EffectivePenalty;
    NProfiling::TTimeGauge ExternalPenalty;
    NProfiling::TEventTimer RequestDuration;
};

DEFINE_REFCOUNTED_TYPE(TCounter)

////////////////////////////////////////////////////////////////////////////////

// ! Counters for TReplicationLagPenaltyProvider.
struct TLagPenaltyProviderCounters final
{
    TLagPenaltyProviderCounters(const NProfiling::TRegistry& registry, const std::vector<std::string>& clusters);
    TLagPenaltyProviderCounters(const NYPath::TYPath& tablePath, const std::vector<std::string>& replicaClusters);

    NProfiling::TCounter SuccessRequestCount;
    NProfiling::TCounter ErrorRequestCount;
};

DEFINE_REFCOUNTED_TYPE(TLagPenaltyProviderCounters)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
