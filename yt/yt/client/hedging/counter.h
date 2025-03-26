#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

// ! Counters which will be collected from yt-client.
struct TCounter final
{
    explicit TCounter(const std::string& clusterName);
    explicit TCounter(const NProfiling::TTagSet& tagSet);
    explicit TCounter(const NProfiling::TRegistry& registry);

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
    // Mapping from cluster replica to number of tablets with lag.
    THashMap<TString, NProfiling::TGauge> TabletWithLagCountPerReplica;
    NProfiling::TGauge TotalTabletCount;
};

DEFINE_REFCOUNTED_TYPE(TLagPenaltyProviderCounters)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
