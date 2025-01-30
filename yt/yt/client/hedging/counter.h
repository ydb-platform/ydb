#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

// ! Counters which will be collected from yt-client.
struct TCounter final
{
    explicit TCounter(const TString& clusterName);
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
    TLagPenaltyProviderCounters(const NProfiling::TRegistry& registry, const std::vector<TString>& clusters);
    TLagPenaltyProviderCounters(const TString& tablePath, const std::vector<TString>& replicaClusters);

    NProfiling::TCounter SuccessRequestCount;
    NProfiling::TCounter ErrorRequestCount;
    // Mapping from cluster replica to number of tablets with lag.
    THashMap<TString, NProfiling::TGauge> TabletWithLagCountPerReplica;
    NProfiling::TGauge TotalTabletCount;
};

DEFINE_REFCOUNTED_TYPE(TLagPenaltyProviderCounters)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
