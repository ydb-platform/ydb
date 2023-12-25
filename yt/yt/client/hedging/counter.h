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
    TLagPenaltyProviderCounters(const NProfiling::TRegistry& registry, const TVector<TString>& clusters);
    TLagPenaltyProviderCounters(const TString& tablePath, const TVector<TString>& replicaClusters);

    NProfiling::TCounter SuccessRequestCount;
    NProfiling::TCounter ErrorRequestCount;
    THashMap<TString, NProfiling::TGauge> LagTabletsCount; // cluster -> # of tablets
    NProfiling::TGauge TotalTabletsCount;
};

DEFINE_REFCOUNTED_TYPE(TLagPenaltyProviderCounters)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
