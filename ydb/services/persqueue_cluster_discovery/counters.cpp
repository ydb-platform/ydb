#include "counters.h"

namespace NKikimr::NPQ::NClusterDiscovery::NCounters {

#define SETUP_SIMPLE_COUNTER(name, derived) \
    name(counters->GetCounter(#name, derived))

TClusterDiscoveryCounters::TClusterDiscoveryCounters(TIntrusivePtr<TDynamicCounters> counters,
                                                     NClusterTracker::TClustersList::TConstPtr clustersList,
                                                     TLabeledAddressClassifier::TConstPtr addressClassifier)
    : SETUP_SIMPLE_COUNTER(NetDataUpdateLagSeconds, false)
    , SETUP_SIMPLE_COUNTER(ClustersListUpdateLagSeconds, false)
    , SETUP_SIMPLE_COUNTER(HealthProbe, false)
    , SETUP_SIMPLE_COUNTER(DroppedRequestsCount, true)
    , SETUP_SIMPLE_COUNTER(TotalRequestsCount, true)
    , SETUP_SIMPLE_COUNTER(SuccessfulRequestsCount, true)
    , SETUP_SIMPLE_COUNTER(BadRequestsCount, true)
    , SETUP_SIMPLE_COUNTER(FailedRequestsCount, true)
    , SETUP_SIMPLE_COUNTER(WriteDiscoveriesCount, true)
    , SETUP_SIMPLE_COUNTER(ReadDiscoveriesCount, true)
    , SETUP_SIMPLE_COUNTER(InfracloudRequestsCount, true)
{
    DiscoveryWorkingDurationMs =
        counters->GetHistogram("DiscoveryWorkingDurationMs",
                               NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 25, 50, 100, 500}));

    if (clustersList) {
        for (const auto& cluster : clustersList->Clusters) {
            TimesPrioritizedForWrite[cluster.Name] =
                counters->GetSubgroup("prioritized_cluster", cluster.Name)->GetCounter("TimesPrioritizedForWrite", true);
        }
    }

    if (addressClassifier) {
        for (const auto& label : addressClassifier->GetLabels()) {
            TimesResolvedByClassifier[label] =
                counters->GetSubgroup("resolved_dc", label)->GetCounter("TimesResolvedByClassifier", true);
        }
    }

    for (
        std::underlying_type<WriteSessionClusters::SelectionReason>::type reason = WriteSessionClusters::SELECTION_REASON_UNSPECIFIED;
        reason <= WriteSessionClusters::CONSISTENT_DISTRIBUTION;
        ++reason
    )
    {
        const auto currentReason = static_cast<WriteSessionClusters::SelectionReason>(reason);
        PrimaryClusterSelectionReasons[currentReason] =
            counters->GetSubgroup("primary_cluster_selection_reason", ToString(currentReason))->GetCounter("TimesMentionedForWrite", true);
    }

}

#undef SETUP_SIMPLE_COUNTER

} // namespace NKikimr::NPQ::NClusterDiscovery::NCounters
