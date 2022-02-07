#pragma once

#include <ydb/core/grpc_services/grpc_request_proxy.h>

#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/util/address_classifier.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>

namespace NKikimr::NPQ::NClusterDiscovery::NCounters {

using namespace NAddressClassifier;
using namespace NClusterTracker;
using namespace NMonitoring;
using namespace Ydb::PersQueue::ClusterDiscovery;

struct TClusterDiscoveryCounters : TAtomicRefCount<TClusterDiscoveryCounters>, TNonCopyable {
    using TPtr = TIntrusivePtr<TClusterDiscoveryCounters>;
    using TCounterPtr = TDynamicCounters::TCounterPtr;

    TClusterDiscoveryCounters(TIntrusivePtr<TDynamicCounters> counters,
                              TClustersList::TConstPtr clustersList,
                              TLabeledAddressClassifier::TConstPtr addressClassifier);

    TCounterPtr NetDataUpdateLagSeconds; // Seconds since last net data update
    TCounterPtr ClustersListUpdateLagSeconds; // Seconds since last clusters list update
    TCounterPtr HealthProbe; // Health check results timeline

    TCounterPtr DroppedRequestsCount; // GRpc status: UNAVAILABLE
    TCounterPtr TotalRequestsCount; // GRpc status: ANY
    TCounterPtr SuccessfulRequestsCount; // GRpc status: SUCCESS
    TCounterPtr BadRequestsCount; // GRrpc status: BAD_REQUEST
    TCounterPtr FailedRequestsCount; // GRpc status: INTERNAL_ERROR

    TCounterPtr WriteDiscoveriesCount; // Write discovery proto elements count in request
    TCounterPtr ReadDiscoveriesCount; // Read discovery proto elements count in request

    TCounterPtr InfracloudRequestsCount; // Requests coming from infracloud clients

    THistogramPtr DiscoveryWorkingDurationMs; // NB: dropped requests are not measured

    THashMap<TString, TCounterPtr> TimesPrioritizedForWrite; // Times a cluster with a certain name was prioritized
    THashMap<TString, TCounterPtr> TimesResolvedByClassifier; // Times a client's datacenter with a certain name was resolved

    // Primary cluster selection reasons for write sessions
    THashMap<WriteSessionClusters::SelectionReason, TCounterPtr> PrimaryClusterSelectionReasons;
};

} // namespace NKikimr::NPQ::NClusterDiscovery::NCounters
