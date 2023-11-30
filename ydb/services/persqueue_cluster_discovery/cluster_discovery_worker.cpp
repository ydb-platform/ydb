#include "cluster_discovery_worker.h"

#include <ydb/services/persqueue_cluster_discovery/cluster_ordering/weighed_ordering.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/string/ascii.h>

#include <algorithm>


namespace NKikimr::NPQ::NClusterDiscovery::NWorker {

using namespace Ydb::PersQueue::ClusterDiscovery;
using NKikimr::NPQ::NClusterDiscovery::NClusterOrdering::OrderByHashAndWeight;

inline auto& Ctx() {
    return TActivationContext::AsActorContext();
}

class TClusterDiscoveryWorker : public TActorBootstrapped<TClusterDiscoveryWorker> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TClusterDiscoveryWorker(TEvDiscoverPQClustersRequest::TPtr& ev,
                            TLabeledAddressClassifier::TConstPtr datacenterClassifier,
                            TLabeledAddressClassifier::TConstPtr cloudNetsClassifier,
                            TClustersList::TConstPtr clustersList,
                            TClusterDiscoveryCounters::TPtr counters)
        : Request(ev->Release().Release())
        , DatacenterClassifier(std::move(datacenterClassifier))
        , CloudNetsClassifier(std::move(cloudNetsClassifier))
        , ClustersList(std::move(clustersList))
        , Counters(counters)
    {}

    void Bootstrap() {
        RequestStartTs = Ctx().Now();
        Process();
    }

    template<typename TSelectClusterFunc>
    bool MoveTheBestClusterToFront(std::vector<TClustersList::TCluster>& clusters, const TSelectClusterFunc selectClusterFunc) const {
        auto it = std::find_if(begin(clusters), end(clusters), selectClusterFunc);

        if (it == clusters.end()) {
            return false;
        } else if (it != begin(clusters)) {
            std::swap(*begin(clusters), *it);
        }

        return true;
    }

    bool FillWriteSessionClusters(const TClustersList& clustersList,
                                  const WriteSessionParams& sessionParams,
                                  WriteSessionClusters& sessionClusters) const
    {
        auto clusters = clustersList.Clusters; // make a copy for reordering

        bool movedFirstPriorityCluster = false;
        WriteSessionClusters::SelectionReason primaryClusterSelectionReason = WriteSessionClusters::CONSISTENT_DISTRIBUTION;

        if (sessionParams.preferred_cluster_name()) {
            movedFirstPriorityCluster = MoveTheBestClusterToFront(clusters,
                [preferred_cluster_name = sessionParams.preferred_cluster_name()](const auto& cluster) {
                    return AsciiEqualsIgnoreCase(cluster.Name, preferred_cluster_name);
                }
            );

            if (!movedFirstPriorityCluster) {
                return false; // failed to prioritize the specified cluster
            }

            primaryClusterSelectionReason = WriteSessionClusters::CLIENT_PREFERENCE;
        }

        if (!movedFirstPriorityCluster && ClientDatacenterName && !IsInfracloudClient) {
            movedFirstPriorityCluster = MoveTheBestClusterToFront(clusters,
                [datacenter = *ClientDatacenterName](const auto& cluster) {
                    return AsciiEqualsIgnoreCase(cluster.Datacenter, datacenter);
                }
            );

            if (movedFirstPriorityCluster) {
                primaryClusterSelectionReason = WriteSessionClusters::CLIENT_LOCATION;
            }
        }

        ui64 hashValue = THash<TString>()(sessionParams.topic());
        hashValue = CombineHashes(hashValue, THash<TString>()(sessionParams.source_id()));
        hashValue = CombineHashes(hashValue, static_cast<ui64>(sessionParams.partition_group()));

        auto reorderingBeginIt = begin(clusters);
        if (!clusters.empty() && movedFirstPriorityCluster) {
            ++reorderingBeginIt;
        }

        OrderByHashAndWeight(reorderingBeginIt, end(clusters), hashValue, [](const auto& cluster){ return cluster.Weight; });

        sessionClusters.set_primary_cluster_selection_reason(primaryClusterSelectionReason);
        ReportPrimaryClusterSelectionReason(primaryClusterSelectionReason);

        if (!clusters.empty()) {
            ReportPrioritizedCluster(clusters.front());
        }

        for (const auto& cluster : clusters) {
            auto& clusterInfo = *sessionClusters.add_clusters();
            clusterInfo.set_endpoint(cluster.Balancer);
            clusterInfo.set_name(cluster.Name);
            clusterInfo.set_available(cluster.IsEnabled);
        }

        return true;
    }

    bool FillReadSessionClusters(const TClustersList& clustersList,
                                 const ReadSessionParams& sessionParams,
                                 ReadSessionClusters& sessionClusters) const
    {
        if (sessionParams.has_all_original()) {
            for (const auto& cluster : clustersList.Clusters) {
                auto& clusterInfo = *sessionClusters.add_clusters();
                clusterInfo.set_endpoint(cluster.Balancer);
                clusterInfo.set_name(cluster.Name);
                clusterInfo.set_available(true); // at the moment we can't logically disable reading
            }
        } else {
            auto it = std::find_if(begin(clustersList.Clusters), end(clustersList.Clusters),
                [mirror_to_cluster = sessionParams.mirror_to_cluster()](const auto& cluster) {
                    return AsciiEqualsIgnoreCase(cluster.Name, mirror_to_cluster);
                }
            );

            if (it != end(clustersList.Clusters)) {
                auto& clusterInfo = *sessionClusters.add_clusters();
                clusterInfo.set_endpoint(it->Balancer);
                clusterInfo.set_name(it->Name);
                clusterInfo.set_available(true);
            } else {
                return false;
            }
        }

        return true;
    }

    TMaybe<TString> TryDetectDatacenter(const TString& address) const {
        if (!DatacenterClassifier) {
            return Nothing();
        }

        return DatacenterClassifier->ClassifyAddress(address);
    }

    void Process() {
        auto* result = TEvDiscoverPQClustersRequest::AllocateResult<DiscoverClustersResult>(Request);

        auto statusCode = Ydb::StatusIds::INTERNAL_ERROR;

        if (ClustersList) {
            const TString address = NAddressClassifier::ExtractAddress(Request->GetPeerName());

            ClientDatacenterName = TryDetectDatacenter(address);

            if (CloudNetsClassifier && CloudNetsClassifier->ClassifyAddress(address)) {
                IsInfracloudClient = true;
            }

            statusCode = ProcessWriteSessions(*result);
            if (statusCode == Ydb::StatusIds::SUCCESS) {
                statusCode = ProcessReadSessions(*result);
            }

            result->set_version(ClustersList->Version);
        }

        ReportMetrics(statusCode);

        Request->SendResult(*result, statusCode);

        return PassAway();
    }

    Ydb::StatusIds::StatusCode ProcessWriteSessions(DiscoverClustersResult& result) const {
        const size_t sessionsCount = Request->GetProtoRequest()->write_sessions_size();
        for (size_t i = 0; i < sessionsCount; ++i) {
            if (!FillWriteSessionClusters(*ClustersList,
                                          Request->GetProtoRequest()->write_sessions(i),
                                          *result.add_write_sessions_clusters()))
            {
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }

        return Ydb::StatusIds::SUCCESS;
    }

    Ydb::StatusIds::StatusCode ProcessReadSessions(DiscoverClustersResult& result) const {
        const size_t sessionsCount = Request->GetProtoRequest()->read_sessions_size();
        for (size_t i = 0; i < sessionsCount; ++i) {
            if (!FillReadSessionClusters(*ClustersList,
                                         Request->GetProtoRequest()->read_sessions(i),
                                         *result.add_read_sessions_clusters()))
            {
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }

        return Ydb::StatusIds::SUCCESS;
    }

    // Monitoring

    template<typename TMappedCounters, typename TLabel>
    void IncrementLabeledCounter(TMappedCounters& counters, const TLabel& label) const {
        auto it = counters.find(label);

        Y_ABORT_UNLESS(it != counters.end());

        it->second->Inc();
    }

    void ReportPrioritizedCluster(const TClustersList::TCluster& cluster) const {
        IncrementLabeledCounter(Counters->TimesPrioritizedForWrite, cluster.Name);
    }

    void ReportPrimaryClusterSelectionReason(const WriteSessionClusters::SelectionReason reason) const {
        IncrementLabeledCounter(Counters->PrimaryClusterSelectionReasons, reason);
    }

    void ReportMetrics(const Ydb::StatusIds::StatusCode statusCode) const {
        if (statusCode == Ydb::StatusIds::SUCCESS) {
            Counters->SuccessfulRequestsCount->Inc();
        } else if (statusCode == Ydb::StatusIds::BAD_REQUEST) {
            Counters->BadRequestsCount->Inc();
        } else {
            Y_ABORT_UNLESS(statusCode == Ydb::StatusIds::INTERNAL_ERROR);
            Counters->FailedRequestsCount->Inc();
        }

        Counters->WriteDiscoveriesCount->Add(Request->GetProtoRequest()->write_sessions_size());
        Counters->ReadDiscoveriesCount->Add(Request->GetProtoRequest()->read_sessions_size());

        if (ClientDatacenterName) {
            IncrementLabeledCounter(Counters->TimesResolvedByClassifier, *ClientDatacenterName);
        }

        if (IsInfracloudClient) {
            Counters->InfracloudRequestsCount->Inc();
        }

        Counters->DiscoveryWorkingDurationMs->Collect(Ctx().Now().MilliSeconds() - RequestStartTs.MilliSeconds());
    }

private:
    THolder<TEvDiscoverPQClustersRequest> Request;
    TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null
    TLabeledAddressClassifier::TConstPtr CloudNetsClassifier; // Special classifier instance for cloud networks detection. May be null
    TMaybe<TString> ClientDatacenterName; // Detected client datacenter
    bool IsInfracloudClient = false; // Client came from infracloud
    TClustersList::TConstPtr ClustersList; // Cached list of clusters
    TInstant RequestStartTs;

    TClusterDiscoveryCounters::TPtr Counters;
};

IActor* CreateClusterDiscoveryWorker(TEvDiscoverPQClustersRequest::TPtr& ev,
                                     TLabeledAddressClassifier::TConstPtr datacenterClassifier,
                                     TLabeledAddressClassifier::TConstPtr cloudNetsClassifier,
                                     TClustersList::TConstPtr clustersList,
                                     TClusterDiscoveryCounters::TPtr counters)
{
    return new TClusterDiscoveryWorker(ev,
                                       std::move(datacenterClassifier),
                                       std::move(cloudNetsClassifier),
                                       std::move(clustersList),
                                       std::move(counters)
    );
}

} // namespace NKikimr::NPQ::NClusterDiscovery::NWorker
