#include "cluster_discovery_service.h"
#include "cluster_discovery_worker.h"

#include "counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/util/address_classifier.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <algorithm>

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NClusterDiscovery {

using namespace NCounters;

inline auto& Ctx() {
    return TActivationContext::AsActorContext();
}

class TClusterDiscoveryServiceActor: public TActorBootstrapped<TClusterDiscoveryServiceActor> {
public:
    TClusterDiscoveryServiceActor(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : RawCounters(counters)
        , Counters(BuildCounters())
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_CLUSTER_DISCOVERY;
    }

    const auto& Cfg() const {
        return AppData(Ctx())->PQClusterDiscoveryConfig;
    }

    void Bootstrap() {
        Become(&TThis::Initing);

        SubscribeToNetClassifier();
        SubscribeToClusterTracker();

        InitCloudNetData();

        RegisterMonitoringPage();

        StartPeriodicalMonitoring();
    }

private:
    void InitCloudNetData() {
        if (Cfg().HasCloudNetData()) {
            CloudNetworksClassifier = NAddressClassifier::BuildLabeledAddressClassifierFromNetData(Cfg().GetCloudNetData());

            Y_ABORT_UNLESS(CloudNetworksClassifier); // the config is expected to be correct if specified
        }
    }

    void SubscribeToNetClassifier() {
        Send(NNetClassifier::MakeNetClassifierID(), new NNetClassifier::TEvNetClassifier::TEvSubscribe);
    }

    void SubscribeToClusterTracker() {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "TClusterDiscoveryServiceActor: send TEvClusterTracker::TEvSubscribe");
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    }

    static TString FormatTs(const TMaybe<TInstant>& time) {
        if (time) {
            return time->ToRfc822String();
        }
        return "NULL";
    }

    bool IsHealthy(bool useLocalEnabled = false) const {
        bool isLocalEnabled = false;
        if (ClustersList) {
            for (const auto& cluster : ClustersList->Clusters) {
                if (cluster.IsLocal)
                    isLocalEnabled = cluster.IsEnabled;
            }

        }
        return ClustersList && DatacenterClassifier && (isLocalEnabled || !useLocalEnabled);
    }

    TString MakeReport() const {
        TStringBuilder message;
        message << "PersQueue Cluster Discovery Service info: \n";
        message << "\tOverall status: " << (IsHealthy() ? "GOOD" : "BAD") << "\n\n";

        message << "\tInitial NetClassifier response ts: " << FormatTs(InitialNetClassifierResponseTs) << "\n";
        message << "\tInitial ClusterTracker response ts: " << FormatTs(InitialClusterTrackerResponseTs)  << "\n";
        message << "\tFully initialized at: " << FormatTs(FullyInitializedTs) << "\n\n";

        message << "\tNetClassifier: " << (DatacenterClassifier ? "OK" : "NULL") << "\n";
        message << "\tNetClassifier NetData update ts: " << FormatTs(NetDataUpdateTs) << "\n\n";

        message << "\tClusters list: " << (ClustersList ? "OK" : "NULL") << "\n";
        message << "\tClusters list update ts: " << FormatTs(ClustersListUpdateTs) << "\n";
        if (ClustersList && ClustersList->Clusters.size()) {
            message << "\tClusters " << "(" << ClustersList->Clusters.size() << "): " << "\n";
            for (const auto& cluster : ClustersList->Clusters) {
                message << "\t\t" << cluster.DebugString() << "\n";
            }
        }

        message << "\n";
        message << "\tNetClassifier updates count: " << NetClassifierUpdatesCount << "\n";
        message << "\tClusters list updates count: " << ClustersListUpdatesCount << "\n";

        return TString(message);
    }

    void HandleHttpRequest(NMon::TEvHttpInfo::TPtr& ev) {
        const TStringBuf path = ev->Get()->Request.GetPath();

        TStringBuilder responseContent;
        if (path.EndsWith("/health")) {
            static const char HTTPNOTAVAIL_H[] = "HTTP/1.1 418 I'm a teapot\r\nConnection: Close\r\n\r\nDiscovery service is disabled on the node\r\n";
            responseContent << (IsHealthy() ? NMonitoring::HTTPOKTEXT : HTTPNOTAVAIL_H) << "Service statuses: 200 - OK, 418 - DISABLED";
        } else if (path.EndsWith("/ping")) {
            static const char HTTPNOTAVAIL_P[] = "HTTP/1.1 418 I'm a teapot\r\nConnection: Close\r\n\r\nDiscovery service is disabled on the node and local cluster is disabled\r\n";
            responseContent << (IsHealthy(true) ? NMonitoring::HTTPOKTEXT : HTTPNOTAVAIL_P) << "Service statuses: 200 - OK, 418 - DISABLED";
        } else{
            responseContent << NMonitoring::HTTPOKTEXT << MakeReport();
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(TString(responseContent), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    void UpdateNetClassifier(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev) {
        DatacenterClassifier = ev->Get()->Classifier;
        NetDataUpdateTs = ev->Get()->NetDataUpdateTimestamp;

        Counters = BuildCounters();

        ++NetClassifierUpdatesCount;

        UpdateHealthProbe();
    }

    void HandleClassifierUpdateWhileIniting(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev) {
        UpdateNetClassifier(ev);

        InitialNetClassifierResponseTs = Ctx().Now();

        CompleteInitialization();
    }

    void UpdateClustersList(NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev) {
        ClustersListUpdateTs = ev->Get()->ClustersListUpdateTimestamp;

        if (!ClustersList || !(*ClustersList == *ev->Get()->ClustersList)) {
            ClustersList = ev->Get()->ClustersList;
            Counters = BuildCounters();
        }

        ++ClustersListUpdatesCount;

        UpdateHealthProbe();
    }

    void HandleClustersUpdateWhileIniting(NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev) {
        UpdateClustersList(ev);

        InitialClusterTrackerResponseTs = Ctx().Now();

        CompleteInitialization();
    }

    void CompleteInitialization() {
        if (!InitialNetClassifierResponseTs || !InitialClusterTrackerResponseTs) {
            return;
        }

        FullyInitializedTs = Ctx().Now();
        Become(&TThis::Working);
    }

    void RegisterMonitoringPage() const {
        NActors::TMon* mon = AppData(Ctx())->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage * page = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(page, "pqcd", "PersQueue Cluster Discovery", false, Ctx().ExecutorThread.ActorSystem, Ctx().SelfID);
        }
    }

    STATEFN(Initing) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, HandleClassifierUpdateWhileIniting);
            hFunc(NClusterTracker::TEvClusterTracker::TEvClustersUpdate, HandleClustersUpdateWhileIniting);
            hFunc(NGRpcService::TEvDiscoverPQClustersRequest, HandleDiscoverPQClustersRequestWhileIniting);
            hFunc(NMon::TEvHttpInfo, HandleHttpRequest);
            hFunc(TEvents::TEvWakeup, UpdateTimedCounters);
        }
    }

    void RespondServiceUnavailable(NGRpcService::TEvDiscoverPQClustersRequest::TPtr& ev) {
        Counters->DroppedRequestsCount->Inc();

        ev->Get()->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
    }

    void HandleDiscoverPQClustersRequestWhileIniting(NGRpcService::TEvDiscoverPQClustersRequest::TPtr& ev) {
        Counters->TotalRequestsCount->Inc();

        RespondServiceUnavailable(ev);
    }

    void HandleClassifierUpdateWhileWorking(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev) {
        UpdateNetClassifier(ev);
    }

    void HandleClustersUpdateWhileWorking(NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev) {
        UpdateClustersList(ev);
    }

    void HandleDiscoverPQClustersRequestWhileWorking(NGRpcService::TEvDiscoverPQClustersRequest::TPtr& ev) {
        Counters->TotalRequestsCount->Inc();

        if (!IsHealthy()) {
            RespondServiceUnavailable(ev);
            return;
        }

        IActor* actor = NWorker::CreateClusterDiscoveryWorker(ev, DatacenterClassifier, CloudNetworksClassifier, ClustersList, Counters);
        Register(actor, TMailboxType::HTSwap, AppData(Ctx())->UserPoolId);
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, HandleClassifierUpdateWhileWorking);
            hFunc(NClusterTracker::TEvClusterTracker::TEvClustersUpdate, HandleClustersUpdateWhileWorking);
            hFunc(NGRpcService::TEvDiscoverPQClustersRequest, HandleDiscoverPQClustersRequestWhileWorking);
            hFunc(NMon::TEvHttpInfo, HandleHttpRequest);
            hFunc(TEvents::TEvWakeup, UpdateTimedCounters);
        }
    }

    // Monitoring features

    void StartPeriodicalMonitoring() {
        Y_ABORT_UNLESS(Cfg().GetTimedCountersUpdateIntervalSeconds());

        Send(Ctx().SelfID, new TEvents::TEvWakeup);
    }

    void UpdateHealthProbe() {
        (*Counters->HealthProbe) = IsHealthy();
    }

    void UpdateTimedCounters(TEvents::TEvWakeup::TPtr&) {
        if (NetDataUpdateTs) {
            (*Counters->NetDataUpdateLagSeconds) = Ctx().Now().Seconds() - NetDataUpdateTs->Seconds();
        }

        if (ClustersListUpdateTs) {
            (*Counters->ClustersListUpdateLagSeconds) = Ctx().Now().Seconds() - ClustersListUpdateTs->Seconds();
        }

        UpdateHealthProbe();

        Schedule(TDuration::Seconds(Cfg().GetTimedCountersUpdateIntervalSeconds()), new TEvents::TEvWakeup);
    }

    TClusterDiscoveryCounters::TPtr BuildCounters() {
        return MakeIntrusive<TClusterDiscoveryCounters>(RawCounters, ClustersList, DatacenterClassifier);
    }

private:
    TActorId NetClassifierActorId;
    TActorId ClusterTrackerActorId;
    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier;
    NAddressClassifier::TLabeledAddressClassifier::TConstPtr CloudNetworksClassifier;
    TMaybe<TInstant> NetDataUpdateTs;

    NClusterTracker::TClustersList::TConstPtr ClustersList;
    TMaybe<TInstant> ClustersListUpdateTs;

    TMaybe<TInstant> InitialNetClassifierResponseTs;
    TMaybe<TInstant> InitialClusterTrackerResponseTs;

    TMaybe<TInstant> FullyInitializedTs;

    size_t NetClassifierUpdatesCount = 0;
    size_t ClustersListUpdatesCount = 0;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> RawCounters;
    TClusterDiscoveryCounters::TPtr Counters;
};

NActors::IActor* CreateClusterDiscoveryService(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TClusterDiscoveryServiceActor(counters);
}

} // namespace NKikimr::NPQ::NClusterDiscovery
