#include "cluster_tracker.h"
#include "pq_database.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>

#include <tuple>
#include <vector>

namespace NKikimr::NPQ::NClusterTracker {

inline auto& Ctx() {
    return TActivationContext::AsActorContext();
}

bool TClustersList::TCluster::operator==(const TCluster& rhs) const {
    return std::tie(Name, Datacenter, Balancer, IsLocal, IsEnabled, Weight) ==
           std::tie(rhs.Name, rhs.Datacenter, rhs.Balancer, rhs.IsLocal, rhs.IsEnabled, rhs.Weight);
}

bool TClustersList::operator==(const TClustersList& rhs) const {
    return Clusters == rhs.Clusters && Version == rhs.Version;
}

class TClusterTracker: public TActorBootstrapped<TClusterTracker> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_CLUSTER_TRACKER;
    }

    const auto& Cfg() const {
        return AppData(Ctx())->PQConfig;
    }

    void Bootstrap() {
        ListClustersQuery = MakeListClustersQuery();

        Become(&TThis::WaitingForSubscribers);
    }

private:
    void AddSubscriber(const TActorId subscriberId) {
        Subscribers.insert(subscriberId);
    }

    STATEFN(WaitingForSubscribers) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvClusterTracker::TEvSubscribe, HandleWhileWaiting);
        }
    }

    void HandleWhileWaiting(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        AddSubscriber(ev->Sender);

        Become(&TThis::Working);

        Send(Ctx().SelfID, new TEvents::TEvWakeup);
    }

    const TString& GetDatabase() {
        if (Database.empty()) {
            Database = GetDatabaseFromConfig(Cfg());
        }

        return Database;
    }

    TString MakeListClustersQuery() const {
        return Sprintf(
            R"(
               --!syntax_v1
               SELECT C.name, C.balancer, C.local, C.enabled, C.weight, V.version FROM `%s` AS C
               CROSS JOIN
               (SELECT version FROM `%s` WHERE name == 'Cluster') AS V;
            )", Cfg().GetClusterTablePath().c_str(), Cfg().GetVersionTablePath().c_str());
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvClusterTracker::TEvSubscribe, HandleWhileWorking);
            hFunc(TEvents::TEvWakeup, HandleWhileWorking);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleWhileWorking);
            hFunc(NKqp::TEvKqp::TEvProcessResponse, HandleWhileWorking);
        }
    }

    void SendClustersList(TActorId subscriberId) {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PQ_METACACHE, "SendClustersList");

        auto ev = MakeHolder<TEvClusterTracker::TEvClustersUpdate>();

        ev->ClustersList = ClustersList;
        ev->ClustersListUpdateTimestamp = ClustersListUpdateTimestamp;

        Send(subscriberId, ev.Release());
    }

    void HandleWhileWorking(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        AddSubscriber(ev->Sender);

        // List may be null due to reinit
        if (ClustersList) {
            SendClustersList(ev->Sender);
        }
    }

    void BroadcastClustersUpdate() {
        for (const auto& subscriberId : Subscribers) {
            SendClustersList(subscriberId);
        }
    }

    void HandleWhileWorking(TEvents::TEvWakeup::TPtr&) {
        auto req = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        req->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        req->Record.MutableRequest()->SetKeepSession(false);
        req->Record.MutableRequest()->SetQuery(MakeListClustersQuery());
        req->Record.MutableRequest()->SetDatabase(GetDatabase());
        // useless without explicit session
        // req->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        req->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        req->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

        Send(NKqp::MakeKqpProxyID(Ctx().SelfID.NodeId()), req.Release());
    }

    void HandleWhileWorking(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetRef();
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS && record.GetResponse().GetResults(0).GetValue().GetStruct(0).ListSize()) {
            UpdateClustersList(record);

            Y_VERIFY(ClustersList);
            Y_VERIFY(ClustersList->Clusters.size());
            Y_VERIFY(ClustersListUpdateTimestamp && *ClustersListUpdateTimestamp);

            BroadcastClustersUpdate();

            Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutSec()), new TEvents::TEvWakeup);
        } else {
            LOG_ERROR_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "failed to list clusters: " << record);

            ClustersList = nullptr;

            Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutOnErrorSec()), new TEvents::TEvWakeup);
        }
    }

    void HandleWhileWorking(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        LOG_ERROR_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "failed to list clusters: " << record);

        ClustersList = nullptr;

        Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutOnErrorSec()), new TEvents::TEvWakeup);
    }

    template<typename TProtoRecord>
    void UpdateClustersList(const TProtoRecord& record) {
        auto clustersList = MakeIntrusive<TClustersList>();
        auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);
        clustersList->Clusters.resize(t.ListSize());

        for (size_t i = 0; i < t.ListSize(); ++i) {
            auto& cluster = clustersList->Clusters[i];

            cluster.Name = t.GetList(i).GetStruct(0).GetOptional().GetText();
            cluster.Datacenter = cluster.Name;
            cluster.Balancer = t.GetList(i).GetStruct(1).GetOptional().GetText();

            cluster.IsLocal = t.GetList(i).GetStruct(2).GetOptional().GetBool();
            cluster.IsEnabled = t.GetList(i).GetStruct(3).GetOptional().GetBool();
            cluster.Weight = t.GetList(i).GetStruct(4).GetOptional().GetUint64();
        }

        clustersList->Version = t.GetList(0).GetStruct(5).GetOptional().GetInt64();

        ClustersList = std::move(clustersList);
        ClustersListUpdateTimestamp = Ctx().Now();
    }

private:
    TString ListClustersQuery;
    TString PreparedQueryId;
    TClustersList::TConstPtr ClustersList = nullptr;
    TMaybe<TInstant> ClustersListUpdateTimestamp;
    THashSet<TActorId> Subscribers;
    TString Database;
};

NActors::IActor* CreateClusterTracker() {
    return new TClusterTracker();
}

} // namespace NKikimr::NPQ::NClusterTracker
