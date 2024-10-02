#include "cluster_tracker.h"
#include "pq_database.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>

#include <tuple>
#include <vector>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

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
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "Subscribers.size: " << Subscribers.size() << " AddSubscriber");

        Subscribers.insert(subscriberId);
    }

    STATEFN(WaitingForSubscribers) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvClusterTracker::TEvSubscribe, HandleWhileWaiting);
        }
    }

    void HandleWhileWaiting(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "AddSubscriber TEvSubscriber");

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
        }
    }

    void SendClustersList(TActorId subscriberId) {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "SendClustersList");

        auto ev = MakeHolder<TEvClusterTracker::TEvClustersUpdate>();

        ev->ClustersList = ClustersList;
        ev->ClustersListUpdateTimestamp = ClustersListUpdateTimestamp;

        Send(subscriberId, ev.Release());
    }

    void HandleWhileWorking(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "HandleWhileWorking TEvSubscribe Subscribers.size: " << Subscribers.size() << " ClustersList: " << (ClustersList == nullptr ? "null" : std::to_string(ClustersList->Clusters.size())));

        AddSubscriber(ev->Sender);

        // List may be null due to reinit
        if (ClustersList) {
            SendClustersList(ev->Sender);
        }
    }

    void BroadcastClustersUpdate() {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "BroadcastClustersUpdate Subscribers.size: " << Subscribers.size());

        for (const auto& subscriberId : Subscribers) {
            LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "BroadcastClustersUpdate subscriberId: " << subscriberId);
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
        req->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        // useless without explicit session
        // req->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        req->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        req->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

        Send(NKqp::MakeKqpProxyID(Ctx().SelfID.NodeId()), req.Release());
    }

    void HandleWhileWorking(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "HandleWhileWorking TEvQueryResponse");

        const auto& record = ev->Get()->Record.GetRef();
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
            if (parser.RowsCount()) {
                LOG_DEBUG_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "HandleWhileWorking TEvQueryResponse UpdateClustersList");
                UpdateClustersList(parser);

                Y_ABORT_UNLESS(ClustersList);
                Y_ABORT_UNLESS(ClustersList->Clusters.size());
                Y_ABORT_UNLESS(ClustersListUpdateTimestamp && *ClustersListUpdateTimestamp);

                BroadcastClustersUpdate();

                Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutSec()), new TEvents::TEvWakeup);
                return;
            }
        }

        LOG_ERROR_S(Ctx(), NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "failed to list clusters: " << record);

        ClustersList = nullptr;
        Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutOnErrorSec()), new TEvents::TEvWakeup);
    }

    template<typename TProtoRecord>
    void UpdateClustersList(TProtoRecord& parser) {
        auto clustersList = MakeIntrusive<TClustersList>();
        clustersList->Clusters.resize(parser.RowsCount());

        bool firstRow = parser.TryNextRow();
        YQL_ENSURE(firstRow);
        clustersList->Version = *parser.ColumnParser(5).GetOptionalInt64();
        size_t i = 0;

        do {
            auto& cluster = clustersList->Clusters[i];

            cluster.Name = *parser.ColumnParser(0).GetOptionalUtf8();
            cluster.Datacenter = cluster.Name;
            cluster.Balancer = *parser.ColumnParser(1).GetOptionalUtf8();

            cluster.IsLocal = *parser.ColumnParser(2).GetOptionalBool();
            cluster.IsEnabled = *parser.ColumnParser(3).GetOptionalBool();
            cluster.Weight = *parser.ColumnParser(4).GetOptionalUint64();

            ++i;
        } while (parser.TryNextRow());

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
