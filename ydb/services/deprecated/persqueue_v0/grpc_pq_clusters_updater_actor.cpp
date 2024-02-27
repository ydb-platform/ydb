#include "grpc_pq_clusters_updater_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

namespace NKikimr {
namespace NGRpcProxy {

static const int CLUSTERS_UPDATER_TIMEOUT_ON_ERROR = 1;


TClustersUpdater::TClustersUpdater(IPQClustersUpdaterCallback* callback)
    : Callback(callback)
    {};

void TClustersUpdater::Bootstrap(const NActors::TActorContext& ctx) {
    ctx.Send(ctx.SelfID, new TEvPQClustersUpdater::TEvUpdateClusters());
    ctx.Send(NNetClassifier::MakeNetClassifierID(), new NNetClassifier::TEvNetClassifier::TEvSubscribe);

    Become(&TThis::StateFunc);
}

void TClustersUpdater::Handle(TEvPQClustersUpdater::TEvUpdateClusters::TPtr&, const TActorContext &ctx) {
    auto req = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    req->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    req->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    req->Record.MutableRequest()->SetKeepSession(false);
    req->Record.MutableRequest()->SetQuery("--!syntax_v1\nSELECT `name`, `local`, `enabled` FROM `" + AppData(ctx)->PQConfig.GetRoot() + "/Config/V2/Cluster`;");
    req->Record.MutableRequest()->SetDatabase(NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig));
    req->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    req->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), req.Release());
}

void TClustersUpdater::Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext&) {

    Callback->NetClassifierUpdated(ev->Get()->Classifier);
}




void TClustersUpdater::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
        auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);
        bool local = false;
        TVector<TString> clusters;
        for (size_t i = 0; i < t.ListSize(); ++i) {
            TString dc = t.GetList(i).GetStruct(0).GetOptional().GetText();
            local = t.GetList(i).GetStruct(1).GetOptional().GetBool();
            clusters.push_back(dc);
            if (local) {
                bool enabled = t.GetList(i).GetStruct(2).GetOptional().GetBool();
                Y_ABORT_UNLESS(LocalCluster.empty() || LocalCluster == dc);
                bool changed = LocalCluster != dc || Enabled != enabled;
                if (changed) {
                    LocalCluster = dc;
                    Enabled = enabled;
                    Callback->CheckClusterChange(LocalCluster, Enabled);
                }
            }
        }
        if (Clusters != clusters) {
            Clusters = clusters;
            Callback->CheckClustersListChange(Clusters);
        }
        ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetClustersUpdateTimeoutSec()), new TEvPQClustersUpdater::TEvUpdateClusters());
    } else {
        LOG_ERROR_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "can't update clusters " << record);
        ctx.Schedule(TDuration::Seconds(CLUSTERS_UPDATER_TIMEOUT_ON_ERROR), new TEvPQClustersUpdater::TEvUpdateClusters());
    }
}


}
}
