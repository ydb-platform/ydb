#include "grpc_pq_clusters_updater_actor.h"

namespace NKikimr {
namespace NGRpcProxy {

TClustersUpdater::TClustersUpdater(IPQClustersUpdaterCallback* callback, TStatus::TPtr& status)
    : Callback(callback)
    , Status(status)
    {};

void TClustersUpdater::Bootstrap(const NActors::TActorContext& ctx) {
    ctx.Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    ctx.Send(NNetClassifier::MakeNetClassifierID(), new NNetClassifier::TEvNetClassifier::TEvSubscribe);

    Become(&TThis::StateFunc);
}

void TClustersUpdater::Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx) {
    TGuard<TSpinLock> guard(Status->Lock);
    if (!Status->Running) {
        return Die(ctx);
    }

    Callback->NetClassifierUpdated(ev->Get()->Classifier);
}

void TClustersUpdater::Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx) {
    TGuard<TSpinLock> guard(Status->Lock);
    if (!Status->Running) {
        return Die(ctx);
    }

    const auto& list = ev->Get()->ClustersList;
    if (!list) {
        return;
    }

    for (const auto& cluster : list->Clusters) {
        if (cluster.IsLocal) {
            const bool changed = LocalCluster != cluster.Name || Enabled != cluster.IsEnabled;
            if (changed) {
                LocalCluster = cluster.Name;
                Enabled = cluster.IsEnabled;
                Callback->CheckClusterChange(LocalCluster, Enabled);
            }
        }
    }
    Callback->ClustersListUpdated(list);
}

}
}
