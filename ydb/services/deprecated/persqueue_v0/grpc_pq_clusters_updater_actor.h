#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/mind/address_classification/net_classifier.h>

#include <ydb/library/services/services.pb.h>

namespace NKikimr {
namespace NGRpcProxy {

struct TEvPQClustersUpdater {
    enum EEv {
        EvUpdateClusters = EventSpaceBegin(TKikimrEvents::ES_PQ_CLUSTERS_UPDATER),
        EvEnd,
    };

    struct TEvUpdateClusters : public NActors::TEventLocal<TEvUpdateClusters, EvUpdateClusters> {
        TEvUpdateClusters()
        {}
    };
};

class IPQClustersUpdaterCallback {
public:
    virtual ~IPQClustersUpdaterCallback() = default;
    virtual void CheckClusterChange(const TString& localCluster, const bool enabled)
    {
        Y_UNUSED(localCluster);
        Y_UNUSED(enabled);
    }

    virtual void CheckClustersListChange(const TVector<TString>& clusters)
    {
        Y_UNUSED(clusters);
    }

    virtual void NetClassifierUpdated(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier) {
        Y_UNUSED(classifier);
    }
};

class TClustersUpdater : public NActors::TActorBootstrapped<TClustersUpdater> {
public:
    struct TStatus {
        using TPtr = std::shared_ptr<TStatus>;

        bool Running = true;
        TSpinLock Lock;

        void Stop() {
            TGuard guard(Lock);
            Running = false;
        }
    };

    TClustersUpdater(IPQClustersUpdaterCallback* callback, TStatus::TPtr& status);

    void Bootstrap(const NActors::TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_WRITE; } // FIXME

private:
    IPQClustersUpdaterCallback* Callback;
    TString LocalCluster;
    TVector<TString> Clusters;
    bool Enabled = false;
    TStatus::TPtr Status;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQClustersUpdater::TEvUpdateClusters, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);
        }
    }

    void Handle(TEvPQClustersUpdater::TEvUpdateClusters::TPtr &ev, const TActorContext &ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

};

}
}
