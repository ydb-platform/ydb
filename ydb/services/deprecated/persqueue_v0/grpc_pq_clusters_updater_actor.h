#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>

#include <ydb/library/services/services.pb.h>

namespace NKikimr {
namespace NGRpcProxy {

class IPQClustersUpdaterCallback {
public:
    virtual ~IPQClustersUpdaterCallback() = default;
    virtual void CheckClusterChange(const TString& localCluster, const bool enabled)
    {
        Y_UNUSED(localCluster);
        Y_UNUSED(enabled);
    }

    virtual void ClustersListUpdated(NPQ::NClusterTracker::TClustersList::TConstPtr list)
    {
        Y_UNUSED(list);
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
    bool Enabled = false;
    TStatus::TPtr Status;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
            HFunc(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate, Handle);
        }
    }

    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(NNetClassifier::TEvNetClassifier::TEvClassifierUpdate::TPtr& ev, const TActorContext& ctx);

};

}
}
