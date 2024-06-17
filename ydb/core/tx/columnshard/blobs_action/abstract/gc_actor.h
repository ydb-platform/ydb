#pragma once
#include "blob_set.h"
#include "common.h"
#include "gc.h"

#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/blobs_action/events/delete_blobs.h>

namespace NKikimr::NOlap::NBlobOperations {

template <class TDerived>
class TSharedBlobsCollectionActor: public TActorBootstrapped<TDerived> {
private:
    using TBase = TActorBootstrapped<TDerived>;
    const TString OperatorId;
    TBlobsByTablet BlobIdsByTablets;
    const TTabletId SelfTabletId;
    std::shared_ptr<IBlobsGCAction> GCAction;
    virtual void DoOnSharedRemovingFinished() = 0;
    void OnSharedRemovingFinished() {
        SharedRemovingFinished = true;
        DoOnSharedRemovingFinished();
    }
    void Handle(NEvents::TEvDeleteSharedBlobsFinished::TPtr& ev) {
        const TTabletId sourceTabletId = (TTabletId)ev->Get()->Record.GetTabletId();
        auto* blobIds = BlobIdsByTablets.Find(sourceTabletId);
        AFL_VERIFY(blobIds);
        switch (ev->Get()->Record.GetStatus()) {
            case NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::Success:
                AFL_VERIFY(BlobIdsByTablets.Remove(sourceTabletId));
                break;
            case NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::DestinationCurrenlyLocked:
                for (auto&& i : *blobIds) {
                    GCAction->AddSharedBlobToNextIteration(i, sourceTabletId);
                }
                AFL_VERIFY(BlobIdsByTablets.Remove(sourceTabletId));
                break;
            case NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::NeedRetry:
                SendToTablet(*blobIds, sourceTabletId);
                break;
        }
        if (BlobIdsByTablets.IsEmpty()) {
            AFL_VERIFY(!SharedRemovingFinished);
            OnSharedRemovingFinished();
        }
    }
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        auto* blobIds = BlobIdsByTablets.Find((TTabletId)ev->Cookie);
        AFL_VERIFY(blobIds);
        SendToTablet(*blobIds, (TTabletId)ev->Cookie);
    }

    void SendToTablet(const THashSet<TUnifiedBlobId>& blobIds, const TTabletId tabletId) const {
        auto ev = std::make_unique<NEvents::TEvDeleteSharedBlobs>(TBase::SelfId(), (ui64)SelfTabletId, OperatorId, blobIds);
        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), (ui64)tabletId, true), IEventHandle::FlagTrackDelivery, (ui64)tabletId);
    }
protected:
    bool SharedRemovingFinished = false;
public:
    TSharedBlobsCollectionActor(const TString& operatorId, const TTabletId selfTabletId, const TBlobsByTablet& blobIds, const std::shared_ptr<IBlobsGCAction>& gcAction)
        : OperatorId(operatorId)
        , BlobIdsByTablets(blobIds)
        , SelfTabletId(selfTabletId)
        , GCAction(gcAction)
    {
        AFL_VERIFY(GCAction);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TEvDeleteSharedBlobsFinished, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                AFL_VERIFY(false)("problem", "unexpected event")("event_type", ev->GetTypeName());
        }
    }

    void Bootstrap(const TActorContext& /*ctx*/) {
        if (BlobIdsByTablets.IsEmpty()) {
            OnSharedRemovingFinished();
        } else {
            for (auto&& i : BlobIdsByTablets) {
                SendToTablet(i.second, i.first);
            }
        }
    }

};

}
