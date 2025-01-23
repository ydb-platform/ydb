#pragma once
#include "gc.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/gc_actor.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TGarbageCollectionActor: public TSharedBlobsCollectionActor<TGarbageCollectionActor> {
private:
    using TBase = TSharedBlobsCollectionActor<TGarbageCollectionActor>;

    const NActors::TActorId TabletActorId;
    std::shared_ptr<TGCTask> GCTask;
    size_t PendingGroupReplies = 0;

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);
    void HandleOnDying(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);
    void CheckReadyToDie();
    void CheckFinished();

    virtual void DoOnSharedRemovingFinished() override {
        CheckFinished();
    }
public:
    TGarbageCollectionActor(const std::shared_ptr<TGCTask>& task, const NActors::TActorId& tabletActorId, const TTabletId selfTabletId)
        : TBase(task->GetStorageId(), selfTabletId, task->GetBlobsToRemove().GetBorrowed(), task)
        , TabletActorId(tabletActorId)
        , GCTask(task)
    {

    }

    STFUNC(StateWork) {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)
            ("action_id", GCTask->GetActionGuid())("tablet_id", GCTask->GetTabletId());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    STFUNC(StateDying) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleOnDying);
            default:
                return TBase::StateWork(ev);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("actor", "TGarbageCollectionActor")("event", "starting")("action_id", GCTask->GetActionGuid());
        for (auto&& i : GCTask->GetListsByGroupId()) {
            auto request = GCTask->BuildRequest(i.first);
            if (!request) {
                Become(&TGarbageCollectionActor::StateDying);
                CheckReadyToDie();
                return;
            }
            SendToBSProxy(ctx, i.first.GetGroupId(), request.release(), i.first.GetGroupId());
            PendingGroupReplies++;
        }
        TBase::Bootstrap(ctx);
        Become(&TGarbageCollectionActor::StateWork);
    }
};

}
