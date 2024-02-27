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
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> Requests;
    std::shared_ptr<TGCTask> GCTask;
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);
    void CheckFinished();

    virtual void DoOnSharedRemovingFinished() override {
        CheckFinished();
    }
public:
    TGarbageCollectionActor(const std::shared_ptr<TGCTask>& task, THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>>&& requests, const NActors::TActorId& tabletActorId, const TTabletId selfTabletId)
        : TBase(task->GetStorageId(), selfTabletId, task->GetBlobsToRemove().GetBorrowed())
        , TabletActorId(tabletActorId)
        , Requests(std::move(requests))
        , GCTask(task)
    {

    }

    STFUNC(StateWork) {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("action_id", GCTask->GetActionGuid());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actor", "TGarbageCollectionActor")("event", "starting")("action_id", GCTask->GetActionGuid());
        for (auto&& i : Requests) {
            SendToBSProxy(ctx, i.first, i.second.release());
        }
        TBase::Bootstrap(ctx);
        Become(&TGarbageCollectionActor::StateWork);
    }
};

}
