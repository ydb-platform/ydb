#pragma once
#include "gc.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc_actor.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TGarbageCollectionActor: public TSharedBlobsCollectionActor<TGarbageCollectionActor> {
private:
    using TBase = TSharedBlobsCollectionActor<TGarbageCollectionActor>;
    const NActors::TActorId TabletActorId;
    std::shared_ptr<TGCTask> GCTask;

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);
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
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS,
            {"actionId", GCTask->GetActionGuid()},
            {"tabletId", GCTask->GetTabletId()});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS, "Dump actor, event, actionId",
            {"actor", "TGarbageCollectionActor"},
            {"event", "starting"},
            {"actionId", GCTask->GetActionGuid()});
        for (auto&& i : GCTask->GetListsByGroupId()) {
            auto request = GCTask->BuildRequest(i.first);
            AFL_VERIFY(request);   // Cannot fail on the first time
            SendToBSProxy(ctx, i.first.GetGroupId(), request.release(), i.first.GetGroupId());
        }
        TBase::Bootstrap(ctx);
        Become(&TGarbageCollectionActor::StateWork);
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
