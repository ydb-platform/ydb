#pragma once
#include "gc.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc_actor.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <library/cpp/retry/retry_policy.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGarbageCollectionActor: public TSharedBlobsCollectionActor<TGarbageCollectionActor> {
private:
    using TBase = TSharedBlobsCollectionActor<TGarbageCollectionActor>;
    const NActors::TActorId TabletActorId;
    std::shared_ptr<TGCTask> GCTask;

    THashMap<TString, ui64> RetryCountersByKey;
    THashSet<TLogoBlobID> BlobIdsToRemove;
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectResponse::TPtr& ev);
    void CheckFinished();

    virtual void DoOnSharedRemovingFinished() override {
        CheckFinished();
    }
    void StartDeletingObject(const TString& key) const {
        auto awsRequest = Aws::S3::Model::DeleteObjectRequest().WithKey(key);
        auto request = std::make_unique<NWrappers::NExternalStorage::TEvDeleteObjectRequest>(awsRequest);
        auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
        TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>> evPtr(
            (TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>*)hRequest.release());
        GCTask->GetExternalStorageOperator()->Execute(evPtr);
    }
    void Abort() const {
        Send(TabletActorId, new NActors::TEvents::TEvPoison());
    }

public:
    TGarbageCollectionActor(const std::shared_ptr<TGCTask>& task, const NActors::TActorId& tabletActorId, const TTabletId selfTabletId)
        : TBase(task->GetStorageId(), selfTabletId, task->GetBlobsToRemove().GetBorrowed(), task)
        , TabletActorId(tabletActorId)
        , GCTask(task)
    {

    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectResponse, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void Bootstrap(const TActorContext& ctx);
};

}
