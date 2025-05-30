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
    using IRetryPolicy = IRetryPolicy<>;

    const NActors::TActorId TabletActorId;
    std::shared_ptr<TGCTask> GCTask;

    IRetryPolicy::TPtr RetryPolicy = IRetryPolicy::GetExponentialBackoffPolicy([]() {
        return ERetryErrorClass::ShortRetry;
    });
    THashMap<TString, IRetryPolicy::IRetryState::TPtr> RetryStateByKey;

    THashSet<TLogoBlobID> BlobIdsToRemove;
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectResponse::TPtr& ev);
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectRequest::TPtr& ev);
    void CheckFinished();
    void StartDeletingObject(const TString& key) const;
    std::optional<TDuration> NextRetryDelay(const Aws::S3::S3Error& reason, const TString& key);

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
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectResponse, Handle);
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectRequest, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void Bootstrap(const TActorContext& ctx);
};

}
