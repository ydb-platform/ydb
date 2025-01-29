#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TGarbageCollectionActor::Handle(NWrappers::NExternalStorage::TEvDeleteObjectResponse::TPtr& ev) {
    AFL_VERIFY(ev->Get()->Key);
    const TString& key = *ev->Get()->Key;

    if (!ev->Get()->IsSuccess()) {
        const auto& error = ev->Get()->GetError();

        bool isRemoved = false;
        switch (error.GetErrorType()) {
            case Aws::S3::S3Errors::NO_SUCH_BUCKET:
            case Aws::S3::S3Errors::NO_SUCH_KEY:
                isRemoved = true;
                break;
            default:
                break;
        }

        if (isRemoved) {
            // Do nothing
        } else if (error.ShouldRetry()) {
            // TODO: add backoff, don't pass away immediately ever
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "retriable_error")(
                "exception", error.GetExceptionName())("message", error.GetMessage())("key", key);
            {
                ui64& retryCounter = RetryCountersByKey.emplace(key, 0).first->second;
                if (++retryCounter > 10) {
                    AFL_CRIT(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "exceeded_retry_limit")(
                        "exception", error.GetExceptionName())("message", error.GetMessage())("key", key);
                    Abort();
                    PassAway();
                    return;
                }
            }
            StartDeletingObject(*ev->Get()->Key);
            return;
        } else {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "error")(
                "exception", error.GetExceptionName())("message", error.GetMessage())("key", key);
            Abort();
            PassAway();
            return;
        }
    }

    TLogoBlobID logoBlobId;
    TString errorMessage;
    Y_ABORT_UNLESS(ev->Get()->Key);
    AFL_VERIFY(TLogoBlobID::Parse(logoBlobId, *ev->Get()->Key, errorMessage))("error", errorMessage);
    BlobIdsToRemove.erase(logoBlobId);
    CheckFinished();
}

void TGarbageCollectionActor::Bootstrap(const TActorContext& ctx) {
    for (auto i = GCTask->GetBlobsToRemove().GetDirect().GetIterator(); i.IsValid(); ++i) {
        BlobIdsToRemove.emplace(i.GetBlobId().GetLogoBlobId());
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "starting")("storage_id", GCTask->GetStorageId())("drafts", GCTask->GetDraftBlobIds().size())("to_delete", BlobIdsToRemove.size());
    for (auto&& i : GCTask->GetDraftBlobIds()) {
        BlobIdsToRemove.emplace(i.GetLogoBlobId());
    }
    for (auto&& i : BlobIdsToRemove) {
        StartDeletingObject(i.ToString());
    }
    TBase::Bootstrap(ctx);
    Become(&TGarbageCollectionActor::StateWork);
}

void TGarbageCollectionActor::CheckFinished() {
    if (SharedRemovingFinished && BlobIdsToRemove.empty()) {
        auto g = PassAwayGuard();
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "finished");
        TActorContext::AsActorContext().Send(TabletActorId, std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(GCTask));
    }
}

}
