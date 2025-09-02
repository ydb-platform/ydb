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
        } else {
            auto delay = NextRetryDelay(error, key).value_or(TDuration::Seconds(30));
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("actor", "TGarbageCollectionActor")("event", "error")(
                "exception", error.GetExceptionName())("message", error.GetMessage())("key", key);
            Schedule(delay, new NWrappers::NExternalStorage::TEvDeleteObjectRequest(Aws::S3::Model::DeleteObjectRequest().WithKey(key)));
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

void TGarbageCollectionActor::Handle(NWrappers::NExternalStorage::TEvDeleteObjectRequest::TPtr& ev) {
    AFL_VERIFY(ev->Get()->Request.KeyHasBeenSet());
    StartDeletingObject(TString(ev->Get()->Request.GetKey()));
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

void TGarbageCollectionActor::StartDeletingObject(const TString& key) const {
    auto awsRequest = Aws::S3::Model::DeleteObjectRequest().WithKey(key);
    auto request = std::make_unique<NWrappers::NExternalStorage::TEvDeleteObjectRequest>(awsRequest);
    auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
    TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>> evPtr(
        (TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>*)hRequest.release());
    GCTask->GetExternalStorageOperator()->Execute(evPtr);
}

std::optional<TDuration> TGarbageCollectionActor::NextRetryDelay(const Aws::S3::S3Error& reason, const TString& key) {
    if (!reason.ShouldRetry()) {
        return std::nullopt;
    }
    auto* findState = RetryStateByKey.FindPtr(key);
    if (!findState) {
        findState = &RetryStateByKey.emplace(key, RetryPolicy->CreateRetryState()).first->second;
    }
    if (auto delay = (*findState)->GetNextRetryDelay()) {
        return *delay;
    }
    return std::nullopt;
}

}
