#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TGarbageCollectionActor::Handle(NWrappers::NExternalStorage::TEvDeleteObjectResponse::TPtr& ev) {
    TLogoBlobID logoBlobId;
    TString errorMessage;
    Y_ABORT_UNLESS(ev->Get()->Key);
    AFL_VERIFY(TLogoBlobID::Parse(logoBlobId, *ev->Get()->Key, errorMessage))("error", errorMessage);
    BlobIdsToRemove.erase(logoBlobId);
    CheckFinished();
}

void TGarbageCollectionActor::Bootstrap(const TActorContext& ctx) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actor", "TGarbageCollectionActor")("event", "starting");
    for (auto&& i : GCTask->GetDraftBlobIds()) {
        BlobIdsToRemove.emplace(i.GetLogoBlobId());
    }
    for (auto i = GCTask->GetBlobsToRemove().GetDirect().GetIterator(); i.IsValid(); ++i) {
        BlobIdsToRemove.emplace(i.GetBlobId().GetLogoBlobId());
    }
    for (auto&& i : BlobIdsToRemove) {
        auto awsRequest = Aws::S3::Model::DeleteObjectRequest().WithKey(i.ToString());
        auto request = std::make_unique<NWrappers::NExternalStorage::TEvDeleteObjectRequest>(awsRequest);
        auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
        TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>> evPtr((TEventHandle<NWrappers::NExternalStorage::TEvDeleteObjectRequest>*)hRequest.release());
        GCTask->GetExternalStorageOperator()->Execute(evPtr);
    }
    TBase::Bootstrap(ctx);
    Become(&TGarbageCollectionActor::StateWork);
}

void TGarbageCollectionActor::CheckFinished() {
    if (SharedRemovingFinished && BlobIdsToRemove.empty()) {
        auto g = PassAwayGuard();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actor", "TGarbageCollectionActor")("event", "finished");
        TActorContext::AsActorContext().Send(TabletActorId, std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(GCTask));
    }
}

}
