#include "read.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TReadingAction::DoStartReading(THashSet<TBlobRange>&& ranges) {
    for (auto&& r : ranges) {
        DoRetryRead(r);
    }
}

void TReadingAction::DoRetryRead(const TBlobRange& range) {
    auto awsRequest = Aws::S3::Model::GetObjectRequest()
                          .WithKey(range.BlobId.GetLogoBlobId().ToString())
                          .WithRange(TStringBuilder() << "bytes=" << range.Offset << "-" << range.Offset + range.Size - 1);
    auto request = std::make_unique<NWrappers::NExternalStorage::TEvGetObjectRequest>(awsRequest);
    auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
    TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>> evPtr(
        (TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>*)hRequest.release());
    ExternalStorageOperator->Execute(evPtr);
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
