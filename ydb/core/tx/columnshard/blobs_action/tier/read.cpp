#include "read.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TReadingAction::DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& ranges) {
    for (auto&& i : ranges) {
        for (auto&& r : i.second) {
            auto awsRequest = Aws::S3::Model::GetObjectRequest()
                .WithKey(i.first.GetLogoBlobId().ToString())
                .WithRange(TStringBuilder() << "bytes=" << r.Offset << "-" << r.Offset + r.Size - 1);
            auto request = std::make_unique<NWrappers::NExternalStorage::TEvGetObjectRequest>(awsRequest);
            auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
            TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>> evPtr((TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>*)hRequest.release());
            ExternalStorageOperator->Execute(evPtr);
        }
    }
}

}
