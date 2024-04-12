#include "write.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/wrappers/events/common.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/PutObjectRequest.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TWriteAction::DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    auto awsRequest = Aws::S3::Model::PutObjectRequest().WithKey(blobId.GetLogoBlobId().ToString());

    TString moveData = data;
    auto request = std::make_unique<NWrappers::NExternalStorage::TEvPutObjectRequest>(awsRequest, std::move(moveData));
    auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
    TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvPutObjectRequest>> evPtr((TEventHandle<NWrappers::NExternalStorage::TEvPutObjectRequest>*)hRequest.release());
    ExternalStorageOperator->Execute(evPtr);
}

void TWriteAction::DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
    if (blobsWroteSuccessfully) {
        for (auto&& i : GetBlobsForWrite()) {
            dbBlobs.RemoveTierDraftBlobId(GetStorageId(), i.first);
        }
    }
}

void TWriteAction::DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& dbBlobs) {
    for (auto&& i : GetBlobsForWrite()) {
        dbBlobs.AddTierDraftBlobId(GetStorageId(), i.first);
    }
}

NKikimr::NOlap::TUnifiedBlobId TWriteAction::AllocateNextBlobId(const TString& data) {
    static TAtomic Counter = 0;
    auto now = TInstant::Now();
    return TUnifiedBlobId(Max<ui32>(), TLogoBlobID(TabletId, now.GetValue() >> 32, now.GetValue() & Max<ui32>(), TLogoBlobID::MaxChannel, data.size(), AtomicIncrement(Counter) % TLogoBlobID::MaxCookie, 1));
}

void TWriteAction::DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/, const bool blobsWroteSuccessfully) {
    if (!blobsWroteSuccessfully) {
        for (auto&& i : GetBlobsForWrite()) {
            GCInfo->MutableDraftBlobIdsToRemove().emplace_back(i.first);
        }
    }
}

}
