#include "object_key.h"
#include "read.h"
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/wrappers/unavailable_storage.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TReadingAction::DoStartReading(THashSet<TBlobRange>&& ranges) {
    for (auto&& r : ranges) {
        DoRetryRead(r);
    }
}

void TReadingAction::DoRetryRead(const TBlobRange& range) {
    auto awsRequest = Aws::S3::Model::GetObjectRequest()
                          .WithKey(TObjectKey(GetStorageId()).Make(range.BlobId.GetLogoBlobId()))
                          .WithRange(TStringBuilder() << "bytes=" << range.Offset << "-" << range.Offset + range.Size - 1);
    auto request = std::make_unique<NWrappers::NExternalStorage::TEvGetObjectRequest>(awsRequest);
    auto hRequest = std::make_unique<IEventHandle>(NActors::TActorId(), TActorContext::AsActorContext().SelfID, request.release());
    TAutoPtr<TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>> evPtr(
        (TEventHandle<NWrappers::NExternalStorage::TEvGetObjectRequest>*)hRequest.release());
    auto storageOperator = ExternalStorageOperator->Get();
    AFL_VERIFY(storageOperator);
    const auto context = GetReadStatsContext(range);
    std::shared_ptr<NScanStats::TOperation> attempt;
    if (context.Collector) {
        const bool isRetry = !DispatchedRanges.emplace(range).second;
        attempt = context.Collector->Start(NScanStats::TStep::StorageRead, "STORAGE_READ", "ExternalStorage", context.Parent);
        if (attempt) {
            attempt->Update([&](auto& record) {
                auto* details = record.MutableDetails()->MutableStorageRead();
                details->SetStorageTier(GetStorageId());
                details->SetBackendKind(dynamic_cast<const NWrappers::NExternalStorage::TFakeExternalStorageOperator*>(storageOperator.get())
                    ? NKqpProto::TColumnShardStepDetails::TStorageRead::MemoryOrFake
                    : dynamic_cast<const NWrappers::NExternalStorage::TUnavailableExternalStorageOperator*>(storageOperator.get())
                        ? NKqpProto::TColumnShardStepDetails::TStorageRead::Unknown
                        : NKqpProto::TColumnShardStepDetails::TStorageRead::ExternalStorage);
                details->SetDataKind(context.DataKind);
                details->SetRequestedRangeBytes(range.Size);
                details->SetIsRetry(isRetry);
            });
            ReadAttempts.emplace(range, attempt);
            attempt->Resume();
        }
    }
    storageOperator->Execute(evPtr);
    if (attempt) {
        attempt->Suspend();
    }
}

void TReadingAction::OnReadAttemptResult(const TBlobRange& range, std::optional<ui64> payloadBytes, bool success) {
    auto it = ReadAttempts.find(range);
    if (it == ReadAttempts.end()) {
        return;
    }
    it->second->Resume();
    if (payloadBytes) {
        it->second->AddReadBytes(*payloadBytes);
    }
    it->second->Finish(success ? NScanStats::TStep::completed : NScanStats::TStep::failed);
    ReadAttempts.erase(it);
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
