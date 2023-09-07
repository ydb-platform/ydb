#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TIndexedWriteController::TBlobConstructor::TBlobConstructor(TIndexedWriteController& owner)
    : Owner(owner)
{}

const TString& TIndexedWriteController::TBlobConstructor::GetBlob() const {
    Y_VERIFY(CurrentIndex > 0);
    return BlobsSplitted[CurrentIndex - 1].GetData();
}

IBlobConstructor::EStatus TIndexedWriteController::TBlobConstructor::BuildNext() {
    if (CurrentIndex == BlobsSplitted.size()) {
        return EStatus::Finished;
    }
    CurrentIndex++;
    return EStatus::Ok;
}

bool TIndexedWriteController::TBlobConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
    const auto& blobInfo = BlobsSplitted[CurrentIndex - 1];
    Owner.BlobData.emplace_back(TBlobRange(blobId, 0, blobId.BlobSize()), blobInfo.GetData(), blobInfo.GetSpecialKeys(), blobInfo.GetRowsCount(), blobInfo.GetRawBytes(), AppData()->TimeProvider->Now());
    return true;
}

bool TIndexedWriteController::TBlobConstructor::Init() {
    const auto& writeMeta = Owner.WriteData.GetWriteMeta();
    const ui64 tableId = writeMeta.GetTableId();
    const ui64 writeId = writeMeta.GetWriteId();

    std::shared_ptr<arrow::RecordBatch> batch;
    {
        NColumnShard::TCpuGuard guard(Owner.ResourceUsage);
        batch = Owner.WriteData.GetData().GetArrowBatch();
    }

    if (!batch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("table_id", tableId);
        return false;
    }

    auto splitResult = NArrow::SplitByBlobSize(batch, NColumnShard::TLimits::GetMaxBlobSize());
    if (!splitResult) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", TStringBuilder() << "cannot split batch in according to limits: " + splitResult.GetErrorMessage());
        return false;
    }
    BlobsSplitted = splitResult.ReleaseResult();
    if (BlobsSplitted.size() > 1) {
        for (auto&& i : BlobsSplitted) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "strange_blobs_splitting")("blob", i.DebugString())("original_size", Owner.WriteData.GetSize());
        }
    }
    return true;
}

TIndexedWriteController::TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData)
    : WriteData(writeData)
    , BlobConstructor(std::make_shared<TBlobConstructor>(*this))
    , DstActor(dstActor)
{
    ResourceUsage.SourceMemorySize = WriteData.GetSize();
}

void TIndexedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    if (putResult->GetPutStatus() == NKikimrProto::OK) {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(BlobData), WriteData.GetWriteMeta(), WriteData.GetData().GetSchemaVersion());
        ctx.Send(DstActor, result.release());
    } else {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, WriteData.GetWriteMeta());
        ctx.Send(DstActor, result.release());
    }
}

NOlap::IBlobConstructor::TPtr TIndexedWriteController::GetBlobConstructor() {
    if (!BlobConstructor->Init()) {
        return nullptr;
    }
    return BlobConstructor;
}

}
