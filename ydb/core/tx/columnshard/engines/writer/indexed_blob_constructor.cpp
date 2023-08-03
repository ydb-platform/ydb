#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TIndexedWriteController::TBlobConstructor::TBlobConstructor(NOlap::ISnapshotSchema::TPtr snapshotSchema, TIndexedWriteController& owner)
    : Owner(owner)
    , SnapshotSchema(snapshotSchema)
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
    Owner.AddBlob(NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData(blobId, blobInfo.GetData(), blobInfo.GetRowsCount(), blobInfo.GetRawBytes()));
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
    return true;
}

TIndexedWriteController::TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData, NOlap::ISnapshotSchema::TPtr snapshotSchema)
    : WriteData(writeData)
    , BlobConstructor(std::make_shared<TBlobConstructor>(snapshotSchema, *this))
    , DstActor(dstActor)
{
    ResourceUsage.SourceMemorySize = WriteData.GetSize();
}

void TIndexedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    if (putResult->GetPutStatus() == NKikimrProto::OK) {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(BlobData), WriteData.GetWriteMeta(), BlobConstructor->GetSnapshot());
        ctx.Send(DstActor, result.release());
    } else {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, WriteData.GetWriteMeta(), BlobConstructor->GetSnapshot());
        ctx.Send(DstActor, result.release());
    }
}

NOlap::IBlobConstructor::TPtr TIndexedWriteController::GetBlobConstructor() {
    if (!BlobConstructor->Init()) {
        return nullptr;
    }
    return BlobConstructor;
}

void TIndexedWriteController::AddBlob(NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData&& data) {
    ui64 dirtyTime = AppData()->TimeProvider->Now().Seconds();
    Y_VERIFY(dirtyTime);

    NKikimrTxColumnShard::TLogicalMetadata outMeta;
    outMeta.SetNumRows(data.GetRowsCount());
    outMeta.SetRawBytes(data.GetRawBytes());
    outMeta.SetDirtyWriteTimeSeconds(dirtyTime);

    TString metaString;
    if (!outMeta.SerializeToString(&metaString)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_metadata");
        Y_VERIFY(false);
    }
    BlobData.push_back(std::move(data));
    BlobData.back().SetLogicalMeta(metaString);
}

}
