#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TIndexedWriteController::TBlobConstructor::TBlobConstructor(NOlap::ISnapshotSchema::TPtr snapshotSchema, TIndexedWriteController& owner)
    : Owner(owner)
    , SnapshotSchema(snapshotSchema)
{}

const TString& TIndexedWriteController::TBlobConstructor::GetBlob() const {
    return DataPrepared;
}

IBlobConstructor::EStatus TIndexedWriteController::TBlobConstructor::BuildNext() {
    if (!!DataPrepared) {
        return EStatus::Finished;
    }

    const auto& writeMeta = Owner.WriteData.GetWriteMeta();
    const ui64 tableId = writeMeta.GetTableId();
    const ui64 writeId = writeMeta.GetWriteId();

    // Heavy operations inside. We cannot run them in tablet event handler.
    {
        NColumnShard::TCpuGuard guard(Owner.ResourceUsage);
        Batch = Owner.WriteData.GetData().GetArrowBatch();
    }

    if (!Batch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("table_id", tableId);
        return EStatus::Error;
    }

    {
        NColumnShard::TCpuGuard guard(Owner.ResourceUsage);
        DataPrepared = NArrow::SerializeBatchNoCompression(Batch);
    }

    if (DataPrepared.size() > NColumnShard::TLimits::GetMaxBlobSize()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_data_too_big")("write_id", writeId)("table_id", tableId);
        return EStatus::Error;
    }
    return EStatus::Ok;
}

bool TIndexedWriteController::TBlobConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
    Y_VERIFY(blobId.BlobSize() == DataPrepared.size());
    Owner.AddBlob(NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData(blobId, DataPrepared, Batch));
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
        Y_VERIFY(BlobData.size() == 1);
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(BlobData[0]), WriteData.GetWriteMeta(), BlobConstructor->GetSnapshot());
        ctx.Send(DstActor, result.release());
    } else {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, WriteData.GetWriteMeta(), BlobConstructor->GetSnapshot());
        ctx.Send(DstActor, result.release());
    }
}

void TIndexedWriteController::AddBlob(NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData&& data) {
    Y_VERIFY(BlobData.empty());

    ui64 dirtyTime = AppData()->TimeProvider->Now().Seconds();
    Y_VERIFY(dirtyTime);
    NKikimrTxColumnShard::TLogicalMetadata outMeta;
    outMeta.SetNumRows(data.GetParsedBatch()->num_rows());
    outMeta.SetRawBytes(NArrow::GetBatchDataSize(data.GetParsedBatch()));
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
