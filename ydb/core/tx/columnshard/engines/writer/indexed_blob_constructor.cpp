#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TIndexedBlobConstructor::TIndexedBlobConstructor(TAutoPtr<TEvColumnShard::TEvWrite> writeEv, NOlap::ISnapshotSchema::TPtr snapshotSchema)
    : WriteEv(writeEv)
    , SnapshotSchema(snapshotSchema)
{}

const TString& TIndexedBlobConstructor::GetBlob() const {
    return DataPrepared;
}

IBlobConstructor::EStatus TIndexedBlobConstructor::BuildNext() {
    if (!!DataPrepared) {
        return EStatus::Finished;
    }

    const auto& evProto = Proto(WriteEv.Get());
    const ui64 pathId = evProto.GetTableId();
    const ui64 writeId = evProto.GetWriteId();

    TString serializedScheme;
    if (evProto.HasMeta()) {
        serializedScheme = evProto.GetMeta().GetSchema();
        if (serializedScheme.empty() || evProto.GetMeta().GetFormat() != NKikimrTxColumnShard::FORMAT_ARROW) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_parsing_fails")("write_id", writeId)("path_id", pathId);
            return EStatus::Error;
        }
    }
    const auto& data = evProto.GetData();

    // Heavy operations inside. We cannot run them in tablet event handler.
    TString strError;
    {
        NColumnShard::TCpuGuard guard(ResourceUsage);
        Batch = SnapshotSchema->PrepareForInsert(data, serializedScheme, strError);
    }
    if (!Batch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("path_id", pathId)("error", strError);
        return EStatus::Error;
    }

    {
        NColumnShard::TCpuGuard guard(ResourceUsage);
        DataPrepared = NArrow::SerializeBatchNoCompression(Batch);
    }

    if (DataPrepared.size() > NColumnShard::TLimits::GetMaxBlobSize()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_data_too_big")("write_id", writeId)("path_id", pathId);
        return EStatus::Error;
    }

    ui64 dirtyTime = TAppData::TimeProvider->Now().Seconds();
    Y_VERIFY(dirtyTime);
    NKikimrTxColumnShard::TLogicalMetadata outMeta;
    outMeta.SetNumRows(Batch->num_rows());
    outMeta.SetRawBytes(NArrow::GetBatchDataSize(Batch));
    outMeta.SetDirtyWriteTimeSeconds(dirtyTime);

    if (!outMeta.SerializeToString(&MetaString)) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_metadata")("write_id", writeId)("path_id", pathId);
        return EStatus::Error;;
    }

    ++Iteration;
    return EStatus::Ok;
}

bool TIndexedBlobConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
    Y_VERIFY(Iteration == 1);
    WriteEv->BlobId = blobId;
    Y_VERIFY(WriteEv->BlobId.BlobSize() == DataPrepared.size());
    return true;
}

TAutoPtr<IEventBase> TIndexedBlobConstructor::BuildResult(NKikimrProto::EReplyStatus status,
                                                        NColumnShard::TBlobBatch&& blobBatch,
                                                        THashSet<ui32>&& yellowMoveChannels,
                                                        THashSet<ui32>&& yellowStopChannels)
{
    WriteEv->WrittenBatch = Batch;

    auto& record = Proto(WriteEv.Get());
    record.SetData(DataPrepared); // modify for TxWrite
    record.MutableMeta()->SetLogicalMeta(MetaString);

    WriteEv->ResourceUsage.Add(ResourceUsage);
    WriteEv->SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
    WriteEv->BlobBatch = std::move(blobBatch);
    return WriteEv.Release();
}

}
