#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxWrite : public TTransactionBase<TColumnShard> {
public:
    TTxWrite(TColumnShard* self, const TEvPrivate::TEvWriteBlobsResult::TPtr& putBlobResult)
        : TBase(self)
        , PutBlobResult(putBlobResult)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE; }

private:
    TEvPrivate::TEvWriteBlobsResult::TPtr PutBlobResult;
    const ui32 TabletTxNo;
    std::unique_ptr<NActors::IEventBase> Result;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWrite[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};


bool TTxWrite::Execute(TTransactionContext& txc, const TActorContext&) {
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    const auto& writeMeta(PutBlobResult->Get()->GetWriteMeta());
    const auto& blobData(PutBlobResult->Get()->GetBlobData());

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    auto writeId = writeMeta.GetWriteId();
    const TString& data = blobData.GetBlobData();

    NKikimrTxColumnShard::TLogicalMetadata meta;
    Y_VERIFY(meta.ParseFromString(blobData.GetLogicalMeta()));

    ui32 status = NKikimrTxColumnShard::EResultStatus::SUCCESS;
    const auto& logoBlobId = blobData.GetBlobId();
    Y_VERIFY(logoBlobId.IsValid());

    Y_VERIFY(Self->TablesManager.IsReadyForWrite(writeMeta.GetTableId()));
    if (writeMeta.HasLongTxId()) {
        Y_VERIFY(writeMeta.GetMetaShard() == 0);
        writeId = (ui64)Self->GetLongTxWrite(db, writeMeta.GetLongTxIdUnsafe(), writeMeta.GetWritePartId());
    }

    ui64 writeUnixTime = meta.GetDirtyWriteTimeSeconds();
    TInstant time = TInstant::Seconds(writeUnixTime);

    // First write wins
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    NOlap::TInsertedData insertData(writeMeta.GetMetaShard(), (ui64)writeId, writeMeta.GetTableId(), writeMeta.GetDedupId(), logoBlobId, blobData.GetLogicalMeta(), time, PutBlobResult->Get()->GetSnapshot());
    bool ok = Self->InsertTable->Insert(dbTable, std::move(insertData));
    if (ok) {
        THashSet<TWriteId> writesToAbort = Self->InsertTable->OldWritesToAbort(time);
        Self->TryAbortWrites(db, dbTable, std::move(writesToAbort));

        // TODO: It leads to write+erase for aborted rows. Abort() inserts rows, EraseAborted() erases them.
        // It's not optimal but correct.
        TBlobManagerDb blobManagerDb(txc.DB);
        auto allAborted = Self->InsertTable->GetAborted(); // copy (src is modified in cycle)
        for (auto& [abortedWriteId, abortedData] : allAborted) {
            Self->InsertTable->EraseAborted(dbTable, abortedData);
            Self->BlobManager->DeleteBlob(abortedData.BlobId, blobManagerDb);
        }

        // Put new data into blob cache
        Y_VERIFY(logoBlobId.BlobSize() == data.size());
        NBlobCache::AddRangeToCache(NBlobCache::TBlobRange(logoBlobId, 0, data.size()), data);

        // Put new data into batch cache
        Y_VERIFY(blobData.GetParsedBatch());
        Self->BatchCache.Insert(TWriteId(writeId), logoBlobId, blobData.GetParsedBatch());

        Self->UpdateInsertTableCounters();

        const auto& blobBatch(PutBlobResult->Get()->GetPutResult().GetBlobBatch());
        ui64 blobsWritten = blobBatch.GetBlobCount();
        ui64 bytesWritten = blobBatch.GetTotalSize();
        Self->IncCounter(COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
        Self->IncCounter(COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
        Self->IncCounter(COUNTER_RAW_BYTES_UPSERTED, meta.GetRawBytes());
        Self->IncCounter(COUNTER_WRITE_SUCCESS);

        Self->BlobManager->SaveBlobBatch((std::move(PutBlobResult->Get()->GetPutResultPtr()->ReleaseBlobBatch())), blobManagerDb);
    } else {
        LOG_S_DEBUG(TxPrefix() << "duplicate writeId " << writeId << TxSuffix());
    }

    // Return EResultStatus::SUCCESS for dups
    Self->IncCounter(COUNTER_WRITE_DUPLICATE);

    if (status != NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        Self->IncCounter(COUNTER_WRITE_FAIL);
    }

    Result = std::make_unique<TEvColumnShard::TEvWriteResult>(Self->TabletID(), writeMeta, writeId, status);
    return true;
}

void TTxWrite::Complete(const TActorContext& ctx) {
    Y_VERIFY(Result);
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());
    ctx.Send(PutBlobResult->Get()->GetWriteMeta().GetSource(), Result.release());
}

void TColumnShard::OverloadWriteFail(const EOverloadStatus& overloadReason, const NEvWrite::TWriteData& writeData, const TActorContext& ctx) {
    IncCounter(COUNTER_WRITE_FAIL);
    if (overloadReason == EOverloadStatus::Disk) {
        IncCounter(COUNTER_OUT_OF_SPACE);
    } else {
        IncCounter(COUNTER_WRITE_OVERLOAD);
    }

    LOG_S_INFO("Write (overload) " << writeData.GetSize() << " bytes into pathId " << writeData.GetWriteMeta().GetTableId()
        << " overload reason: [" << overloadReason << "]"
        << " at tablet " << TabletID());

    auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeData.GetWriteMeta(), NKikimrTxColumnShard::EResultStatus::OVERLOADED);
    ctx.Send(writeData.GetWriteMeta().GetSource(), result.release());
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloaded(const ui64 tableId, const ui64 dataSize) const {
    if (IsAnyChannelYellowStop()) {
        return EOverloadStatus::Disk;
    }

    if (InsertTable && InsertTable->IsOverloadedByCommitted(tableId)) {
        CSCounters.OnOverloadInsertTable(dataSize);
        return EOverloadStatus::InsertTable;
    } else if (TablesManager.IsOverloaded(tableId)) {
        CSCounters.OnOverloadGranule(dataSize);;
        return EOverloadStatus::Granule;
    } else if (WritesMonitor.ShardOverloaded()) {
        CSCounters.OnOverloadShard(dataSize);
        return EOverloadStatus::Shard;
    }
    return EOverloadStatus::None;
}

void TColumnShard::Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx) {
    auto& putResult = ev->Get()->GetPutResult();
    OnYellowChannels(putResult);
    const auto& writeMeta = ev->Get()->GetWriteMeta();

    auto wg = WritesMonitor.FinishWrite(putResult.GetResourceUsage().SourceMemorySize);

    if (putResult.GetPutStatus() != NKikimrProto::OK) {
        IncCounter(COUNTER_WRITE_FAIL);

        auto errCode = NKikimrTxColumnShard::EResultStatus::ERROR;
        if (putResult.GetPutStatus() == NKikimrProto::TIMEOUT || putResult.GetPutStatus() == NKikimrProto::DEADLINE) {
            errCode = NKikimrTxColumnShard::EResultStatus::TIMEOUT;
        } else if (putResult.GetPutStatus() == NKikimrProto::TRYLATER || putResult.GetPutStatus() == NKikimrProto::OUT_OF_SPACE) {
            errCode = NKikimrTxColumnShard::EResultStatus::OVERLOADED;
        } else {
            errCode = NKikimrTxColumnShard::EResultStatus::STORAGE_ERROR;
        }

        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, errCode);
        ctx.Send(writeMeta.GetSource(), result.release());
        return;
    }

    const auto& blobData = ev->Get()->GetBlobData();
    Y_VERIFY(blobData.GetBlobId().IsValid());
    LOG_S_DEBUG("Write (record) " << blobData.GetBlobData().size() << " bytes into pathId " << writeMeta.GetTableId()
        << (writeMeta.GetWriteId() ? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : "") << " at tablet " << TabletID());

    Execute(new TTxWrite(this, ev), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    LastAccessTime = TAppData::TimeProvider->Now();

    const auto& record = Proto(ev->Get());
    const ui64 tableId = record.GetTableId();
    const ui64 metaShard = record.GetTxInitiator();
    const ui64 writeId = record.GetWriteId();
    const TString dedupId = record.GetDedupId();
    const auto source = ev->Get()->GetSource();

    NEvWrite::TWriteMeta writeMeta(writeId, tableId, source);
    writeMeta.SetMetaShard(metaShard);
    writeMeta.SetDedupId(dedupId);
    if (record.HasLongTxId()) {
        Y_VERIFY(metaShard == 0);
        writeMeta.SetLongTxId(NLongTxService::TLongTxId::FromProto(record.GetLongTxId()));
        writeMeta.SetWritePartId(record.GetWritePartId());
    }

    auto arrowData = std::make_shared<NEvWrite::TArrowData>();
    if (!arrowData->ParseFromProto(record)) {
        LOG_S_ERROR("Write (fail) " << record.GetData().size() << " bytes into pathId " << writeMeta.GetTableId()
            << " at tablet " << TabletID());
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR);
        ctx.Send(ev->Get()->GetSource(), result.release());
        return;
    }

    NEvWrite::TWriteData writeData(writeMeta, arrowData);
    auto overloadStatus = CheckOverloaded(tableId, writeData.GetSize());
    if (!TablesManager.IsReadyForWrite(tableId)) {
        LOG_S_NOTICE("Write (fail) into pathId:" << writeMeta.GetTableId() << (TablesManager.HasPrimaryIndex()? "": " no index")
            << " at tablet " << TabletID());
        IncCounter(COUNTER_WRITE_FAIL);

        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR);
        ctx.Send(ev->Get()->GetSource(), result.release());
    } else if (overloadStatus != EOverloadStatus::None) {
        OverloadWriteFail(overloadStatus, writeData, ctx);
    } else {
        if (writeMeta.HasLongTxId()) {
            // TODO: multiple blobs in one longTx ({longTxId, dedupId} -> writeId)
            if (ui64 writeId = (ui64) HasLongTxWrite(writeMeta.GetLongTxIdUnsafe(), writeMeta.GetWritePartId())) {
                LOG_S_DEBUG("Write (duplicate) into pathId " << writeMeta.GetTableId()
                    << " longTx " << writeMeta.GetLongTxIdUnsafe().ToString()
                    << " at tablet " << TabletID());

                IncCounter(COUNTER_WRITE_DUPLICATE);

                auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
                    TabletID(), writeMeta, writeId, NKikimrTxColumnShard::EResultStatus::SUCCESS);
                ctx.Send(writeMeta.GetSource(), result.release());
                return;
            }
        }

        auto wg = WritesMonitor.RegisterWrite(writeData.GetSize());

        LOG_S_DEBUG("Write (blob) " << writeData.GetSize() << " bytes into pathId " << writeMeta.GetTableId()
            << (writeMeta.GetWriteId()? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : " ")
            << WritesMonitor.DebugString()
            << " at tablet " << TabletID());

        const auto& snapshotSchema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
        auto writeController = std::make_shared<NOlap::TIndexedWriteController>(ctx.SelfID, writeData, snapshotSchema);
        ctx.Register(CreateWriteActor(TabletID(), writeController, BlobManager->StartBlobBatch(), TInstant::Max(), Settings.MaxSmallBlobSize));
    }
}

}
