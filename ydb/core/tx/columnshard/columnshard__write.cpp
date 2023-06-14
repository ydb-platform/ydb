#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxWrite : public TTransactionBase<TColumnShard> {
public:
    TTxWrite(TColumnShard* self, TEvColumnShard::TEvWrite::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE; }

private:
    TEvColumnShard::TEvWrite::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvWriteResult> Result;
};


bool TTxWrite::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxWrite.Execute at tablet " << Self->TabletID());

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    auto& record = Proto(Ev->Get());

    ui64 metaShard = record.GetTxInitiator();
    ui64 writeId = record.GetWriteId();
    ui64 tableId = record.GetTableId();
    TString dedupId = record.GetDedupId();
    TString data = record.GetData();
    auto& metaStr = record.GetMeta().GetLogicalMeta();
    NKikimrTxColumnShard::TLogicalMetadata meta;
    Y_VERIFY(meta.ParseFromString(metaStr)); // TODO: get it from message

    ui32 status = NKikimrTxColumnShard::EResultStatus::SUCCESS;
    auto& logoBlobId = Ev->Get()->BlobId;
    auto putStatus = Ev->Get()->GetPutStatus();
    Y_VERIFY(putStatus == NKikimrProto::OK);
    Y_VERIFY(logoBlobId.IsValid());

    bool ok = false;
    if (!Self->TablesManager.HasPrimaryIndex() || !Self->TablesManager.IsWritableTable(tableId)) {
        status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
    } else {
        if (record.HasLongTxId()) {
            Y_VERIFY(metaShard == 0);
            auto longTxId = NLongTxService::TLongTxId::FromProto(record.GetLongTxId());
            writeId = (ui64)Self->GetLongTxWrite(db, longTxId, record.GetWritePartId());
        }

        ui64 writeUnixTime = meta.GetDirtyWriteTimeSeconds();
        TInstant time = TInstant::Seconds(writeUnixTime);

        // First write wins
        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        const auto& snapshotSchema = Self->TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
        NOlap::TInsertedData insertData(metaShard, writeId, tableId, dedupId, logoBlobId, metaStr, time, snapshotSchema->GetSnapshot());
        ok = Self->InsertTable->Insert(dbTable, std::move(insertData));
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
            Y_VERIFY(Ev->Get()->WrittenBatch);
            Self->BatchCache.Insert(TWriteId(writeId), logoBlobId, Ev->Get()->WrittenBatch);

            Self->UpdateInsertTableCounters();

            ui64 blobsWritten = Ev->Get()->BlobBatch.GetBlobCount();
            ui64 bytesWritten = Ev->Get()->BlobBatch.GetTotalSize();
            Self->IncCounter(COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
            Self->IncCounter(COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
            Self->IncCounter(COUNTER_RAW_BYTES_UPSERTED, meta.GetRawBytes());
            Self->IncCounter(COUNTER_WRITE_SUCCESS);

            Self->BlobManager->SaveBlobBatch(std::move(Ev->Get()->BlobBatch), blobManagerDb);
        } else {
            LOG_S_DEBUG("TTxWrite duplicate writeId " << writeId << " at tablet " << Self->TabletID());

            // Return EResultStatus::SUCCESS for dups
            Self->IncCounter(COUNTER_WRITE_DUPLICATE);
        }
    }

    if (status != NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        Self->IncCounter(COUNTER_WRITE_FAIL);
    }

    Result = std::make_unique<TEvColumnShard::TEvWriteResult>(
        Self->TabletID(), metaShard, writeId, tableId, dedupId, status);
    return true;
}

void TTxWrite::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Result);
    LOG_S_DEBUG("TTxWrite.Complete at tablet " << Self->TabletID());

    ctx.Send(Ev->Get()->GetSource(), Result.release());
}

void TColumnShard::OverloadWriteFail(const TString& overloadReason, TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    IncCounter(COUNTER_WRITE_FAIL);
    IncCounter(COUNTER_WRITE_OVERLOAD);

    const auto& record = Proto(ev->Get());
    const auto& data = record.GetData();
    const ui64 tableId = record.GetTableId();
    const ui64 metaShard = record.GetTxInitiator();
    const ui64 writeId = record.GetWriteId();
    const TString& dedupId = record.GetDedupId();

    LOG_S_INFO("Write (overload) " << data.size() << " bytes into pathId " << tableId
        << "overload reason: [" << overloadReason << "]"
        << " at tablet " << TabletID());

    auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
        TabletID(), metaShard, writeId, tableId, dedupId, NKikimrTxColumnShard::EResultStatus::OVERLOADED);
    ctx.Send(ev->Get()->GetSource(), result.release());
}

// EvWrite -> WriteActor (attach BlobId without proto changes) -> EvWrite
void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    LastAccessTime = TAppData::TimeProvider->Now();

    OnYellowChannels(*ev->Get());

    const auto& record = Proto(ev->Get());
    const auto& data = record.GetData();
    const ui64 tableId = record.GetTableId();
    const ui64 metaShard = record.GetTxInitiator();
    const ui64 writeId = record.GetWriteId();
    const TString dedupId = record.GetDedupId();
    const auto putStatus = ev->Get()->GetPutStatus();

    bool isWritable = TablesManager.IsWritableTable(tableId);
    bool error = data.empty() || data.size() > TLimits::GetMaxBlobSize() || !TablesManager.HasPrimaryIndex() || !isWritable;
    bool errorReturned = (putStatus != NKikimrProto::OK) && (putStatus != NKikimrProto::UNKNOWN);
    bool isOutOfSpace = IsAnyChannelYellowStop();

    if (error || errorReturned) {
        LOG_S_NOTICE("Write (fail) " << data.size() << " bytes into pathId " << tableId
            << ", status " << putStatus
            << (TablesManager.HasPrimaryIndex()? "": ", no index") << (isWritable? "": ", ro")
            << " at tablet " << TabletID());

        IncCounter(COUNTER_WRITE_FAIL);

        auto errCode = NKikimrTxColumnShard::EResultStatus::ERROR;
        if (errorReturned) {
            if (putStatus == NKikimrProto::TIMEOUT || putStatus == NKikimrProto::DEADLINE) {
                errCode = NKikimrTxColumnShard::EResultStatus::TIMEOUT;
            } else if (putStatus == NKikimrProto::TRYLATER || putStatus == NKikimrProto::OUT_OF_SPACE) {
                errCode = NKikimrTxColumnShard::EResultStatus::OVERLOADED;
            } else {
                errCode = NKikimrTxColumnShard::EResultStatus::STORAGE_ERROR;
            }
            --WritesInFlight; // write failed
            WritesSizeInFlight -= ev->Get()->ResourceUsage.SourceMemorySize;
        }

        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
            TabletID(), metaShard, writeId, tableId, dedupId, errCode);
        ctx.Send(ev->Get()->GetSource(), result.release());

    } else if (ev->Get()->BlobId.IsValid()) {
        LOG_S_DEBUG("Write (record) " << data.size() << " bytes into pathId " << tableId
            << (writeId? (" writeId " + ToString(writeId)).c_str() : "") << " at tablet " << TabletID());

        --WritesInFlight; // write successed
        WritesSizeInFlight -= ev->Get()->ResourceUsage.SourceMemorySize;
        Y_VERIFY(putStatus == NKikimrProto::OK);
        Execute(new TTxWrite(this, ev), ctx);
    } else if (isOutOfSpace) {
        IncCounter(COUNTER_WRITE_FAIL);
        IncCounter(COUNTER_OUT_OF_SPACE);
        LOG_S_ERROR("Write (out of disk space) " << data.size() << " bytes into pathId " << tableId
            << " at tablet " << TabletID());

        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
            TabletID(), metaShard, writeId, tableId, dedupId, NKikimrTxColumnShard::EResultStatus::OVERLOADED);
        ctx.Send(ev->Get()->GetSource(), result.release());
    } else if (InsertTable && InsertTable->IsOverloadedByCommitted(tableId)) {
        CSCounters.OnOverloadInsertTable(data.size());
        OverloadWriteFail("insert_table", ev, ctx);
    } else if (TablesManager.IsOverloaded(tableId)) {
        CSCounters.OnOverloadGranule(data.size());
        OverloadWriteFail("granule", ev, ctx);
    } else if (ShardOverloaded()) {
        CSCounters.OnOverloadShard(data.size());
        OverloadWriteFail("shard", ev, ctx);
    } else {
        if (record.HasLongTxId()) {
            // TODO: multiple blobs in one longTx ({longTxId, dedupId} -> writeId)
            auto longTxId = NLongTxService::TLongTxId::FromProto(record.GetLongTxId());
            if (ui64 writeId = (ui64)HasLongTxWrite(longTxId, record.GetWritePartId())) {
                LOG_S_DEBUG("Write (duplicate) into pathId " << tableId
                    << " longTx " << longTxId.ToString()
                    << " at tablet " << TabletID());

                IncCounter(COUNTER_WRITE_DUPLICATE);

                auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
                    TabletID(), metaShard, writeId, tableId, dedupId, NKikimrTxColumnShard::EResultStatus::SUCCESS);
                ctx.Send(ev->Get()->GetSource(), result.release());
                return;
            }
        }

        ev->Get()->MaxSmallBlobSize = Settings.MaxSmallBlobSize;
        ev->Get()->ResourceUsage.SourceMemorySize = data.size();

        ++WritesInFlight; // write started
        WritesSizeInFlight += ev->Get()->ResourceUsage.SourceMemorySize;

        LOG_S_DEBUG("Write (blob) " << data.size() << " bytes into pathId " << tableId
            << (writeId? (" writeId " + ToString(writeId)).c_str() : "")
            << " inflight " << WritesInFlight << " (" << WritesSizeInFlight << " bytes)"
            << " at tablet " << TabletID());

        const auto& snapshotSchema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
        ctx.Register(CreateWriteActor(TabletID(), snapshotSchema, ctx.SelfID,
            BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
    }

    SetCounter(COUNTER_WRITES_IN_FLY, WritesInFlight);
}

}
