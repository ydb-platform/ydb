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
    auto putStatus = Ev->Get()->PutStatus;

    bool ok = false;
    if (!Self->PrimaryIndex || !Self->IsTableWritable(tableId)) {
        status = NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR;
    } else if (putStatus == NKikimrProto::OK && logoBlobId.IsValid()) {
        if (record.HasLongTxId()) {
            Y_VERIFY(metaShard == 0);
            auto longTxId = NLongTxService::TLongTxId::FromProto(record.GetLongTxId());
            writeId = (ui64)Self->GetLongTxWrite(db, longTxId);
        }

        ui64 writeUnixTime = meta.GetDirtyWriteTimeSeconds();
        TInstant time = TInstant::Seconds(writeUnixTime);

        // First write wins
        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
        ok = Self->InsertTable->Insert(dbTable, NOlap::TInsertedData(metaShard, writeId, tableId, dedupId, logoBlobId, metaStr, time));
        if (ok) {
            auto newAborted = Self->InsertTable->AbortOld(dbTable, time);
            for (auto& writeId : newAborted) {
                Self->RemoveLongTxWrite(db, writeId);
            }

            // TODO: It leads to write+erase for new aborted rows. AbortOld() inserts rows, EraseAborted() erases them.
            // It's not optimal but correct.
            TBlobManagerDb blobManagerDb(txc.DB);
            auto allAborted = Self->InsertTable->GetAborted(); // copy (src is modified in cycle)
            for (auto& [abortedWriteId, abortedData] : allAborted) {
                Self->InsertTable->EraseAborted(dbTable, abortedData);
                Self->BlobManager->DeleteBlob(abortedData.BlobId, blobManagerDb);
            }

            // Put new data into cache
            Y_VERIFY(logoBlobId.BlobSize() == data.size());
            NBlobCache::AddRangeToCache(NBlobCache::TBlobRange(logoBlobId, 0, data.size()), data);

            bool committedChanged = !allAborted.empty();
            Self->UpdateInsertTableCounters(committedChanged);

            ui64 blobsWritten = Ev->Get()->BlobBatch.GetBlobCount();
            ui64 bytesWritten = Ev->Get()->BlobBatch.GetTotalSize();
            Self->IncCounter(COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
            Self->IncCounter(COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
            Self->IncCounter(COUNTER_RAW_BYTES_UPSERTED, meta.GetRawBytes());
            Self->IncCounter(COUNTER_WRITE_SUCCESS);

            Self->BlobManager->SaveBlobBatch(std::move(Ev->Get()->BlobBatch), blobManagerDb);
        } else {
            // Return EResultStatus::SUCCESS for dups
            Self->IncCounter(COUNTER_WRITE_DUPLICATE);
        }
    } else if (putStatus == NKikimrProto::TIMEOUT) {
        status = NKikimrTxColumnShard::EResultStatus::TIMEOUT;
    } else if (putStatus == NKikimrProto::TRYLATER) {
        status = NKikimrTxColumnShard::EResultStatus::OVERLOADED;
    } else {
        status = NKikimrTxColumnShard::EResultStatus::ERROR;
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


// EvWrite -> WriteActor (attach BlobId without proto changes) -> EvWrite
void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    OnYellowChannels(std::move(ev->Get()->YellowMoveChannels), std::move(ev->Get()->YellowStopChannels));

    auto& data = Proto(ev->Get()).GetData();
    const ui64 tableId = ev->Get()->Record.GetTableId();
    bool error = data.empty() || data.size() > TLimits::MAX_BLOB_SIZE || !PrimaryIndex || !IsTableWritable(tableId)
        || ev->Get()->PutStatus == NKikimrProto::ERROR;

    if (error) {
        LOG_S_WARN("Write (fail) " << data.size() << " bytes at tablet " << TabletID());

        ev->Get()->PutStatus = NKikimrProto::ERROR;
        Execute(new TTxWrite(this, ev), ctx);
    } else if (InsertTable->IsOverloaded(tableId)) {
        LOG_S_INFO("Write (overload) " << data.size() << " bytes for table " << tableId << " at tablet " << TabletID());

        ev->Get()->PutStatus = NKikimrProto::TRYLATER;
        Execute(new TTxWrite(this, ev), ctx);
    } else if (ev->Get()->BlobId.IsValid()) {
        LOG_S_DEBUG("Write (record) " << data.size() << " bytes at tablet " << TabletID());

        Execute(new TTxWrite(this, ev), ctx);
    } else {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("Write (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->PutStatus = NKikimrProto::TRYLATER;
            Execute(new TTxWrite(this, ev), ctx);
        } else {
            LOG_S_DEBUG("Write (blob) " << data.size() << " bytes at tablet " << TabletID());

            ev->Get()->MaxSmallBlobSize = Settings.MaxSmallBlobSize;

            ctx.Register(CreateWriteActor(TabletID(), PrimaryIndex->GetIndexInfo(), ctx.SelfID,
                BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
        }
    }
}

}
