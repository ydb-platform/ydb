#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "blob_cache.h"
#include "blobs_action/bs.h"
#include "operations/slice_builder.h"

#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxWriteDraft: public TTransactionBase<TColumnShard> {
private:
    const IWriteController::TPtr WriteController;
public:
    TTxWriteDraft(TColumnShard* self, const IWriteController::TPtr writeController)
        : TBase(self)
        , WriteController(writeController) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        TBlobManagerDb blobManagerDb(txc.DB);
        for (auto&& action : WriteController->GetBlobActions()) {
            action->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
        }
        return true;
    }
    void Complete(const TActorContext& ctx) override {
        for (auto&& action : WriteController->GetBlobActions()) {
            action->OnCompleteTxBeforeWrite(*Self);
        }
        ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
    }
    TTxType GetTxType() const override { return TXTYPE_WRITE_DRAFT; }
};

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

    bool InsertOneBlob(TTransactionContext& txc, const TEvPrivate::TEvWriteBlobsResult::TPutBlobData& blobData, const TWriteId writeId);

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWrite[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};

bool TTxWrite::InsertOneBlob(TTransactionContext& txc, const TEvPrivate::TEvWriteBlobsResult::TPutBlobData& blobData, const TWriteId writeId) {
    const NKikimrTxColumnShard::TLogicalMetadata& meta = blobData.GetLogicalMeta();

    const auto& blobRange = blobData.GetBlobRange();
    Y_VERIFY(blobRange.GetBlobId().IsValid());

    ui64 writeUnixTime = meta.GetDirtyWriteTimeSeconds();
    TInstant time = TInstant::Seconds(writeUnixTime);

    // First write wins
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    const auto& writeMeta(PutBlobResult->Get()->GetWriteMeta());

    auto tableSchema = Self->TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaUnsafe(PutBlobResult->Get()->GetSchemaVersion());

    NOlap::TInsertedData insertData((ui64)writeId, writeMeta.GetTableId(), writeMeta.GetDedupId(), blobRange, meta, tableSchema->GetSnapshot());
    bool ok = Self->InsertTable->Insert(dbTable, std::move(insertData));
    if (ok) {
        THashSet<TWriteId> writesToAbort = Self->InsertTable->OldWritesToAbort(time);
        NIceDb::TNiceDb db(txc.DB);
        Self->TryAbortWrites(db, dbTable, std::move(writesToAbort));

        // TODO: It leads to write+erase for aborted rows. Abort() inserts rows, EraseAborted() erases them.
        // It's not optimal but correct.
        TBlobManagerDb blobManagerDb(txc.DB);
        auto allAborted = Self->InsertTable->GetAborted(); // copy (src is modified in cycle)
        for (auto& [abortedWriteId, abortedData] : allAborted) {
            Self->InsertTable->EraseAborted(dbTable, abortedData);
            Y_VERIFY(blobRange.IsFullBlob());
            Self->BlobManager->DeleteBlob(abortedData.GetBlobRange().GetBlobId(), blobManagerDb);
        }

        // Put new data into blob cache
        Y_VERIFY(blobRange.IsFullBlob());

        Self->UpdateInsertTableCounters();
        return true;
    }
    return false;
}


bool TTxWrite::Execute(TTransactionContext& txc, const TActorContext&) {
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    const auto& writeMeta(PutBlobResult->Get()->GetWriteMeta());
    Y_VERIFY(Self->TablesManager.IsReadyForWrite(writeMeta.GetTableId()));

    txc.DB.NoMoreReadsForTx();
    TWriteOperation::TPtr operation;
    if (writeMeta.HasLongTxId()) {
        Y_VERIFY_S(PutBlobResult->Get()->GetBlobData().size() == 1, TStringBuilder() << "Blobs count: " << PutBlobResult->Get()->GetBlobData().size());
    } else {
        operation = Self->OperationsManager.GetOperation((TWriteId)writeMeta.GetWriteId());
        Y_VERIFY(operation);
        Y_VERIFY(operation->GetStatus() == EOperationStatus::Started);
    }

    TVector<TWriteId> writeIds;
    for (auto blobData : PutBlobResult->Get()->GetBlobData()) {
        auto writeId = TWriteId(writeMeta.GetWriteId());
        if (operation) {
            writeId = Self->BuildNextWriteId(txc);
        } else {
            NIceDb::TNiceDb db(txc.DB);
            writeId = Self->GetLongTxWrite(db, writeMeta.GetLongTxIdUnsafe(), writeMeta.GetWritePartId());
        }

        if (!InsertOneBlob(txc, blobData, writeId)) {
            LOG_S_DEBUG(TxPrefix() << "duplicate writeId " << (ui64)writeId << TxSuffix());
            Self->IncCounter(COUNTER_WRITE_DUPLICATE);
        }
        writeIds.push_back(writeId);
    }

    TBlobManagerDb blobManagerDb(txc.DB);
    for (auto&& i : PutBlobResult->Get()->GetActions()) {
        i->OnExecuteTxAfterWrite(*Self, blobManagerDb);
    }

    if (operation) {
        operation->OnWriteFinish(txc, writeIds);
        auto txInfo = Self->ProgressTxController.RegisterTxWithDeadline(operation->GetTxId(), NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE, "", writeMeta.GetSource(), 0, txc);
        Y_UNUSED(txInfo);
        NEvents::TDataEvents::TCoordinatorInfo tInfo = Self->ProgressTxController.GetCoordinatorInfo(operation->GetTxId());
        Result = NEvents::TDataEvents::TEvWriteResult::BuildPrepared(operation->GetTxId(), tInfo);
    } else {
        Y_VERIFY(writeIds.size() == 1);
        Result = std::make_unique<TEvColumnShard::TEvWriteResult>(Self->TabletID(), writeMeta, (ui64)writeIds.front(), NKikimrTxColumnShard::EResultStatus::SUCCESS);
    }
    return true;
}

void TTxWrite::Complete(const TActorContext& ctx) {
    Y_VERIFY(Result);
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());
    Self->CSCounters.OnWriteTxComplete((TMonotonic::Now() - PutBlobResult->Get()->GetWriteMeta().GetWriteStartInstant()).MilliSeconds());
    Self->CSCounters.OnSuccessWriteResponse();
    ctx.Send(PutBlobResult->Get()->GetWriteMeta().GetSource(), Result.release());
}

void TColumnShard::OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteData& writeData, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx) {
    IncCounter(COUNTER_WRITE_FAIL);
    switch (overloadReason) {
        case EOverloadStatus::Disk:
            IncCounter(COUNTER_OUT_OF_SPACE);
            break;
        case EOverloadStatus::InsertTable:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadInsertTable(writeData.GetSize());
            break;
        case EOverloadStatus::Shard:
            IncCounter(COUNTER_WRITE_OVERLOAD);
            CSCounters.OnOverloadShard(writeData.GetSize());
            break;
        case EOverloadStatus::None:
            Y_FAIL("invalid function usage");
    }

    LOG_S_INFO("Write (overload) " << writeData.GetSize() << " bytes into pathId " << writeData.GetWriteMeta().GetTableId()
        << " overload reason: [" << overloadReason << "]"
        << " at tablet " << TabletID());

    ctx.Send(writeData.GetWriteMeta().GetSource(), event.release());
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloaded(const ui64 tableId) const {
    if (IsAnyChannelYellowStop()) {
        return EOverloadStatus::Disk;
    }

    if (InsertTable && InsertTable->IsOverloadedByCommitted(tableId)) {
        return EOverloadStatus::InsertTable;
    }

    if (WritesMonitor.ShardOverloaded()) {
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
        CSCounters.OnWritePutBlobsFail((TMonotonic::Now() - writeMeta.GetWriteStartInstant()).MilliSeconds());
        IncCounter(COUNTER_WRITE_FAIL);

        auto errCode = NKikimrTxColumnShard::EResultStatus::STORAGE_ERROR;
        if (putResult.GetPutStatus() == NKikimrProto::TIMEOUT || putResult.GetPutStatus() == NKikimrProto::DEADLINE) {
            errCode = NKikimrTxColumnShard::EResultStatus::TIMEOUT;
        } else if (putResult.GetPutStatus() == NKikimrProto::TRYLATER || putResult.GetPutStatus() == NKikimrProto::OUT_OF_SPACE) {
            errCode = NKikimrTxColumnShard::EResultStatus::OVERLOADED;
        } else if (putResult.GetPutStatus() == NKikimrProto::CORRUPTED) {
            errCode = NKikimrTxColumnShard::EResultStatus::ERROR;
        }

        if (writeMeta.HasLongTxId()) {
            auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, errCode);
            ctx.Send(writeMeta.GetSource(), result.release());
            CSCounters.OnFailedWriteResponse();
        } else {
            auto operation = OperationsManager.GetOperation((TWriteId)writeMeta.GetWriteId());
            Y_VERIFY(operation);
            auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(operation->GetTxId(), NKikimrDataEvents::TEvWriteResult::ERROR, "put data fails");
            ctx.Send(writeMeta.GetSource(), result.release());
            CSCounters.OnFailedWriteResponse();
        }
    } else {
        CSCounters.OnWritePutBlobsSuccess((TMonotonic::Now() - writeMeta.GetWriteStartInstant()).MilliSeconds());
        LOG_S_DEBUG("Write (record) into pathId " << writeMeta.GetTableId()
            << (writeMeta.GetWriteId() ? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : "") << " at tablet " << TabletID());

        Execute(new TTxWrite(this, ev), ctx);
    }
}

void TColumnShard::Handle(TEvPrivate::TEvWriteDraft::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteDraft(this, ev->Get()->WriteController), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    CSCounters.OnStartWriteRequest();
    LastAccessTime = TAppData::TimeProvider->Now();

    const auto& record = Proto(ev->Get());
    const ui64 tableId = record.GetTableId();
    const ui64 writeId = record.GetWriteId();
    const TString dedupId = record.GetDedupId();
    const auto source = ev->Sender;

    NEvWrite::TWriteMeta writeMeta(writeId, tableId, source);
    writeMeta.SetDedupId(dedupId);
    Y_VERIFY(record.HasLongTxId());
    writeMeta.SetLongTxId(NLongTxService::TLongTxId::FromProto(record.GetLongTxId()));
    writeMeta.SetWritePartId(record.GetWritePartId());

    if (!TablesManager.IsReadyForWrite(tableId)) {
        LOG_S_NOTICE("Write (fail) into pathId:" << writeMeta.GetTableId() << (TablesManager.HasPrimaryIndex()? "": " no index")
            << " at tablet " << TabletID());
        IncCounter(COUNTER_WRITE_FAIL);

        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR);
        ctx.Send(source, result.release());
        CSCounters.OnFailedWriteResponse();
        return;
    }

    const auto& snapshotSchema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
    auto arrowData = std::make_shared<TProtoArrowData>(snapshotSchema);
    if (!arrowData->ParseFromProto(record)) {
        LOG_S_ERROR("Write (fail) " << record.GetData().size() << " bytes into pathId " << writeMeta.GetTableId()
            << " at tablet " << TabletID());
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeMeta, NKikimrTxColumnShard::EResultStatus::ERROR);
        ctx.Send(source, result.release());
        CSCounters.OnFailedWriteResponse();
        return;
    }

    NEvWrite::TWriteData writeData(writeMeta, arrowData);
    auto overloadStatus = CheckOverloaded(tableId);
    if (overloadStatus != EOverloadStatus::None) {
        std::unique_ptr<NActors::IEventBase> result = std::make_unique<TEvColumnShard::TEvWriteResult>(TabletID(), writeData.GetWriteMeta(), NKikimrTxColumnShard::EResultStatus::OVERLOADED);
        OverloadWriteFail(overloadStatus, writeData, std::move(result), ctx);
        CSCounters.OnFailedWriteResponse();
    } else {
        if (ui64 writeId = (ui64) HasLongTxWrite(writeMeta.GetLongTxIdUnsafe(), writeMeta.GetWritePartId())) {
            LOG_S_DEBUG("Write (duplicate) into pathId " << writeMeta.GetTableId()
                << " longTx " << writeMeta.GetLongTxIdUnsafe().ToString()
                << " at tablet " << TabletID());

            IncCounter(COUNTER_WRITE_DUPLICATE);

            auto result = std::make_unique<TEvColumnShard::TEvWriteResult>(
                TabletID(), writeMeta, writeId, NKikimrTxColumnShard::EResultStatus::SUCCESS);
            ctx.Send(writeMeta.GetSource(), result.release());
            CSCounters.OnFailedWriteResponse();
            return;
        }

        WritesMonitor.RegisterWrite(writeData.GetSize());

        LOG_S_DEBUG("Write (blob) " << writeData.GetSize() << " bytes into pathId " << writeMeta.GetTableId()
            << (writeMeta.GetWriteId()? (" writeId " + ToString(writeMeta.GetWriteId())).c_str() : " ")
            << WritesMonitor.DebugString()
            << " at tablet " << TabletID());

        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(TabletID(), SelfId(), std::make_shared<NOlap::TBSWriteAction>(*BlobManager), writeData);
        NConveyor::TCompServiceOperator::SendTaskToExecute(task);
    }
}

void TColumnShard::Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const auto& tableId = record.GetTableId().GetTableId();
    const ui64 txId = ev->Get()->GetTxId();
    const auto source = ev->Sender;

    if (!record.GetTableId().HasSchemaVersion()) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(txId, NKikimrDataEvents::TEvWriteResult::BAD_REQUEST, "schema version not set");
        ctx.Send(source, result.release());
        return;
    }

    auto schema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchema(record.GetTableId().GetSchemaVersion());
    if (!schema) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(txId, NKikimrDataEvents::TEvWriteResult::BAD_REQUEST, "unknown schema version");
        ctx.Send(source, result.release());
        return;
    }

    if (!TablesManager.IsReadyForWrite(tableId)) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(txId, NKikimrDataEvents::TEvWriteResult::ERROR, "table not writable");
        ctx.Send(source, result.release());
        return;
    }

    auto arrowData = std::make_shared<TArrowData>(schema);
    if (!arrowData->Parse(record.GetReplace(), TPayloadHelper<NEvents::TDataEvents::TEvWrite>(*ev->Get()))) {
        IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(txId, NKikimrDataEvents::TEvWriteResult::BAD_REQUEST, "parsing data error");
        ctx.Send(source, result.release());
    }

    auto overloadStatus = CheckOverloaded(tableId);
    if (overloadStatus != EOverloadStatus::None) {
        NEvWrite::TWriteData writeData(NEvWrite::TWriteMeta(0, tableId, source), arrowData);
        std::unique_ptr<NActors::IEventBase> result = NEvents::TDataEvents::TEvWriteResult::BuildError(txId, NKikimrDataEvents::TEvWriteResult::OVERLOADED, "overload data error");
        OverloadWriteFail(overloadStatus, writeData, std::move(result), ctx);
        return;
    }

    auto wg = WritesMonitor.RegisterWrite(arrowData->GetData().size());
    auto operation = OperationsManager.RegisterOperation(txId);
    Y_VERIFY(operation);
    operation->Start(*this, tableId, arrowData, source, ctx);
}

}
