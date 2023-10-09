#include "tx_write.h"

namespace NKikimr::NColumnShard {
bool TTxWrite::InsertOneBlob(TTransactionContext& txc, const TEvPrivate::TEvWriteBlobsResult::TPutBlobData& blobData, const TWriteId writeId, const TString& blob) {
    const NKikimrTxColumnShard::TLogicalMetadata& meta = blobData.GetLogicalMeta();

    const auto& blobRange = blobData.GetBlobRange();
    Y_ABORT_UNLESS(blobRange.GetBlobId().IsValid());

    // First write wins
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    const auto& writeMeta(PutBlobResult->Get()->GetWriteMeta());

    auto tableSchema = Self->TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaUnsafe(PutBlobResult->Get()->GetSchemaVersion());

    NOlap::TInsertedData insertData((ui64)writeId, writeMeta.GetTableId(), writeMeta.GetDedupId(), blobRange, meta, tableSchema->GetVersion(), blob);
    bool ok = Self->InsertTable->Insert(dbTable, std::move(insertData));
    if (ok) {
        // Put new data into blob cache
        Y_ABORT_UNLESS(blobRange.IsFullBlob());

        Self->UpdateInsertTableCounters();
        return true;
    }
    return false;
}


bool TTxWrite::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "execute");
    ACFL_DEBUG("event", "start_execute");
    const auto& writeMeta(PutBlobResult->Get()->GetWriteMeta());
    Y_ABORT_UNLESS(Self->TablesManager.IsReadyForWrite(writeMeta.GetTableId()));

    txc.DB.NoMoreReadsForTx();
    TWriteOperation::TPtr operation;
    if (writeMeta.HasLongTxId()) {
        AFL_VERIFY(PutBlobResult->Get()->GetBlobData().size() == 1)("count", PutBlobResult->Get()->GetBlobData().size());
    } else {
        operation = Self->OperationsManager->GetOperation((TWriteId)writeMeta.GetWriteId());
        Y_ABORT_UNLESS(operation);
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
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

        if (!InsertOneBlob(txc, blobData, writeId, PutBlobResult->Get()->GetBlobVerified(blobData.GetBlobRange()))) {
            LOG_S_DEBUG(TxPrefix() << "duplicate writeId " << (ui64)writeId << TxSuffix());
            Self->IncCounter(COUNTER_WRITE_DUPLICATE);
        }
        writeIds.push_back(writeId);
    }

    TBlobManagerDb blobManagerDb(txc.DB);
    AFL_VERIFY(PutBlobResult->Get()->GetActions().size() == 1);
    AFL_VERIFY(PutBlobResult->Get()->GetActions().front()->GetBlobsCount() == PutBlobResult->Get()->GetBlobData().size());
    for (auto&& i : PutBlobResult->Get()->GetActions()) {
        i->OnExecuteTxAfterWrite(*Self, blobManagerDb, true);
    }

    if (operation) {
        operation->OnWriteFinish(txc, writeIds);
        auto txInfo = Self->ProgressTxController->RegisterTxWithDeadline(operation->GetTxId(), NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE, "", writeMeta.GetSource(), 0, txc);
        Y_UNUSED(txInfo);
        NEvents::TDataEvents::TCoordinatorInfo tInfo = Self->ProgressTxController->GetCoordinatorInfo(operation->GetTxId());
        Result = NEvents::TDataEvents::TEvWriteResult::BuildPrepared(operation->GetTxId(), tInfo);
    } else {
        Y_ABORT_UNLESS(writeIds.size() == 1);
        Result = std::make_unique<TEvColumnShard::TEvWriteResult>(Self->TabletID(), writeMeta, (ui64)writeIds.front(), NKikimrTxColumnShard::EResultStatus::SUCCESS);
    }
    return true;
}

void TTxWrite::Complete(const TActorContext& ctx) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "complete");
    AFL_VERIFY(Result);
    Self->CSCounters.OnWriteTxComplete((TMonotonic::Now() - PutBlobResult->Get()->GetWriteMeta().GetWriteStartInstant()).MilliSeconds());
    Self->CSCounters.OnSuccessWriteResponse();
    ctx.Send(PutBlobResult->Get()->GetWriteMeta().GetSource(), Result.release());
}

}
