#include "tx_write.h"

#include <ydb/core/tx/columnshard/engines/insert_table/user_data.h>
#include <ydb/core/tx/columnshard/transactions/locks/write.h>

namespace NKikimr::NColumnShard {

bool TTxWrite::InsertOneBlob(TTransactionContext& txc, const NOlap::TWideSerializedBatch& batch, const TInsertWriteId writeId) {
    auto userData = batch.BuildInsertionUserData(*Self);
    NOlap::TInsertedData insertData(writeId, userData);

    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    bool ok = Self->InsertTable->Insert(dbTable, std::move(insertData));
    if (ok) {
        Self->UpdateInsertTableCounters();
        return true;
    }
    return false;
}

bool TTxWrite::CommitOneBlob(TTransactionContext& txc, const NOlap::TWideSerializedBatch& batch, const TInsertWriteId writeId) {
    auto userData = batch.BuildInsertionUserData(*Self);
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    AFL_VERIFY(CommitSnapshot);
    NOlap::TCommittedData commitData(userData, *CommitSnapshot, Self->Generation(), writeId);
    if (Self->TablesManager.HasTable(userData->GetPathId())) {
        auto counters = Self->InsertTable->CommitEphemeral(dbTable, std::move(commitData));
        Self->Counters.GetTabletCounters()->OnWriteCommitted(counters);
    }
    Self->UpdateInsertTableCounters();
    return true;
}

bool TTxWrite::DoExecute(TTransactionContext& txc, const TActorContext&) {
    CommitSnapshot = Self->GetCurrentSnapshotForInternalModification();
    TMemoryProfileGuard mpg("TTxWrite::Execute");
    NActors::TLogContextGuard logGuard =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "execute");
    ACFL_DEBUG("event", "start_execute");
    const NOlap::TWritingBuffer& buffer = PutBlobResult->Get()->MutableWritesBuffer();
    const auto minReadSnapshot = Self->GetMinReadSnapshot();
    for (auto&& aggr : buffer.GetAggregations()) {
        const auto& writeMeta = aggr->GetWriteMeta();
        Y_ABORT_UNLESS(Self->TablesManager.IsReadyForFinishWrite(writeMeta.GetPathId().InternalPathId, minReadSnapshot));
        txc.DB.NoMoreReadsForTx();
        TWriteOperation::TPtr operation;
        AFL_VERIFY(!writeMeta.HasLongTxId());
        operation = Self->OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        for (auto&& i : aggr->GetSplittedBlobs()) {
            if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
                static TAtomicCounter Counter = 0;
                const TInsertWriteId insertWriteId = (TInsertWriteId)Counter.Inc();
                AFL_VERIFY(CommitOneBlob(txc, i, insertWriteId))("write_id", writeMeta.GetWriteId())("insert_write_id", insertWriteId)(
                    "size", aggr->GetSplittedBlobs().size());
            } else {
                const TInsertWriteId insertWriteId = Self->InsertTable->BuildNextWriteId(txc);
                aggr->AddInsertWriteId(insertWriteId);
                AFL_VERIFY(InsertOneBlob(txc, i, insertWriteId))("write_id", writeMeta.GetWriteId())("insert_write_id", insertWriteId)(
                    "size", aggr->GetSplittedBlobs().size());
            }
        }
    }

    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    AFL_VERIFY(buffer.GetAddActions().size() == 1);
    for (auto&& i : buffer.GetAddActions()) {
        i->OnExecuteTxAfterWrite(*Self, blobManagerDb, true);
    }
    for (auto&& i : buffer.GetRemoveActions()) {
        i->OnExecuteTxAfterRemoving(blobManagerDb, true);
    }
    Results.clear();
    for (auto&& aggr : buffer.GetAggregations()) {
        const auto& writeMeta = aggr->GetWriteMeta();
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto operation = Self->OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        operation->OnWriteFinish(txc, aggr->GetInsertWriteIds(), operation->GetBehaviour() == EOperationBehaviour::NoTxWrite);
        Self->OperationsManager->LinkInsertWriteIdToOperationWriteId(aggr->GetInsertWriteIds(), operation->GetWriteId());
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            auto ev = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(Self->TabletID());
            Results.emplace_back(std::move(ev), writeMeta.GetSource(), operation->GetCookie());
        } else {
            auto& info = Self->OperationsManager->GetLockVerified(operation->GetLockId());
            NKikimrDataEvents::TLock lock;
            lock.SetLockId(operation->GetLockId());
            lock.SetDataShard(Self->TabletID());
            lock.SetGeneration(info.GetGeneration());
            lock.SetCounter(info.GetInternalGenerationCounter());
            writeMeta.GetPathId().SchemeShardLocalPathId.ToProto(lock);
            auto ev = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(Self->TabletID(), operation->GetLockId(), lock);
            Results.emplace_back(std::move(ev), writeMeta.GetSource(), operation->GetCookie());
        }
    }
    return true;
}

void TTxWrite::DoComplete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxWrite::Complete");
    NActors::TLogContextGuard logGuard =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "complete");
    const auto now = TMonotonic::Now();
    const NOlap::TWritingBuffer& buffer = PutBlobResult->Get()->MutableWritesBuffer();
    for (auto&& i : buffer.GetAddActions()) {
        i->OnCompleteTxAfterWrite(*Self, true);
    }
    for (auto&& i : buffer.GetRemoveActions()) {
        i->OnCompleteTxAfterRemoving(true);
    }

    AFL_VERIFY(buffer.GetAggregations().size() == Results.size());
    for (auto&& i : Results) {
        i.DoSendReply(ctx);
    }
    for (ui32 i = 0; i < buffer.GetAggregations().size(); ++i) {
        const auto& writeMeta = buffer.GetAggregations()[i]->GetWriteMeta();
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (op->GetBehaviour() == EOperationBehaviour::WriteWithLock || op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            if (op->GetBehaviour() != EOperationBehaviour::NoTxWrite || Self->GetOperationsManager().HasReadLocks(writeMeta.GetPathId().InternalPathId)) {
                auto evWrite = std::make_shared<NOlap::NTxInteractions::TEvWriteWriter>(writeMeta.GetPathId().InternalPathId,
                    buffer.GetAggregations()[i]->GetRecordBatch(), Self->GetIndexOptional()->GetVersionedIndex().GetPrimaryKey());
                Self->GetOperationsManager().AddEventForLock(*Self, op->GetLockId(), evWrite);
            }
        }
        if (op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            Self->OperationsManager->AddTemporaryTxLink(op->GetLockId());
            AFL_VERIFY(CommitSnapshot);
            Self->OperationsManager->CommitTransactionOnComplete(*Self, op->GetLockId(), *CommitSnapshot);
            Self->Counters.GetTabletCounters()->IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
        }
        Self->Counters.GetCSCounters().OnWriteTxComplete(now - writeMeta.GetWriteStartInstant());
        Self->Counters.GetCSCounters().OnSuccessWriteResponse();
    }
    Self->SetupIndexation();
}

}   // namespace NKikimr::NColumnShard
