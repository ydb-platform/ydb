#include "tx_blobs_written.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/insert_table/user_data.h>
#include <ydb/core/tx/columnshard/transactions/locks/write.h>

namespace NKikimr::NColumnShard {

bool TTxBlobsWritingFinished::DoExecute(TTransactionContext& txc, const TActorContext&) {
    TMemoryProfileGuard mpg("TTxBlobsWritingFinished::Execute");
    txc.DB.NoMoreReadsForTx();
    CommitSnapshot = Self->GetCurrentSnapshotForInternalModification();
    NActors::TLogContextGuard logGuard =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "execute");
    ACFL_DEBUG("event", "start_execute");
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    const auto minReadSnapshot = Self->GetMinReadSnapshot();

    for (auto&& writeResult : Pack.GetWriteResults()) {
        const auto& writeMeta = writeResult.GetWriteMeta();
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (!PackBehaviour) {
            PackBehaviour = op->GetBehaviour();
        } else {
            AFL_VERIFY(PackBehaviour == op->GetBehaviour());
        }
    }
    AFL_VERIFY(!!PackBehaviour);
    auto& granule = index.MutableGranuleVerified(Pack.GetPathId());
    for (auto&& portion : Pack.MutablePortions()) {
        if (PackBehaviour == EOperationBehaviour::NoTxWrite) {
            static TAtomicCounter Counter = 0;
            portion.GetPortionInfoConstructor()->MutablePortionConstructor().SetInsertWriteId((TInsertWriteId)Counter.Inc());
        } else {
            portion.GetPortionInfoConstructor()->MutablePortionConstructor().SetInsertWriteId(Self->InsertTable->BuildNextWriteId(txc));
        }
        InsertWriteIds.emplace_back(portion.GetPortionInfoConstructor()->GetPortionConstructor().GetInsertWriteIdVerified());
        portion.Finalize(Self, txc);
        if (PackBehaviour == EOperationBehaviour::NoTxWrite) {
            granule.CommitImmediateOnExecute(txc, *CommitSnapshot, portion.GetPortionInfo());
        } else {
            granule.InsertPortionOnExecute(txc, portion.GetPortionInfo());
        }
    }

    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    if (WritingActions) {
        WritingActions->OnExecuteTxAfterWrite(*Self, blobManagerDb, true);
    }
    std::set<TOperationWriteId> operationIds;
    for (auto&& writeResult : Pack.GetWriteResults()) {
        const auto& writeMeta = writeResult.GetWriteMeta();
        auto operation = Self->OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (!operationIds.emplace(operation->GetWriteId()).second) {
            continue;
        }
        AFL_VERIFY(Self->TablesManager.IsReadyForFinishWrite(writeMeta.GetTableId(), minReadSnapshot));
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            operation->OnWriteFinish(txc, {}, true);
        } else {
            operation->OnWriteFinish(txc, InsertWriteIds, false);
            Self->OperationsManager->LinkInsertWriteIdToOperationWriteId(InsertWriteIds, operation->GetWriteId());
        }
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
            lock.SetPathId(writeMeta.GetTableId());
            auto ev = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(Self->TabletID(), operation->GetLockId(), lock);
            Results.emplace_back(std::move(ev), writeMeta.GetSource(), operation->GetCookie());
        }
    }
    return true;
}

void TTxBlobsWritingFinished::DoComplete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxBlobsWritingFinished::Complete");
    NActors::TLogContextGuard logGuard =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "complete");
    const auto now = TMonotonic::Now();
    if (WritingActions) {
        WritingActions->OnCompleteTxAfterWrite(*Self, true);
    }

    for (auto&& i : Results) {
        i.DoSendReply(ctx);
    }
    std::set<ui64> pathIds;
    for (auto&& writeResult : Pack.GetWriteResults()) {
        if (writeResult.GetNoDataToWrite()) {
            continue;
        }
        const auto& writeMeta = writeResult.GetWriteMeta();
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        pathIds.emplace(op->GetPathId());
        if (op->GetBehaviour() == EOperationBehaviour::WriteWithLock || op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            if (op->GetBehaviour() != EOperationBehaviour::NoTxWrite || Self->GetOperationsManager().HasReadLocks(writeMeta.GetTableId())) {
                auto evWrite = std::make_shared<NOlap::NTxInteractions::TEvWriteWriter>(
                    writeMeta.GetTableId(), writeResult.GetPKBatchVerified(), Self->GetIndexOptional()->GetVersionedIndex().GetPrimaryKey());
                Self->GetOperationsManager().AddEventForLock(*Self, op->GetLockId(), evWrite);
            }
        }
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    auto& granule = index.MutableGranuleVerified(Pack.GetPathId());
    for (auto&& portion : Pack.GetPortions()) {
        granule.InsertPortionOnComplete(portion.GetPortionInfo(), index);
    }
    if (PackBehaviour == EOperationBehaviour::NoTxWrite) {
        for (auto&& i : InsertWriteIds) {
            granule.CommitPortionOnComplete(i, index);
        }
    }
    for (auto&& writeResult : Pack.GetWriteResults()) {
        const auto& writeMeta = writeResult.GetWriteMeta();
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            AFL_VERIFY(CommitSnapshot);
            Self->OperationsManager->AddTemporaryTxLink(op->GetLockId());
            Self->OperationsManager->CommitTransactionOnComplete(*Self, op->GetLockId(), *CommitSnapshot);
            Self->Counters.GetTabletCounters()->IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
        }
        Self->Counters.GetCSCounters().OnWriteTxComplete(now - writeMeta.GetWriteStartInstant());
        Self->Counters.GetCSCounters().OnSuccessWriteResponse();
    }
    Self->SetupCompaction(pathIds);
}

TTxBlobsWritingFinished::TTxBlobsWritingFinished(TColumnShard* self, const NKikimrProto::EReplyStatus /* writeStatus */,
    const std::shared_ptr<NOlap::IBlobsWritingAction>& writingActions, TInsertedPortions&& pack)
    : TBase(self, "TTxBlobsWritingFinished")
    , Pack(std::move(pack))
    , WritingActions(writingActions) {
}

bool TTxBlobsWritingFailed::DoExecute(TTransactionContext& txc, const TActorContext& /* ctx */) {
    for (auto&& wResult : Pack.GetWriteResults()) {
        const auto& writeMeta = wResult.GetWriteMeta();
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Self->OperationsManager->AddTemporaryTxLink(op->GetLockId());
        Self->OperationsManager->AbortTransactionOnExecute(*Self, op->GetLockId(), txc);

        auto ev = NEvents::TDataEvents::TEvWriteResult::BuildError(Self->TabletID(), op->GetLockId(),
            NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, "cannot write blob: " + ::ToString(PutBlobResult));
        Results.emplace_back(std::move(ev), writeMeta.GetSource(), op->GetCookie());
    }
    return true;
}

void TTxBlobsWritingFailed::DoComplete(const TActorContext& ctx) {
    for (auto&& i : Results) {
        i.DoSendReply(ctx);
        Self->Counters.GetCSCounters().OnFailedWriteResponse(EWriteFailReason::PutBlob);
    }
    for (auto&& wResult : Pack.GetWriteResults()) {
        const auto& writeMeta = wResult.GetWriteMeta();
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Self->OperationsManager->AbortTransactionOnComplete(*Self, op->GetLockId());
    }
}

}   // namespace NKikimr::NColumnShard
