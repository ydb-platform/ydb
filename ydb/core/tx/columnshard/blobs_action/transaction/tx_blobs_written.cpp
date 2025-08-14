#include "tx_blobs_written.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/common/statistics.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/columnshard/transactions/locks/write.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

bool TTxBlobsWritingFinished::DoExecute(TTransactionContext& txc, const TActorContext&) {
    TInstant startTransactionTime = TInstant::Now();
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
    const ui64 firstPKColumnId = Self->TablesManager.GetIndexInfo(*CommitSnapshot).GetPKFirstColumnId();
    for (auto&& portion : Pack.MutablePortions()) {
        AFL_VERIFY(portion.GetPortionInfoConstructor()->GetPortionConstructor().GetType() == NOlap::EPortionType::Written);
        auto* constructor =
            static_cast<NOlap::TWrittenPortionInfoConstructor*>(&portion.GetPortionInfoConstructor()->MutablePortionConstructor());
        constructor->SetInsertWriteId(granule.BuildNextInsertWriteId());
        InsertWriteIds.emplace_back(constructor->GetInsertWriteIdVerified());
        portion.Finalize(Self, txc);
        if (PackBehaviour == EOperationBehaviour::NoTxWrite) {
            granule.CommitImmediateOnExecute(txc, *CommitSnapshot, portion.GetPortionInfo(), firstPKColumnId);
        } else {
            granule.InsertPortionOnExecute(txc, portion.GetPortionInfoPtr(), firstPKColumnId);
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
        AFL_VERIFY(Self->TablesManager.IsReadyForFinishWrite(writeMeta.GetPathId().InternalPathId, minReadSnapshot));
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            operation->OnWriteFinish(txc, {}, true);
        } else {
            operation->OnWriteFinish(txc, InsertWriteIds, false);
            Self->OperationsManager->LinkInsertWriteIdToOperationWriteId(InsertWriteIds, operation->GetWriteId());
        }
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            LWPROBE(EvWriteResult, Self->TabletID(), writeMeta.GetSource().ToString(), 0, operation->GetCookie(), "no_tx_write", true, "");
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
            LWPROBE(EvWriteResult, Self->TabletID(), writeMeta.GetSource().ToString(), 0, operation->GetCookie(), "tx_write", true, "");
            auto ev = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(Self->TabletID(), operation->GetLockId(), lock);
            AddTableAccessStatsToTxStats(*ev->Record.MutableTxStats(), writeMeta.GetPathId().SchemeShardLocalPathId.GetRawValue(),
                                         writeResult.GetRecordsCount(), writeResult.GetDataSize(), operation->GetModificationType());
            Results.emplace_back(std::move(ev), writeMeta.GetSource(), operation->GetCookie());
        }
    }
    TransactionTime = TInstant::Now() - startTransactionTime;
    return true;
}

void TTxBlobsWritingFinished::DoComplete(const TActorContext& ctx) {
    TInstant startCompleteTime = TInstant::Now();
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
    std::set<TInternalPathId> pathIds;
    for (auto&& writeResult : Pack.GetWriteResults()) {
        const auto& writeMeta = writeResult.GetWriteMeta();
        writeMeta.OnStage(NEvWrite::EWriteStage::Replied);
        if (writeResult.GetNoDataToWrite()) {
            continue;
        }
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        pathIds.emplace(op->GetPathId().InternalPathId);
        if (op->GetBehaviour() == EOperationBehaviour::WriteWithLock || op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            if (op->GetBehaviour() != EOperationBehaviour::NoTxWrite || Self->GetOperationsManager().HasReadLocks(writeMeta.GetPathId().InternalPathId)) {
                auto evWrite = std::make_shared<NOlap::NTxInteractions::TEvWriteWriter>(
                    writeMeta.GetPathId().InternalPathId, writeResult.GetPKBatchVerified(), Self->GetIndexOptional()->GetVersionedIndex().GetPrimaryKey());
                Self->GetOperationsManager().AddEventForLock(*Self, op->GetLockId(), evWrite);
            }
        }
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    auto& granule = index.MutableGranuleVerified(Pack.GetPathId());
    for (auto&& portion : Pack.GetPortions()) {
        granule.InsertPortionOnComplete(portion.GetPortionInfoPtr(), index);
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
        writeMeta.OnStage(NEvWrite::EWriteStage::Finished);
        LWPROBE(TxBlobsWritingFinished, Self->TabletID(), TransactionTime, TInstant::Now() - startCompleteTime, TInstant::Now() - StartTime, now - writeMeta.GetWriteStartInstant());
    }
    Self->SetupCompaction(pathIds);
    TDuration completeTime = TInstant::Now() - startCompleteTime;
    LWPROBE(TTxBlobsWritingFinished, Self->TabletID(), TransactionTime, completeTime, TInstant::Now() - StartTime, TDuration::Zero());
}

TTxBlobsWritingFinished::TTxBlobsWritingFinished(TColumnShard* self, const NKikimrProto::EReplyStatus /* writeStatus */,
    const std::shared_ptr<NOlap::IBlobsWritingAction>& writingActions, TInsertedPortions&& pack)
    : TBase(self, "TTxBlobsWritingFinished")
    , Pack(std::move(pack))
    , WritingActions(writingActions)
    , StartTime(TInstant::Now()) {
}

bool TTxBlobsWritingFailed::DoExecute(TTransactionContext& txc, const TActorContext& /* ctx */) {
    for (auto&& wResult : Pack.GetWriteResults()) {
        const auto& writeMeta = wResult.GetWriteMeta();
        writeMeta.OnStage(NEvWrite::EWriteStage::Replied);
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Self->OperationsManager->AddTemporaryTxLink(op->GetLockId());
        Self->OperationsManager->AbortTransactionOnExecute(*Self, op->GetLockId(), txc);

        LWPROBE(EvWriteResult, Self->TabletID(), writeMeta.GetSource().ToString(), 0, op->GetCookie(), "tx_write", false, wResult.GetErrorMessage());
        auto ev = NEvents::TDataEvents::TEvWriteResult::BuildError(Self->TabletID(), op->GetLockId(),
            wResult.IsInternalError() ? NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR
                                      : NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
            wResult.GetErrorMessage());
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
        writeMeta.OnStage(NEvWrite::EWriteStage::Aborted);
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Self->OperationsManager->AbortTransactionOnComplete(*Self, op->GetLockId());
    }
}

}   // namespace NKikimr::NColumnShard
