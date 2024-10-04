#include "tx_blobs_written.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/insert_table/user_data.h>
#include <ydb/core/tx/columnshard/transactions/locks/write.h>

namespace NKikimr::NColumnShard {

bool TTxBlobsWritingFinished::DoExecute(TTransactionContext& txc, const TActorContext&) {
    TMemoryProfileGuard mpg("TTxBlobsWritingFinished::Execute");
    txc.DB.NoMoreReadsForTx();
    CommitSnapshot = NOlap::TSnapshot::MaxForPlanStep(Self->GetOutdatedStep());
    NActors::TLogContextGuard logGuard =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "execute");
    ACFL_DEBUG("event", "start_execute");
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    for (auto&& portion : Portions) {
        const auto& writeMeta = portion.GetWriteMeta();
        AFL_VERIFY(Self->TablesManager.IsReadyForWrite(writeMeta.GetTableId()));
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto operation = Self->OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        auto& granule = index.MutableGranuleVerified(portion.GetPortionInfoConstructor()->GetPathId());
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            static TAtomicCounter Counter = 0;
            portion.GetPortionInfoConstructor()->SetInsertWriteId((TInsertWriteId)Counter.Inc());
        } else {
            portion.GetPortionInfoConstructor()->SetInsertWriteId(Self->InsertTable->BuildNextWriteId(txc));
        }
        portion.Finalize(Self, txc);
        if (operation->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            granule.CommitImmediateOnExecute(txc, *CommitSnapshot, portion.GetPortionInfo());
        } else {
            granule.InsertPortionOnExecute(txc, portion.GetPortionInfo());
        }
    }

    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    WritingActions->OnExecuteTxAfterWrite(*Self, blobManagerDb, true);
    Results.clear();
    std::set<TOperationWriteId> operationIds;
    for (auto&& portion : Portions) {
        const auto& writeMeta = portion.GetWriteMeta();
        auto operation = Self->OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (!operationIds.emplace(operation->GetWriteId()).second) {
            continue;
        }
        Y_ABORT_UNLESS(operation->GetStatus() == EOperationStatus::Started);
        const std::vector<TInsertWriteId> insertWriteIds = { portion.GetPortionInfo()->GetInsertWriteIdVerified() };
        operation->OnWriteFinish(txc, insertWriteIds, operation->GetBehaviour() == EOperationBehaviour::NoTxWrite);
        Self->OperationsManager->LinkInsertWriteIdToOperationWriteId(insertWriteIds, operation->GetWriteId());
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
    WritingActions->OnCompleteTxAfterWrite(*Self, true);

    for (auto&& i : Results) {
        i.DoSendReply(ctx);
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    for (auto&& portion : Portions) {
        const auto& writeMeta = portion.GetWriteMeta();
        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto op = Self->GetOperationsManager().GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        if (op->GetBehaviour() == EOperationBehaviour::WriteWithLock || op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            if (op->GetBehaviour() != EOperationBehaviour::NoTxWrite || Self->GetOperationsManager().HasReadLocks(writeMeta.GetTableId())) {
                auto evWrite = std::make_shared<NOlap::NTxInteractions::TEvWriteWriter>(
                    writeMeta.GetTableId(), portion.GetPKBatch(), Self->GetIndexOptional()->GetVersionedIndex().GetPrimaryKey());
                Self->GetOperationsManager().AddEventForLock(*Self, op->GetLockId(), evWrite);
            }
        }
        auto& granule = index.MutableGranuleVerified(portion.GetPortionInfo()->GetPathId());
        if (op->GetBehaviour() == EOperationBehaviour::NoTxWrite) {
            Self->OperationsManager->AddTemporaryTxLink(op->GetLockId());
            AFL_VERIFY(CommitSnapshot);
            granule.CommitImmediateOnComplete(portion.GetPortionInfo(), index);
        } else {
            granule.InsertPortionOnComplete(portion.GetPortionInfo());
        }

        Self->Counters.GetCSCounters().OnWriteTxComplete(now - writeMeta.GetWriteStartInstant());
        Self->Counters.GetCSCounters().OnSuccessWriteResponse();
    }
    Self->Counters.GetTabletCounters()->IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
    Self->SetupCompaction();
}

void TInsertedPortion::Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc) {
    auto* lastPortionId = shard->MutableIndexAs<NOlap::TColumnEngineForLogs>().GetLastPortionPointer();
    PortionInfoConstructor->SetPortionId(++*lastPortionId);
    NOlap::TDbWrapper wrapper(txc.DB, nullptr);
    wrapper.WriteCounter(NOlap::TColumnEngineForLogs::LAST_PORTION, *lastPortionId);
    PortionInfo = PortionInfoConstructor->BuildPtr(true);
}

}   // namespace NKikimr::NColumnShard
