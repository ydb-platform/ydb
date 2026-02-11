#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NColumnShard {

void TLockFeatures::SetTxId(const ui64 txId) {
    AFL_VERIFY(!TxId || TxId == txId)("tx_id", txId)("lock_id", GetLockId())("tx_id_assigned", TxId);
    TxId = txId;
}
bool TLockFeatures::IsTxIdAssigned() const {
    return TxId != 0;
}
ui64 TLockFeatures::GetTxId() const {
    AFL_VERIFY(IsTxIdAssigned())("lock_id", GetLockId());
    return TxId;
}

bool TOperationsManager::Load(NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    {
        auto rowset = db.Table<Schema::Operations>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TOperationWriteId writeId = (TOperationWriteId)rowset.GetValue<Schema::Operations::WriteId>();
            const ui64 createdAtSec = rowset.GetValue<Schema::Operations::CreatedAt>();
            const ui64 lockId = rowset.GetValue<Schema::Operations::LockId>();
            const ui64 cookie = rowset.GetValueOrDefault<Schema::Operations::Cookie>(0);
            const TString metadata = rowset.GetValue<Schema::Operations::Metadata>();
            const EOperationStatus status = (EOperationStatus)rowset.GetValue<Schema::Operations::Status>();
            std::optional<ui32> granuleShardingVersionId;
            if (rowset.HaveValue<Schema::Operations::GranuleShardingVersionId>() &&
                rowset.GetValue<Schema::Operations::GranuleShardingVersionId>()) {
                granuleShardingVersionId = rowset.GetValue<Schema::Operations::GranuleShardingVersionId>();
            }

            NKikimrTxColumnShard::TInternalOperationData metaProto;
            Y_ABORT_UNLESS(metaProto.ParseFromString(metadata));

            auto operation = std::make_shared<TWriteOperation>(TUnifiedPathId{}, writeId, lockId, cookie, status, TInstant::Seconds(createdAtSec),
                granuleShardingVersionId, NEvWrite::EModificationType::Upsert, metaProto.GetIsBulk());
            operation->FromProto(metaProto);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "register_operation_on_load")("operation_id", operation->GetWriteId());
            AFL_VERIFY(operation->GetStatus() != EOperationStatus::Draft);
            AFL_VERIFY(Operations.emplace(operation->GetWriteId(), operation).second);
            LinkInsertWriteIdToOperationWriteId(operation->GetInsertWriteIds(), operation->GetWriteId());

            auto it = LockFeatures.try_emplace(lockId, lockId, 0).first;
            it->second.AddWriteOperation(operation);
            // all the operations are finished at the moment of transaction proposal (or later) 
            it->second.OnWriteOperationFinished();
            LastWriteId = std::max(LastWriteId, operation->GetWriteId());
            if (!rowset.Next()) {
                return false;
            }
        }
    }
    {
        auto rowset = db.Table<Schema::OperationTxIds>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 lockId = rowset.GetValue<Schema::OperationTxIds::LockId>();
            const ui64 txId = rowset.GetValue<Schema::OperationTxIds::TxId>();
            const bool broken = rowset.GetValueOrDefault<Schema::OperationTxIds::Broken>(true);

            auto it = LockFeatures.try_emplace(lockId, lockId, 0).first;
            auto& lock = it->second;

            lock.SetTxId(txId);
            if (broken) {
                lock.SetBroken();
            }
            AFL_VERIFY(Tx2Lock.emplace(txId, lockId).second);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    return true;
}

void TOperationsManager::BreakConflictingTxs(const TLockFeatures& lock, NTabletFlatExecutor::TTransactionContext& txc) {
    for (auto&& lockIdToBreak : lock.GetBreakOnCommit()) {
        if (auto lockToBreak = GetLockOptional(lockIdToBreak)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("broken_lock_id", lockIdToBreak);
            if (!lockToBreak->IsBroken()) {
                lockToBreak->SetBroken();
                if (lockToBreak->IsTxIdAssigned()) {
                    PersistLock(*lockToBreak, txc);
                }
            }
        }
    }
    for (auto&& lockIdToNotify : lock.GetNotifyOnCommit()) {
        if (auto lockToNotify = GetLockOptional(lockIdToNotify)) {
            lockToNotify->NotifyAboutCommit(lock.GetLockId());
        }
    }
}

void TOperationsManager::BreakConflictingTxs(const ui64 lockId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto& lock = GetLockVerified(lockId);
    BreakConflictingTxs(lock, txc);
}

void TOperationsManager::CommitTransactionOnExecute(
    TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) {
    auto& lock = GetLockFeaturesForTxVerified(txId);
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)("commit_tx_id", txId)("commit_lock_id", lock.GetLockId()));
    TVector<TWriteOperation::TPtr> commited;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->CommitOnExecute(owner, txc, snapshot);
        commited.emplace_back(opPtr);
    }

    BreakConflictingTxs(lock, txc);

    OnTransactionFinishOnExecute(commited, lock, txId, txc);
}

void TOperationsManager::CommitTransactionOnComplete(
    TColumnShard& owner, const ui64 txId, const ui64 lockId, const NOlap::TSnapshot& snapshot) {
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)("commit_tx_id", txId)("commit_lock_id", lockId));

    auto& lock = GetLockVerified(lockId);

    TVector<TWriteOperation::TPtr> commited;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->CommitOnComplete(owner, snapshot);
        commited.emplace_back(opPtr);
    }
    OnTransactionFinishOnComplete(commited, lock, txId);
}

void TOperationsManager::AbortTransactionOnExecute(TColumnShard& owner, const ui64 txId, const ui64 lockId, NTabletFlatExecutor::TTransactionContext& txc) {
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)("tx_id", txId)("lock_id", lockId));

    auto& lock = GetLockVerified(lockId);
    lock.SetAborted(txId);

    TVector<TWriteOperation::TPtr> aborted;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->AbortOnExecute(owner, txc);
        aborted.emplace_back(opPtr);
    }

    OnTransactionFinishOnExecute(aborted, lock, txId, txc);
}

void TOperationsManager::AbortTransactionOnComplete(TColumnShard& owner, const ui64 txId, const ui64 lockId) {
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)("tx_id", txId)("lock_id", lockId));

    auto& lock = GetLockVerified(lockId);
    AFL_VERIFY(lock.IsAborted())("lock_id", lockId)("tx_id", txId);

    TVector<TWriteOperation::TPtr> aborted;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->AbortOnComplete(owner);
        aborted.emplace_back(opPtr);
    }

    OnTransactionFinishOnComplete(aborted, lock, txId);
}

void TOperationsManager::OnTransactionFinishOnExecute(
    const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    for (auto&& op : operations) {
        RemoveOperationOnExecute(op, txc);
    }
    if (txId != 0) {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::OperationTxIds>().Key(txId, lock.GetLockId()).Delete();
    }
}

void TOperationsManager::OnTransactionFinishOnComplete(
    const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId) {
    {
        lock.RemoveInteractions(InteractionsContext);
        LockFeatures.erase(lock.GetLockId());
    }
    if (txId != 0) {
        Tx2Lock.erase(txId);
    }
    for (auto&& op : operations) {
        RemoveOperationOnComplete(op);
    }
}

void TOperationsManager::RemoveOperationOnExecute(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Operations>().Key((ui64)op->GetWriteId()).Delete();
}

void TOperationsManager::RemoveOperationOnComplete(const TWriteOperation::TPtr& op) {
    for (auto&& i : op->GetInsertWriteIds()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "remove_write_id_to_operation_id")("write_id", i)("operation_id", op->GetWriteId());
        AFL_VERIFY(InsertWriteIdToOpWriteId.erase(i));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "remove_operation")("operation_id", op->GetWriteId());
    Operations.erase(op->GetWriteId());
}

TOperationWriteId TOperationsManager::BuildNextOperationWriteId() {
    return ++LastWriteId;
}

std::optional<ui64> TOperationsManager::GetLockForTx(const ui64 txId) const {
    auto lockIt = Tx2Lock.find(txId);
    if (lockIt != Tx2Lock.end()) {
        return lockIt->second;
    }
    return std::nullopt;
}

void TOperationsManager::LinkTransactionOnExecute(TLockFeatures& lock, NTabletFlatExecutor::TTransactionContext& txc) {
    PersistLock(lock, txc);
    Tx2Lock[lock.GetTxId()] = lock.GetLockId();
}

void TOperationsManager::PersistLock(TLockFeatures& lock, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::OperationTxIds>()
        .Key(lock.GetTxId(), lock.GetLockId())
        .Update(NIceDb::TUpdate<Schema::OperationTxIds::Broken>(lock.IsBroken()));
}

void TOperationsManager::LinkTransactionOnComplete(const ui64 /*lockId*/, const ui64 /*txId*/) {
}

TWriteOperation::TPtr TOperationsManager::CreateWriteOperation(const TUnifiedPathId& pathId, const ui64 lockId, const ui64 cookie,
    const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType, const bool isBulk) {
    auto writeId = BuildNextOperationWriteId();
    auto operation = std::make_shared<TWriteOperation>(pathId, writeId, lockId, cookie, EOperationStatus::Draft, AppData()->TimeProvider->Now(),
        granuleShardingVersionId, mType, isBulk);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "register_operation")("operation_id", operation->GetWriteId())(
        "last", LastWriteId);
    AFL_VERIFY(Operations.emplace(operation->GetWriteId(), operation).second);
    GetLockVerified(operation->GetLockId()).AddWriteOperation(operation);
    return operation;
}

TConclusion<EOperationBehaviour> TOperationsManager::GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite) {
    if (evWrite.Record.HasLocks() && evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback) {
        //FIXME #23784
        // AFL_VERIFY_DEBUG(!evWrite.Record.HasTxId())("TxId", evWrite.Record.GetTxId());
        // if (evWrite.Record.HasTxId()) {
        //     return TConclusionStatus::Fail("Rollback TEvWrite shouldn't have TxId");
        // }

        AFL_VERIFY_DEBUG(evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE)("TxMode", evWrite.Record.GetTxMode());
        if (evWrite.Record.GetTxMode() != NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
            return TConclusionStatus::Fail("Rollback TEvWrite should be Immediate");
        }

        return EOperationBehaviour::AbortWriteLock;
    }

    if (evWrite.Record.HasTxId() && evWrite.Record.HasLocks()) {
        if (evWrite.Record.GetLocks().GetLocks().size() < 1) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
            return TConclusionStatus::Fail("no locks in case tx/locks");
        }
        auto& baseLock = evWrite.Record.GetLocks().GetLocks()[0];
        for (auto&& i : evWrite.Record.GetLocks().GetLocks()) {
            if (i.GetLockId() != baseLock.GetLockId()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock ids in operation");
            }
            if (i.GetGeneration() != baseLock.GetGeneration()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock generations in operation");
            }
            if (i.GetCounter() != baseLock.GetCounter()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock generation counters in operation");
            }
        }
        if (evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit) {
            return EOperationBehaviour::CommitWriteLock;
        }
    }

    if (evWrite.Record.HasLockTxId() && evWrite.Record.HasLockNodeId()) {
        if (evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
            return EOperationBehaviour::WriteWithLock;
        }

        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
        return TConclusionStatus::Fail("mode not IMMEDIATE for LockTxId + LockNodeId");
    }

    if (!evWrite.Record.HasLockTxId() && !evWrite.Record.HasLockNodeId() &&
        evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
        return EOperationBehaviour::NoTxWrite;
    }

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
    return TConclusionStatus::Fail("undefined request for detect tx type");
}

TOperationsManager::TOperationsManager() {
}

void TOperationsManager::AddEventForTx(TColumnShard& owner, const ui64 txId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer) {
    return AddEventForLock(owner, GetLockForTxVerified(txId), writer);
}

void TOperationsManager::AddEventForLock(
    TColumnShard& /*owner*/, const ui64 lockId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer) {
    AFL_VERIFY(writer);
    NOlap::NTxInteractions::TTxConflicts txNotifications;
    NOlap::NTxInteractions::TTxConflicts txConflicts;
    auto& txLock = GetLockVerified(lockId);
    writer->CheckInteraction(lockId, InteractionsContext, txConflicts, txNotifications);
    for (auto& [commitLockId, breakLockIds] : txConflicts) {
        // if commitLockId not found, it means the conflicting tx is already committed or aborted
        if (GetLockOptional(commitLockId)) {
            GetLockVerified(commitLockId).AddBreakOnCommit(breakLockIds);
        } 
        // if the conflicting tx is already committed, we cannot commit the given tx, so break its lock
        if (txLock.IsCommitted(commitLockId)) {
            txLock.SetBroken();
        }
    }
    for (auto& [commitLockId, lockIdsToNotify] : txNotifications) {
        GetLockVerified(commitLockId).AddNotifyOnCommit(lockIdsToNotify);
    }
    if (auto txEvent = writer->BuildEvent()) {
        NOlap::NTxInteractions::TTxEventContainer container(lockId, txEvent);
        container.AddToInteraction(InteractionsContext);
        txLock.AddTxEvent(std::move(container));
    }
}

void TOperationsManager::SetOperationFinished(const TOperationWriteId writeId) {
    auto operation = GetOperationVerified(writeId);
    auto& lock = GetLockVerified(operation->GetLockId());
    lock.OnWriteOperationFinished();
}

}   // namespace NKikimr::NColumnShard
