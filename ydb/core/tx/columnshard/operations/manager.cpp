#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NColumnShard {

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

            auto operation = std::make_shared<TWriteOperation>(
                writeId, lockId, cookie, status, TInstant::Seconds(createdAtSec), granuleShardingVersionId, NEvWrite::EModificationType::Upsert);
            operation->FromProto(metaProto);
            LinkInsertWriteIdToOperationWriteId(operation->GetInsertWriteIds(), operation->GetWriteId());
            AFL_VERIFY(operation->GetStatus() != EOperationStatus::Draft);

            AFL_VERIFY(Operations.emplace(operation->GetWriteId(), operation).second);
            auto it = LockFeatures.find(lockId);
            if (it == LockFeatures.end()) {
                it = LockFeatures.emplace(lockId, TLockFeatures(lockId, 0)).first;
            }
            it->second.MutableWriteOperations().emplace_back(operation);
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
            AFL_VERIFY(LockFeatures.contains(lockId))("lock_id", lockId);
            AFL_VERIFY(Tx2Lock.emplace(txId, lockId).second);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    return true;
}

void TOperationsManager::CommitTransactionOnExecute(
    TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) {
    auto& lock = GetLockFeaturesForTxVerified(txId);
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("commit_tx_id", txId)("commit_lock_id", lock.GetLockId()));
    TVector<TWriteOperation::TPtr> commited;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->CommitOnExecute(owner, txc, snapshot);
        commited.emplace_back(opPtr);
    }
    OnTransactionFinishOnExecute(commited, lock, txId, txc);
}

void TOperationsManager::CommitTransactionOnComplete(
    TColumnShard& owner, const ui64 txId, const NOlap::TSnapshot& snapshot) {
    auto& lock = GetLockFeaturesForTxVerified(txId);
    TLogContextGuard gLogging(
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("commit_tx_id", txId)("commit_lock_id", lock.GetLockId()));
    for (auto&& i : lock.GetBrokeOnCommit()) {
        if (auto lockNotify = GetLockOptional(i)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("broken_lock_id", i);
            lockNotify->SetBroken();
        }
    }

    for (auto&& i : lock.GetNotifyOnCommit()) {
        if (auto lockNotify = GetLockOptional(i)) {
            lockNotify->AddNotifyCommit(lock.GetLockId());
        }
    }

    TVector<TWriteOperation::TPtr> commited;
    for (auto&& opPtr : lock.GetWriteOperations()) {
        opPtr->CommitOnComplete(owner, snapshot);
        commited.emplace_back(opPtr);
    }
    OnTransactionFinishOnComplete(commited, lock, txId);
}

void TOperationsManager::AbortTransactionOnExecute(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto* lock = GetLockFeaturesForTxOptional(txId);
    if (!lock) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort")("tx_id", txId)("problem", "finished");
        return;
    }
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId)("lock_id", lock->GetLockId()));

    TVector<TWriteOperation::TPtr> aborted;
    for (auto&& opPtr : lock->GetWriteOperations()) {
        opPtr->AbortOnExecute(owner, txc);
        aborted.emplace_back(opPtr);
    }

    OnTransactionFinishOnExecute(aborted, *lock, txId, txc);
}

void TOperationsManager::AbortTransactionOnComplete(TColumnShard& owner, const ui64 txId) {
    auto* lock = GetLockFeaturesForTxOptional(txId);
    if (!lock) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort")("tx_id", txId)("problem", "finished");
        return;
    }
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId)("lock_id", lock->GetLockId()));

    TVector<TWriteOperation::TPtr> aborted;
    for (auto&& opPtr : lock->GetWriteOperations()) {
        opPtr->AbortOnComplete(owner);
        aborted.emplace_back(opPtr);
    }

    OnTransactionFinishOnComplete(aborted, *lock, txId);
}

TWriteOperation::TPtr TOperationsManager::GetOperation(const TOperationWriteId writeId) const {
    auto it = Operations.find(writeId);
    if (it == Operations.end()) {
        return nullptr;
    }
    return it->second;
}

void TOperationsManager::OnTransactionFinishOnExecute(
    const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    for (auto&& op : operations) {
        RemoveOperationOnExecute(op, txc);
    }
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::OperationTxIds>().Key(txId, lock.GetLockId()).Delete();
}

void TOperationsManager::OnTransactionFinishOnComplete(
    const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId) {
    {
        lock.RemoveInteractions(InteractionsContext);
        LockFeatures.erase(lock.GetLockId());
    }
    Tx2Lock.erase(txId);
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
        AFL_VERIFY(InsertWriteIdToOpWriteId.erase(i));
    }
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

void TOperationsManager::LinkTransactionOnExecute(const ui64 lockId, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::OperationTxIds>().Key(txId, lockId).Update();
    Tx2Lock[txId] = lockId;
}

void TOperationsManager::LinkTransactionOnComplete(const ui64 /*lockId*/, const ui64 /*txId*/) {
}

TWriteOperation::TPtr TOperationsManager::RegisterOperation(
    const ui64 lockId, const ui64 cookie, const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType) {
    auto writeId = BuildNextOperationWriteId();
    auto operation = std::make_shared<TWriteOperation>(
        writeId, lockId, cookie, EOperationStatus::Draft, AppData()->TimeProvider->Now(), granuleShardingVersionId, mType);
    Y_ABORT_UNLESS(Operations.emplace(operation->GetWriteId(), operation).second);
    GetLockVerified(operation->GetLockId()).MutableWriteOperations().emplace_back(operation);
    GetLockVerified(operation->GetLockId()).AddWrite();
    return operation;
}

TConclusion<EOperationBehaviour> TOperationsManager::GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite) {
    if (evWrite.Record.HasTxId() && evWrite.Record.HasLocks()) {
        if (evWrite.Record.GetLocks().GetLocks().size() < 1) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
            return TConclusionStatus::Fail("no locks in case tx/locks");
        }
        auto& baseLock = evWrite.Record.GetLocks().GetLocks()[0];
        for (auto&& i : evWrite.Record.GetLocks().GetLocks()) {
            if (i.GetLockId() != baseLock.GetLockId()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock ids in operation");
            }
            if (i.GetGeneration() != baseLock.GetGeneration()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock generations in operation");
            }
            if (i.GetCounter() != baseLock.GetCounter()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
                return TConclusionStatus::Fail("different lock generation counters in operation");
            }
        }
        if (evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit) {
            return EOperationBehaviour::CommitWriteLock;
        }
        if (evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback) {
            return EOperationBehaviour::AbortWriteLock;
        }
    }

    if (evWrite.Record.HasLockTxId() && evWrite.Record.HasLockNodeId()) {
        if (evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
            return EOperationBehaviour::WriteWithLock;
        }

        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
        return TConclusionStatus::Fail("mode not IMMEDIATE for LockTxId + LockNodeId");
    }

    if (!evWrite.Record.HasLockTxId() && !evWrite.Record.HasLockNodeId() &&
        evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
        return EOperationBehaviour::NoTxWrite;
    }

    if (evWrite.Record.HasTxId() && evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_PREPARE) {
        return EOperationBehaviour::InTxWrite;
    }
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("proto", evWrite.Record.DebugString())("event", "undefined behaviour");
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
    for (auto&& i : txConflicts) {
        if (auto lock = GetLockOptional(i.first)) {
            GetLockVerified(i.first).AddBrokeOnCommit(i.second);
        } else if (txLock.IsCommitted(i.first)) {
            txLock.SetBroken();
        }
    }
    for (auto&& i : txNotifications) {
        GetLockVerified(i.first).AddNotificationsOnCommit(i.second);
    }
    if (auto txEvent = writer->BuildEvent()) {
        NOlap::NTxInteractions::TTxEventContainer container(lockId, txEvent);
        container.AddToInteraction(InteractionsContext);
        txLock.MutableEvents().emplace_back(std::move(container));
    }
}

}   // namespace NKikimr::NColumnShard
