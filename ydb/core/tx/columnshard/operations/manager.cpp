#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NColumnShard {

bool TOperationsManager::Load(NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    {
        auto rowset = db.Table<Schema::LockFeatures>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 lockId = rowset.GetValue<Schema::LockFeatures::LockId>();
            const ui64 generation = rowset.GetValueOrDefault<Schema::LockFeatures::Generation>(0);
            const ui64 counter = rowset.GetValueOrDefault<Schema::LockFeatures::InternalGenerationCounter>(0);

            AFL_VERIFY(LockFeatures.emplace(lockId, TLockFeatures(lockId, generation, counter)).second);

            if (!rowset.Next()) {
                return false;
            }
        }
    }
    {
        auto rowset = db.Table<Schema::Operations>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TWriteId writeId = (TWriteId)rowset.GetValue<Schema::Operations::WriteId>();
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
            AFL_VERIFY(operation->GetStatus() != EOperationStatus::Draft);

            AFL_VERIFY(Operations.emplace(operation->GetWriteId(), operation).second);
            auto it = LockFeatures.find(lockId);
            if (it == LockFeatures.end()) {
                it = LockFeatures.emplace(lockId, lockId).first;
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

bool TOperationsManager::CommitTransaction(
    TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId));
    auto lockId = GetLockForTx(txId);
    if (!lockId) {
        ACFL_ERROR("details", "unknown_transaction");
        return true;
    }
    auto tIt = LockFeatures.find(*lockId);
    AFL_VERIFY(tIt != LockFeatures.end())("tx_id", txId)("lock_id", *lockId);

    for (auto&& i : tIt->second.GetBrokeOnCommit()) {
        if (auto lock = GetLockOptional(i)) {
            for (auto&& brokenLockId : lock->GetBrokeOnCommit()) {
                if (auto lockBroken = GetLockOptional(brokenLockId)) {
                    lockBroken->SetBroken(true);
                }
            }
            for (auto&& brokenLockId : lock->GetNotifyOnCommit()) {
                if (auto lockBroken = GetLockOptional(brokenLockId)) {
                    lockBroken->AddNotifyCommit(lock->GetLockId());
                }
            }
        }
    }

    TVector<TWriteOperation::TPtr> commited;
    for (auto&& opPtr : tIt->second.GetWriteOperations()) {
        opPtr->Commit(owner, txc, snapshot);
        commited.emplace_back(opPtr);
    }
    OnTransactionFinish(commited, txId, txc);
    return true;
}

bool TOperationsManager::AbortTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId));

    auto lockId = GetLockForTx(txId);
    if (!lockId) {
        ACFL_ERROR("details", "unknown_transaction");
        return true;
    }
    auto tIt = LockFeatures.find(*lockId);
    AFL_VERIFY(tIt != LockFeatures.end())("tx_id", txId)("lock_id", *lockId);

    TVector<TWriteOperation::TPtr> aborted;
    for (auto&& opPtr : tIt->second.GetWriteOperations()) {
        opPtr->Abort(owner, txc);
        aborted.emplace_back(opPtr);
    }

    OnTransactionFinish(aborted, txId, txc);
    return true;
}

TWriteOperation::TPtr TOperationsManager::GetOperation(const TWriteId writeId) const {
    auto it = Operations.find(writeId);
    if (it == Operations.end()) {
        return nullptr;
    }
    return it->second;
}

void TOperationsManager::OnTransactionFinish(
    const TVector<TWriteOperation::TPtr>& operations, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    const ui64 lockId = GetLockForTxVerified(txId);
    auto itLock = LockFeatures.find(lockId);
    AFL_VERIFY(itLock != LockFeatures.end());
    itLock->second.RemoveInteractions(InteractionsContext);
    LockFeatures.erase(lockId);
    Tx2Lock.erase(txId);
    for (auto&& op : operations) {
        RemoveOperation(op, txc);
    }
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::OperationTxIds>().Key(txId, lockId).Delete();
}

void TOperationsManager::RemoveOperation(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc) {
    Operations.erase(op->GetWriteId());
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Operations>().Key((ui64)op->GetWriteId()).Delete();
}

TWriteId TOperationsManager::BuildNextWriteId() {
    return ++LastWriteId;
}

std::optional<ui64> TOperationsManager::GetLockForTx(const ui64 txId) const {
    auto lockIt = Tx2Lock.find(txId);
    if (lockIt != Tx2Lock.end()) {
        return lockIt->second;
    }
    return std::nullopt;
}

void TOperationsManager::LinkTransaction(const ui64 lockId, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    Tx2Lock[txId] = lockId;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::OperationTxIds>().Key(txId, lockId).Update();
}

TWriteOperation::TPtr TOperationsManager::RegisterOperation(
    const ui64 lockId, const ui64 cookie, const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType) {
    auto writeId = BuildNextWriteId();
    auto operation = std::make_shared<TWriteOperation>(
        writeId, lockId, cookie, EOperationStatus::Draft, AppData()->TimeProvider->Now(), granuleShardingVersionId, mType);
    Y_ABORT_UNLESS(Operations.emplace(operation->GetWriteId(), operation).second);
    GetLockVerified(operation->GetLockId()).MutableWriteOperations().emplace_back(operation);
    return operation;
}

EOperationBehaviour TOperationsManager::GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite) {
    if (evWrite.Record.HasTxId() && evWrite.Record.HasLocks() && evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit) {
        return EOperationBehaviour::CommitWriteLock;
    }

    if (evWrite.Record.HasLockTxId() && evWrite.Record.HasLockNodeId()) {
        if (evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
            return EOperationBehaviour::WriteWithLock;
        }

        return EOperationBehaviour::Undefined;
    }

    if (evWrite.Record.HasTxId() && evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_PREPARE) {
        return EOperationBehaviour::InTxWrite;
    }
    return EOperationBehaviour::Undefined;
}

TOperationsManager::TOperationsManager() {
}

void TOperationsManager::AddEventForTx(TColumnShard& /*owner*/, const ui64 txId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer) {
    AFL_VERIFY(writer);
    NOlap::NTxInteractions::TTxConflicts txNotifications;
    NOlap::NTxInteractions::TTxConflicts txConflicts;
    auto& txLock = GetLockVerified(GetLockForTxVerified(txId));
    writer->CheckInteraction(txId, InteractionsContext, txConflicts, txNotifications);
    for (auto&& i : txConflicts) {
        if (auto lock = GetLockOptional(i.first)) {
            GetLockVerified(i.first).AddBrokeOnCommit(i.second);
        } else if (txLock.IsCommitted(i.first)) {
            txLock.SetBroken(true);
        }
        
    }
    for (auto&& i : txNotifications) {
        GetLockVerified(i.first).AddNotificationsOnCommit(i.second);
    }
    NOlap::NTxInteractions::TTxEventContainer container(txId, writer->BuildEvent());
    container.AddToInteraction(InteractionsContext);
    GetLockVerified(GetLockForTxVerified(txId)).MutableEvents().emplace_back(std::move(container));
}

}   // namespace NKikimr::NColumnShard
