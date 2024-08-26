#pragma once
#include "write.h"
#include <ydb/core/tx/columnshard/transactions/locks/abstract.h>

namespace NKikimr::NOlap::NTxInteractions {
class TManager;
class TTxEventContainer;
class TInteractionsContext;
class ITxEventWriter;
}

namespace NKikimr::NColumnShard {

class TColumnShard;

class TLockFeatures: TMoveOnly {
private:
    YDB_ACCESSOR_DEF(std::vector<TWriteOperation::TPtr>, WriteOperations);
    YDB_ACCESSOR_DEF(std::vector<NOlap::NTxInteractions::TTxEventContainer>, Events);
    YDB_ACCESSOR(ui64, LockId, 0);
    YDB_ACCESSOR(ui64, Generation, 0);
    YDB_ACCESSOR(ui64, InternalGenerationCounter, 0);

    YDB_ACCESSOR_DEF(THashSet<ui64>, BrokeOnCommit);
    YDB_ACCESSOR_DEF(THashSet<ui64>, NotifyOnCommit);
    YDB_ACCESSOR_DEF(THashSet<ui64>, Committed);
    YDB_ACCESSOR(bool, Broken, false);

public:
    bool IsCommitted(const ui64 lockId) const {
        return Committed.contains(lockId);
    }

    void AddNotifyCommit(const ui64 lockId) {
        AFL_VERIFY(NotifyOnCommit.erase(lockId));
        Committed.emplace(lockId);
    }

    void AddBrokeOnCommit(const THashSet<ui64>& lockIds) {
        BrokeOnCommit.insert(lockIds.begin(), lockIds.end());
    }

    void AddNotificationsOnCommit(const THashSet<ui64>& lockIds) {
        NotifyOnCommit.insert(lockIds.begin(), lockIds.end());
    }

    void RemoveInteractions(NOlap::NTxInteractions::TInteractionsContext& context) const {
        for (auto&& i : Events) {
            i.RemoveFromInteraction(context);
        }
    }

    TLockFeatures(const ui64 lockId)
        : LockId(lockId) {
    }
    TLockFeatures(const ui64 lockId, const ui64 gen, const ui64 counter)
        : LockId(lockId)
        , Generation(gen)
        , InternalGenerationCounter(counter) {
    }
};

class TOperationsManager {
    NOlap::NTxInteractions::TInteractionsContext InteractionsContext;

    THashMap<ui64, ui64> Tx2Lock;
    THashMap<ui64, TLockFeatures> LockFeatures;
    THashMap<TWriteId, TWriteOperation::TPtr> Operations;
    TWriteId LastWriteId = TWriteId(0);

public:
    bool Load(NTabletFlatExecutor::TTransactionContext& txc);
    void AddEventForTx(TColumnShard& owner, const ui64 txId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);
    void AddEventForLock(TColumnShard& owner, const ui64 lockId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);

    TWriteOperation::TPtr GetOperation(const TWriteId writeId) const;
    TWriteOperation::TPtr GetOperationVerified(const TWriteId writeId) const {
        return TValidator::CheckNotNull(GetOperationOptional(writeId));
    }
    TWriteOperation::TPtr GetOperationOptional(const TWriteId writeId) const {
        return GetOperation(writeId);
    }
    bool CommitTransaction(
        TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot);
    bool AbortTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void LinkTransaction(const ui64 lockId, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    std::optional<ui64> GetLockForTx(const ui64 txId) const;
    std::optional<ui64> GetLockForTxOptional(const ui64 txId) const {
        return GetLockForTx(txId);
    }
    TLockFeatures* GetLockFeaturesForTxOptional(const ui64 txId) {
        auto lockId = GetLockForTxOptional(txId);
        if (!lockId) {
            return nullptr;
        }
        return &GetLockVerified(*lockId);
    }
    ui64 GetLockForTxVerified(const ui64 txId) const {
        auto result = GetLockForTxOptional(txId);
        AFL_VERIFY(result)("tx_id", txId);
        return *result;
    }

    TWriteOperation::TPtr RegisterOperation(
        const ui64 lockId, const ui64 cookie, const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType);
    bool RegisterLock(const ui64 lockId, const ui64 generationId) {
        if (LockFeatures.contains(lockId)) {
            return false;
        } else {
            static TAtomicCounter Counter = 0;
            LockFeatures.emplace(lockId, TLockFeatures(lockId, generationId, Counter.Inc()));
            return true;
        }
    }
    static EOperationBehaviour GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite);
    TLockFeatures& GetLockVerified(const ui64 lockId) {
        return *TValidator::CheckNotNull(GetLockOptional(lockId));
    }

    TLockFeatures* GetLockOptional(const ui64 lockId) {
        auto it = LockFeatures.find(lockId);
        if (it != LockFeatures.end()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

    TOperationsManager();

private:
    TWriteId BuildNextWriteId();
    void RemoveOperation(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc);
    void OnTransactionFinish(const TVector<TWriteOperation::TPtr>& operations, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
};
}   // namespace NKikimr::NColumnShard
