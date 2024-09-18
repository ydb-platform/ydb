#pragma once
#include "write.h"

#include <ydb/core/tx/columnshard/transactions/locks/abstract.h>
#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr::NOlap::NTxInteractions {
class TManager;
class TTxEventContainer;
class TInteractionsContext;
class ITxEventWriter;
}   // namespace NKikimr::NOlap::NTxInteractions

namespace NKikimr::NColumnShard {

class TColumnShard;
class TLockFeatures;

class TLockSharingInfo {
private:
    const ui64 LockId;
    const ui64 Generation;
    TAtomicCounter InternalGenerationCounter = 0;
    TAtomicCounter Broken = 0;
    TAtomicCounter WritesCounter = 0;
    friend class TLockFeatures;

public:
    ui64 GetLockId() const {
        return LockId;
    }
    ui64 GetGeneration() const {
        return Generation;
    }

    TLockSharingInfo(const ui64 lockId, const ui64 generation)
        : LockId(lockId)
        , Generation(generation) {
    }

    bool HasWrites() const {
        return WritesCounter.Val();
    }

    bool IsBroken() const {
        return Broken.Val();
    }

    ui64 GetCounter() const {
        return InternalGenerationCounter.Val();
    }
};

class TLockFeatures: TMoveOnly {
private:
    YDB_ACCESSOR_DEF(std::vector<TWriteOperation::TPtr>, WriteOperations);
    YDB_ACCESSOR_DEF(std::vector<NOlap::NTxInteractions::TTxEventContainer>, Events);
    YDB_ACCESSOR(ui64, LockId, 0);
    YDB_ACCESSOR(ui64, Generation, 0);
    std::shared_ptr<TLockSharingInfo> SharingInfo;

    YDB_READONLY_DEF(THashSet<ui64>, BrokeOnCommit);
    YDB_READONLY_DEF(THashSet<ui64>, NotifyOnCommit);
    YDB_READONLY_DEF(THashSet<ui64>, Committed);

public:
    const std::shared_ptr<TLockSharingInfo>& GetSharingInfo() const {
        return SharingInfo;
    }

    ui64 GetInternalGenerationCounter() const {
        return SharingInfo->GetCounter();
    }

    void AddWrite() {
        SharingInfo->WritesCounter.Inc();
    }

    void SetBroken() {
        SharingInfo->Broken = 1;
        SharingInfo->InternalGenerationCounter = (i64)TSysTables::TLocksTable::TLock::ESetErrors::ErrorBroken;
    }

    bool IsBroken() const {
        return SharingInfo->IsBroken();
    }

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

    TLockFeatures(const ui64 lockId, const ui64 gen)
        : LockId(lockId)
        , Generation(gen) {
        SharingInfo = std::make_shared<TLockSharingInfo>(lockId, gen);
    }
};

class TOperationsManager {
    NOlap::NTxInteractions::TInteractionsContext InteractionsContext;

    THashMap<ui64, ui64> Tx2Lock;
    THashMap<TInsertWriteId, TOperationWriteId> InsertWriteIdToOpWriteId;
    THashMap<ui64, TLockFeatures> LockFeatures;
    THashMap<TOperationWriteId, TWriteOperation::TPtr> Operations;
    TOperationWriteId LastWriteId = TOperationWriteId(0);

public:

    TWriteOperation::TPtr GetOperationByInsertWriteIdVerified(const TInsertWriteId insertWriteId) const {
        auto it = InsertWriteIdToOpWriteId.find(insertWriteId);
        AFL_VERIFY(it != InsertWriteIdToOpWriteId.end());
        return GetOperationVerified(it->second);
    }

    void LinkInsertWriteIdToOperationWriteId(const std::vector<TInsertWriteId>& insertions, const TOperationWriteId operationId) {
        for (auto&& i : insertions) {
            InsertWriteIdToOpWriteId.emplace(i, operationId);
        }
    }
    bool Load(NTabletFlatExecutor::TTransactionContext& txc);
    void AddEventForTx(TColumnShard& owner, const ui64 txId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);
    void AddEventForLock(TColumnShard& owner, const ui64 lockId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);

    TWriteOperation::TPtr GetOperation(const TOperationWriteId writeId) const;
    TWriteOperation::TPtr GetOperationVerified(const TOperationWriteId writeId) const {
        return TValidator::CheckNotNull(GetOperationOptional(writeId));
    }
    TWriteOperation::TPtr GetOperationOptional(const TOperationWriteId writeId) const {
        return GetOperation(writeId);
    }
    void CommitTransactionOnExecute(
        TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot);
    void CommitTransactionOnComplete(
        TColumnShard& owner, const ui64 txId, const NOlap::TSnapshot& snapshot);
    void AddTemporaryTxLink(const ui64 lockId) {
        AFL_VERIFY(Tx2Lock.emplace(lockId, lockId).second);
    }
    void LinkTransactionOnExecute(const ui64 lockId, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void LinkTransactionOnComplete(const ui64 lockId, const ui64 txId);
    void AbortTransactionOnExecute(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void AbortTransactionOnComplete(TColumnShard& owner, const ui64 txId);

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
    TLockFeatures& GetLockFeaturesForTxVerified(const ui64 txId) {
        auto lockId = GetLockForTxOptional(txId);
        AFL_VERIFY(lockId);
        return GetLockVerified(*lockId);
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
            LockFeatures.emplace(lockId, TLockFeatures(lockId, generationId));
            return true;
        }
    }
    static TConclusion<EOperationBehaviour> GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite);
    TLockFeatures& GetLockVerified(const ui64 lockId) {
        auto result = GetLockOptional(lockId);
        AFL_VERIFY(result)("lock_id", lockId);
        return *result;
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
    TOperationWriteId BuildNextOperationWriteId();
    void RemoveOperationOnExecute(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc);
    void RemoveOperationOnComplete(const TWriteOperation::TPtr& op);
    void OnTransactionFinishOnExecute(const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId,
        NTabletFlatExecutor::TTransactionContext& txc);
    void OnTransactionFinishOnComplete(const TVector<TWriteOperation::TPtr>& operations, const TLockFeatures& lock, const ui64 txId);
};
}   // namespace NKikimr::NColumnShard
