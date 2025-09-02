#pragma once
#include "write.h"

#include <ydb/core/tx/columnshard/transactions/locks/abstract.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NTxInteractions {
class TManager;
class TTxEventContainer;
class TInteractionsContext;
class ITxEventWriter;
}   // namespace NKikimr::NOlap::NTxInteractions

namespace NKikimr::NColumnShard {

class TColumnShard;
class TLockFeatures;

class TLockSharingInfo: TMoveOnly {
private:
    const ui64 LockId;
    const ui64 Generation;
    std::atomic<bool> Broken = false;
    std::atomic<bool> Writes = false;
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
        return Writes;
    }

    bool IsBroken() const {
        return Broken;
    }

    ui64 GetInternalGenerationCounter() const {
        return IsBroken() ? TSysTables::TLocksTable::TLock::ESetErrors::ErrorBroken : 0;
    }
};

class TLockFeatures: TMoveOnly {
private:
    YDB_READONLY_DEF(std::vector<TWriteOperation::TPtr>, WriteOperations);
    YDB_READONLY_DEF(std::vector<NOlap::NTxInteractions::TTxEventContainer>, Events);
    std::shared_ptr<TLockSharingInfo> SharingInfo;

    YDB_READONLY_DEF(THashSet<ui64>, BrokeOnCommit);
    YDB_READONLY_DEF(THashSet<ui64>, NotifyOnCommit);
    YDB_READONLY_DEF(THashSet<ui64>, Committed);

public:
    ui64 GetLockId() const {
        return SharingInfo->GetLockId();
    }

    ui64 GetGeneration() const {
        return SharingInfo->GetGeneration();
    }

    const std::shared_ptr<TLockSharingInfo>& GetSharingInfo() const {
        return SharingInfo;
    }

    ui64 GetInternalGenerationCounter() const {
        return SharingInfo->GetInternalGenerationCounter();
    }


    void AddWriteOperation(const TWriteOperation::TPtr op) {
        WriteOperations.push_back(op);
        SharingInfo->Writes = true;
    }

    void AddTxEvent(NOlap::NTxInteractions::TTxEventContainer&& container) {
        Events.emplace_back(std::move(container));
    }

    void SetBroken() {
        SharingInfo->Broken = true;
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

    TLockFeatures(const ui64 lockId, const ui64 gen) {
        SharingInfo = std::make_shared<TLockSharingInfo>(lockId, gen);
    }
};

class IResolveWriteIdToLockId {
protected:
    virtual ~IResolveWriteIdToLockId() {
    }

public:
    virtual std::optional<ui64> ResolveWriteIdToLockId(const TInsertWriteId& writeId) const = 0;
};

class TOperationsManager: public IResolveWriteIdToLockId {
    NOlap::NTxInteractions::TInteractionsContext InteractionsContext;

    THashMap<ui64, ui64> Tx2Lock;
    THashMap<TInsertWriteId, TOperationWriteId> InsertWriteIdToOpWriteId;
    THashMap<ui64, TLockFeatures> LockFeatures;
    THashMap<TOperationWriteId, TWriteOperation::TPtr> Operations;
    TOperationWriteId LastWriteId = TOperationWriteId(0);

public:   //IResolveWriteIdToLockId
    virtual std::optional<ui64> ResolveWriteIdToLockId(const TInsertWriteId& writeId) const override {
        if (const auto operationWriteId = InsertWriteIdToOpWriteId.FindPtr(writeId)) {
            if (const auto* operation = Operations.FindPtr(*operationWriteId)) {
                return (*operation)->GetLockId();
            }
        }
        return std::nullopt;
    }

public:

    void StopWriting() {
        for (auto&& i : Operations) {
            i.second->StopWriting();
        }
    }

    TWriteOperation::TPtr GetOperationByInsertWriteIdVerified(const TInsertWriteId insertWriteId) const {
        auto it = InsertWriteIdToOpWriteId.find(insertWriteId);
        AFL_VERIFY(it != InsertWriteIdToOpWriteId.end())("write_id", insertWriteId);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "ask_by_insert_id")("write_id", insertWriteId)("operation_id", it->second);
        return GetOperationVerified(it->second);
    }

    void LinkInsertWriteIdToOperationWriteId(const std::vector<TInsertWriteId>& insertions, const TOperationWriteId operationId) {
        const auto op = GetOperationVerified(operationId);
        AFL_VERIFY(op->GetInsertWriteIds() == insertions)("operation_data", JoinSeq(", ", op->GetInsertWriteIds()))(
            "expected", JoinSeq(", ", insertions));
        for (auto&& i : insertions) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "add_by_insert_id")("id", i)("operation_id", operationId);
            InsertWriteIdToOpWriteId.emplace(i, operationId);
        }
    }
    bool Load(NTabletFlatExecutor::TTransactionContext& txc);
    void AddEventForTx(TColumnShard& owner, const ui64 txId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);
    void AddEventForLock(TColumnShard& owner, const ui64 lockId, const std::shared_ptr<NOlap::NTxInteractions::ITxEventWriter>& writer);

    TWriteOperation::TPtr GetOperationVerified(const TOperationWriteId writeId) const {
        auto result = GetOperationOptional(writeId);
        AFL_VERIFY(!!result)("op_id", writeId);
        return result;
    }
    TWriteOperation::TPtr GetOperationOptional(const TOperationWriteId writeId) const {
        auto it = Operations.find(writeId);
        if (it == Operations.end()) {
            return nullptr;
        }
        return it->second;
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

    TWriteOperation::TPtr CreateWriteOperation(const TUnifiedPathId& pathId, const ui64 lockId, const ui64 cookie, const std::optional<ui32> granuleShardingVersionId,
        const NEvWrite::EModificationType mType, const bool isBulk);
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

    bool HasReadLocks(const TInternalPathId pathId) const {
        return InteractionsContext.HasReadIntervals(pathId);
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
