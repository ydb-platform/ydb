#include "kqp_tx_manager.h"

#include <algorithm>
#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr {
namespace NKqp {

namespace {

struct TKqpLock {
    using TKey = std::tuple<ui64, ui64, ui64, ui64>;
    TKey GetKey() const { return std::make_tuple(Proto.GetLockId(), Proto.GetDataShard(), Proto.GetSchemeShard(), Proto.GetPathId()); }

    bool Invalidated(const TKqpLock& newLock) const {
        AFL_ENSURE(GetKey() == newLock.GetKey());
        return Proto.GetGeneration() != newLock.Proto.GetGeneration() || Proto.GetCounter() != newLock.Proto.GetCounter();
    }

    TKqpLock(const NKikimrDataEvents::TLock& proto)
        : Proto(proto) {}

    NKikimrDataEvents::TLock Proto;
};

class TKqpTransactionManager : public IKqpTransactionManager {
    enum ETransactionState {
        COLLECTING,
        PREPARING,
        EXECUTING,   
    };
public:
    TKqpTransactionManager(bool collectOnly)
        : CollectOnly(collectOnly) {}

    void AddShard(ui64 shardId, bool isOlap, const TString& path) override {
        Y_ABORT_UNLESS(State == ETransactionState::COLLECTING);
        ShardsIds.insert(shardId);
        auto& shardInfo = ShardsInfo[shardId];
        shardInfo.IsOlap = isOlap;
        HasOlapTableShard |= isOlap;

        const auto [stringsIter, _] = TablePathes.insert(path);
        const TStringBuf pathBuf = *stringsIter;
        shardInfo.Pathes.insert(pathBuf);
    }

    void AddAction(ui64 shardId, ui8 action) override {
        Y_ABORT_UNLESS(State == ETransactionState::COLLECTING);
        ShardsInfo.at(shardId).Flags |= action;
        if (action & EAction::WRITE) {
            ReadOnly = false;
        }
    }

    void AddTopic(ui64 topicId, const TString& path) override {
        Y_ABORT_UNLESS(State == ETransactionState::COLLECTING);
        ShardsIds.insert(topicId);
        auto& shardInfo = ShardsInfo[topicId];

        const auto [stringsIter, _] = TablePathes.insert(path);
        const TStringBuf pathBuf = *stringsIter;
        shardInfo.Pathes.insert(pathBuf);
    }

    void AddTopicsToShards() override {
        if (!HasTopics()) {
            return;
        }

        for (auto& topicId : GetTopicOperations().GetSendingTabletIds()) {
            AddTopic(topicId, *GetTopicOperations().GetTabletName(topicId));
            AddAction(topicId, EAction::READ);
        }

        for (auto& topicId : GetTopicOperations().GetReceivingTabletIds()) {
            AddTopic(topicId, *GetTopicOperations().GetTabletName(topicId));
            AddAction(topicId, EAction::WRITE);
        }
    }

    bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lockProto) override {
        Y_ABORT_UNLESS(State == ETransactionState::COLLECTING);
        TKqpLock lock(lockProto);
        bool isError = (lock.Proto.GetCounter() >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin);
        bool isInvalidated = (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken)
                            || (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken);
        bool isLocksAcquireFailure = isError && !isInvalidated;
        bool broken = false;

        auto& shardInfo = ShardsInfo.at(shardId);
        if (auto lockPtr = shardInfo.Locks.FindPtr(lock.GetKey()); lockPtr) {
            if (lock.Proto.GetHasWrites()) {
                lockPtr->Lock.Proto.SetHasWrites(true);
            }

            lockPtr->LocksAcquireFailure |= isLocksAcquireFailure;
            if (!lockPtr->LocksAcquireFailure) {
                isInvalidated |= lockPtr->Lock.Invalidated(lock);
                lockPtr->Invalidated |= isInvalidated;
            }
            broken = lockPtr->Invalidated || lockPtr->LocksAcquireFailure;
        } else {
            shardInfo.Locks.emplace(
                lock.GetKey(),
                TShardInfo::TLockInfo {
                    .Lock = std::move(lock),
                    .Invalidated = isInvalidated,
                    .LocksAcquireFailure = isLocksAcquireFailure,
                });
            broken = isInvalidated || isLocksAcquireFailure;
        }

        if (broken && !LocksIssue) {
            if (isLocksAcquireFailure) {
                LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE);
                return false;
            } else if (isInvalidated) {
                MakeLocksIssue(shardInfo);
                return false;
            }
            AFL_ENSURE(false);
        }

        return true;
    }

    void BreakLock(ui64 shardId) override {
        if (LocksIssue) {
            return;
        }
        auto& shardInfo = ShardsInfo.at(shardId);
        MakeLocksIssue(shardInfo);
    }

    TTableInfo GetShardTableInfo(ui64 shardId) const override {
        const auto& info = ShardsInfo.at(shardId);
        return TTableInfo{
            .IsOlap = info.IsOlap,
            .Pathes = info.Pathes,
        };
    }

    EShardState GetState(ui64 shardId) const override {
        return ShardsInfo.at(shardId).State;
    }

    void SetError(ui64 shardId) override {
        auto& shardInfo = ShardsInfo.at(shardId);
        shardInfo.State = EShardState::ERROR;
    }

    void SetPartitioning(const TTableId tableId, const std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>& partitioning) override {
        TablePartitioning[tableId] = partitioning;
    }

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> GetPartitioning(const TTableId tableId) const override {
        auto iterator = TablePartitioning.find(tableId);
        if (iterator != std::end(TablePartitioning)) {
            return iterator->second;
        }
        return nullptr;
    }

    void SetTopicOperations(NTopic::TTopicOperations&& topicOperations) override {
        TopicOperations = std::move(topicOperations);
    }

    const NTopic::TTopicOperations& GetTopicOperations() const override {
        return TopicOperations;
    }

    void SetAllowVolatile(bool allowVolatile) override {
        AllowVolatile = allowVolatile;
    }

    void BuildTopicTxs(NTopic::TTopicOperationTransactions& txs) override {
        TopicOperations.BuildTopicTxs(txs);
    }

    bool HasTopics() const override {
        return GetTopicOperations().GetSize() != 0;
    }

    TVector<NKikimrDataEvents::TLock> GetLocks() const override {
        TVector<NKikimrDataEvents::TLock> locks;
        for (const auto& [_, shardInfo] : ShardsInfo) {
            for (const auto& [_, lockInfo] : shardInfo.Locks) {
                locks.push_back(lockInfo.Lock.Proto);
            }
        }
        return locks;
    }

    TVector<NKikimrDataEvents::TLock> GetLocks(ui64 shardId) const override {
        TVector<NKikimrDataEvents::TLock> locks;
        const auto& shardInfo = ShardsInfo.at(shardId);
        for (const auto& [_, lockInfo] : shardInfo.Locks) {
            locks.push_back(lockInfo.Lock.Proto);
        }
        return locks;
    }

    void Reattached(ui64 shardId) override {
        auto& shardInfo = ShardsInfo.at(shardId);
        shardInfo.Reattaching = false;
    }

    void SetRestarting(ui64 shardId) override {
        auto& shardInfo = ShardsInfo.at(shardId);
        shardInfo.Restarting = true;
    }

    bool ShouldReattach(ui64 shardId, TInstant now) override {
        auto& shardInfo = ShardsInfo.at(shardId);
        if (!std::exchange(shardInfo.Restarting, false) && !shardInfo.Reattaching) {
            return false;
        }
        return ::NKikimr::NKqp::ShouldReattach(now, shardInfo.ReattachState.ReattachInfo);;
    }

    TReattachState& GetReattachState(ui64 shardId) override {
        auto& shardInfo = ShardsInfo.at(shardId);
        return shardInfo.ReattachState;
    }

    bool IsTxPrepared() const override {
        for (const auto& [_, shardInfo] : ShardsInfo) {
            if (shardInfo.State != EShardState::PREPARED) {
                return false;
            }
        }
        return true;
    }

    bool IsTxFinished() const override {
        for (const auto& [_, shardInfo] : ShardsInfo) {
            if (shardInfo.State != EShardState::FINISHED) {
                return false;
            }
        }
        return true;
    }

    bool IsReadOnly() const override {
        return ReadOnly;
    }

    bool IsSingleShard() const override {
        return GetShardsCount() == 1;
    }

    bool HasOlapTable() const override {
        return HasOlapTableShard;
    }

    bool IsEmpty() const override {
        return GetShardsCount() == 0;
    }

    bool HasLocks() const override { 
        for (const auto& [_, shardInfo] : ShardsInfo) {
            if (!shardInfo.Locks.empty()) {
                return true;
            }
        }
        return false;
    }

    bool IsVolatile() const override {
        return AllowVolatile
            && !HasOlapTable()
            && !IsSingleShard()
            && !HasTopics();

        // TODO: && !HasPersistentChannels;
        // Note: currently persistent channels are never used
    }

    bool HasSnapshot() const override {
        return ValidSnapshot;
    }

    void SetHasSnapshot(bool hasSnapshot) override {
        ValidSnapshot = hasSnapshot;
    }

    bool BrokenLocks() const override {
        return LocksIssue.has_value() && !(HasSnapshot() && IsReadOnly());
    }

    const std::optional<NYql::TIssue>& GetLockIssue() const override {
        return LocksIssue;
    }

    const THashSet<ui64>& GetShards() const override {
        return ShardsIds;
    }

    ui64 GetShardsCount() const override {
        return ShardsIds.size();
    }

    bool NeedCommit() const override {
        const bool dontNeedCommit = IsEmpty() || IsReadOnly() && (IsSingleShard() || HasSnapshot());
        return !dontNeedCommit;
    }

    virtual ui64 GetCoordinator() const override {
        return Coordinator;
    }

    void StartPrepare() override {
        AFL_ENSURE(!CollectOnly);
        AFL_ENSURE(State == ETransactionState::COLLECTING);
        AFL_ENSURE(NeedCommit());

        THashSet<ui64> sendingColumnShardsSet;
        THashSet<ui64> receivingColumnShardsSet;

        for (auto& [shardId, shardInfo] : ShardsInfo) {
            if ((shardInfo.Flags & EAction::WRITE)) {
                ReceivingShards.insert(shardId);
                if (shardInfo.IsOlap) {
                    receivingColumnShardsSet.insert(shardId);
                }
                if (IsVolatile()) {
                    SendingShards.insert(shardId);
                }
            }
            if (!shardInfo.Locks.empty()) {
                SendingShards.insert(shardId);
                if (shardInfo.IsOlap) {
                    sendingColumnShardsSet.insert(shardId);
                }
            }

            AFL_ENSURE(shardInfo.State == EShardState::PROCESSING);
            shardInfo.State = EShardState::PREPARING;
        }

        constexpr size_t minArbiterMeshSize = 5;
        if ((IsVolatile() &&
            ReceivingShards.size() >= minArbiterMeshSize))
        {
            std::vector<ui64> candidates;
            candidates.reserve(ReceivingShards.size());
            for (ui64 candidate : ReceivingShards) {
                // Note: all receivers are also senders in volatile transactions
                if (Y_LIKELY(SendingShards.contains(candidate))) {
                    candidates.push_back(candidate);
                }
            }
            if (candidates.size() >= minArbiterMeshSize) {
                // Select a random arbiter
                const ui32 index = RandomNumber<ui32>(candidates.size());
                Arbiter = candidates.at(index);
            }
        }

        if (!receivingColumnShardsSet.empty() || !sendingColumnShardsSet.empty()) {
            AFL_ENSURE(!IsVolatile());
            const auto& shards = receivingColumnShardsSet.empty()
                ? sendingColumnShardsSet
                : receivingColumnShardsSet;

            const ui32 index = RandomNumber<ui32>(shards.size());
            auto arbiterIterator = std::begin(shards);
            std::advance(arbiterIterator, index);
            ArbiterColumnShard = *arbiterIterator;
            ReceivingShards.insert(*ArbiterColumnShard);
        }

        ShardsToWait = ShardsIds;

        MinStep = std::numeric_limits<ui64>::min();
        MaxStep = std::numeric_limits<ui64>::max();
        Coordinator = 0;

        State = ETransactionState::PREPARING;
    }

    TPrepareInfo GetPrepareTransactionInfo() override {
        AFL_ENSURE(State == ETransactionState::PREPARING);
        AFL_ENSURE(!ReceivingShards.empty() || !SendingShards.empty());

        TPrepareInfo result {
            .SendingShards = SendingShards,
            .ReceivingShards = ReceivingShards,
            .Arbiter = Arbiter,
            .ArbiterColumnShard = ArbiterColumnShard,
        };

        return result;
    }

    bool ConsumePrepareTransactionResult(TPrepareResult&& result) override {
        AFL_ENSURE(State == ETransactionState::PREPARING);
        auto& shardInfo = ShardsInfo.at(result.ShardId);
        AFL_ENSURE(shardInfo.State == EShardState::PREPARING);
        shardInfo.State = EShardState::PREPARED;

        ShardsToWait.erase(result.ShardId);

        MinStep = std::max(MinStep, result.MinStep);
        MaxStep = std::min(MaxStep, result.MaxStep);

        if (result.Coordinator && !Coordinator) {
            Coordinator = result.Coordinator;
        }

        AFL_ENSURE(Coordinator && Coordinator == result.Coordinator)("prev_coordinator", Coordinator)("new_coordinator", result.Coordinator);

        return ShardsToWait.empty();
    }

    void StartExecute() override {
        AFL_ENSURE(!CollectOnly);
        AFL_ENSURE(State == ETransactionState::PREPARING
                || (State == ETransactionState::COLLECTING
                    && IsSingleShard()));
        AFL_ENSURE(NeedCommit());
        State = ETransactionState::EXECUTING;

        for (auto& [_, shardInfo] : ShardsInfo) {
            AFL_ENSURE(shardInfo.State == EShardState::PREPARED
                || (shardInfo.State == EShardState::PROCESSING
                    && IsSingleShard()));
            shardInfo.State = EShardState::EXECUTING;
        }

        ShardsToWait = ShardsIds;

        AFL_ENSURE(ReceivingShards.empty() || HasTopics() || !IsSingleShard() || HasOlapTable());
    }

    TCommitInfo GetCommitInfo() override {
        AFL_ENSURE(State == ETransactionState::EXECUTING);
        TCommitInfo result;
        result.MinStep = MinStep;
        result.MaxStep = MaxStep;
        result.Coordinator = Coordinator;

        for (auto& [shardId, shardInfo] : ShardsInfo) {
            result.ShardsInfo.push_back(TCommitShardInfo{
                .ShardId = shardId,
                .AffectedFlags = shardInfo.Flags,
            });

            AFL_ENSURE(shardInfo.State == EShardState::EXECUTING);
        }
        return result;
    }

    bool ConsumeCommitResult(ui64 shardId) override {
        AFL_ENSURE(State == ETransactionState::EXECUTING);
        auto& shardInfo = ShardsInfo.at(shardId);
        AFL_ENSURE(shardInfo.State == EShardState::EXECUTING);
        shardInfo.State = EShardState::FINISHED;

        ShardsToWait.erase(shardId);
        return ShardsToWait.empty();
    }

private:
    bool CollectOnly = false;
    ETransactionState State = ETransactionState::COLLECTING;

    struct TShardInfo {
        EShardState State = EShardState::PROCESSING;
        TActionFlags Flags = 0;

        struct TLockInfo {
            TKqpLock Lock;
            bool Invalidated = false;
            bool LocksAcquireFailure = false;
        };

        THashMap<TKqpLock::TKey, TLockInfo> Locks;

        bool IsOlap = false;
        THashSet<TStringBuf> Pathes;

        bool Restarting = false;
        bool Reattaching = false;
        TReattachState ReattachState;
    };

    void MakeLocksIssue(const TShardInfo& shardInfo) {
        TStringBuilder message;
        message << "Transaction locks invalidated. ";
        message << (shardInfo.Pathes.size() == 1 ? "Table: " : "Tables: ");
        bool first = true;
        // TODO: add error by pathid
        for (const auto& path : shardInfo.Pathes) {
            if (!first) {
                message << ", ";
                first = false;
            }
            message << "`" << path << "`";
        }
        LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    }

    THashSet<ui64> ShardsIds;
    THashMap<ui64, TShardInfo> ShardsInfo;
    std::unordered_set<TString> TablePathes;

    THashMap<TTableId, std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>> TablePartitioning;

    bool AllowVolatile = false;
    bool ReadOnly = true;
    bool ValidSnapshot = false;
    bool HasOlapTableShard = false;
    std::optional<NYql::TIssue> LocksIssue;

    THashSet<ui64> SendingShards;
    THashSet<ui64> ReceivingShards;
    std::optional<ui64> Arbiter;
    std::optional<ui64> ArbiterColumnShard;

    THashSet<ui64> ShardsToWait;

    NTopic::TTopicOperations TopicOperations;

    ui64 MinStep = 0;
    ui64 MaxStep = 0;
    ui64 Coordinator = 0;
};

}

IKqpTransactionManagerPtr CreateKqpTransactionManager(bool collectOnly) {
    return std::make_shared<TKqpTransactionManager>(collectOnly);
}

}
}
