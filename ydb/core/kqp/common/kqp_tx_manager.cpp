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
        ERROR,
        ROLLINGBACK,
    };
public:
    TKqpTransactionManager(bool collectOnly)
        : CollectOnly(collectOnly) {}

    void AddShard(ui64 shardId, bool isOlap, const TString& path) override {
        AFL_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        ShardsIds.insert(shardId);
        auto& shardInfo = ShardsInfo[shardId];
        shardInfo.IsOlap = isOlap;
        HasOlapTableShard |= isOlap;

        const auto [stringsIter, _] = TablePathes.insert(path);
        const TStringBuf pathBuf = *stringsIter;
        shardInfo.Pathes.insert(pathBuf);
    }

    void AddAction(ui64 shardId, ui8 action) override {
        AddAction(shardId, action, 0);
    }

    void AddAction(ui64 shardId, ui8 action, ui64 querySpanId) override {
        AFL_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        auto& shardInfo = ShardsInfo.at(shardId);
        shardInfo.Flags |= action;
        if (action & EAction::WRITE) {
            ReadOnly = false;
            // Track all QuerySpanIds of queries that wrote to this shard
            AddBreakerQuerySpanId(shardInfo, querySpanId);
        }
        ++ActionsCount;
    }

    void AddTopic(ui64 topicId, const TString& path) override {
        AFL_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
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

    bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lockProto, ui64 querySpanId, ui64 deferredVictimQuerySpanId) override {
        AFL_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        TKqpLock lock(lockProto);
        bool isError = (lock.Proto.GetCounter() >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin);
        bool isInvalidated = (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken)
                            || (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken);
        bool isLocksAcquireFailure = isError && !isInvalidated;
        bool broken = false;

        // For broken locks from the shard (error counter), prefer deferredVictimQuerySpanId
        // which the shard computed based on whether the lock was already broken or deferred.
        // For non-error locks, use querySpanId (the current query that set the lock).
        ui64 effectiveVictimSpanId = (isError && deferredVictimQuerySpanId != 0)
            ? deferredVictimQuerySpanId : querySpanId;

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

            if (broken && isInvalidated) {
                // For broken locks from shard: use effectiveVictimSpanId (from shard's determination).
                // For comparison-based invalidation: use the original lock setter's SpanId.
                ui64 victimSpanId = (isError && effectiveVictimSpanId != 0)
                    ? effectiveVictimSpanId : lockPtr->VictimQuerySpanId;
                SetVictimQuerySpanId(victimSpanId);
            }
        } else {
            shardInfo.Locks.emplace(
                lock.GetKey(),
                TShardInfo::TLockInfo {
                    .Lock = std::move(lock),
                    .Invalidated = isInvalidated,
                    .LocksAcquireFailure = isLocksAcquireFailure,
                    .VictimQuerySpanId = effectiveVictimSpanId,
                });
            broken = isInvalidated || isLocksAcquireFailure;

            if (broken && isInvalidated) {
                SetVictimQuerySpanId(effectiveVictimSpanId);
            }
        }

        if (broken && !LocksIssue && State != ETransactionState::ERROR) {
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

    void SetError() override {
        State = ETransactionState::ERROR;
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

    void AddParticipantNode(const ui32 nodeId) override {
        ParticipantNodes.insert(nodeId);
    }

    const THashSet<ui32>& GetParticipantNodes() const override {
        return ParticipantNodes;
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

    ui64 GetBrokenLocksCount() const override {
        ui64 count = 0;
        for (const auto& [shardId, shardInfo] : ShardsInfo) {
            for (const auto& [key, lockInfo] : shardInfo.Locks) {
                if (lockInfo.Invalidated || lockInfo.LocksAcquireFailure) {
                    ++count;
                }
            }
        }
        return count;
    }

    const std::optional<NYql::TIssue>& GetLockIssue() const override {
        return LocksIssue;
    }

    void SetVictimQuerySpanId(ui64 querySpanId) override {
        if (querySpanId == 0) {
            return;
        }

        if (!VictimQuerySpanId_) {
            VictimQuerySpanId_ = querySpanId;

            // If we already have a LocksIssue, update its message to include the victim query trace id
            if (LocksIssue) {
                TString currentMessage = LocksIssue->GetMessage();
                if (!currentMessage.Contains("VictimQuerySpanId:")) {
                    TStringBuilder message;
                    message << currentMessage;
                    message << " VictimQuerySpanId: " << *VictimQuerySpanId_ << ".";
                    LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
                }
            }
        }
    }

    std::optional<ui64> GetVictimQuerySpanId() const override {
        return VictimQuerySpanId_;
    }

    std::optional<ui64> LookupVictimQuerySpanId(ui64 shardId, const NKikimrDataEvents::TLock& lockProto) const override {
        auto shardIt = ShardsInfo.find(shardId);
        if (shardIt == ShardsInfo.end()) {
            return std::nullopt;
        }
        TKqpLock lock(lockProto);
        auto lockIt = shardIt->second.Locks.find(lock.GetKey());
        if (lockIt == shardIt->second.Locks.end()) {
            return std::nullopt;
        }
        ui64 spanId = lockIt->second.VictimQuerySpanId;
        return spanId != 0 ? std::make_optional(spanId) : std::nullopt;
    }

    void SetShardBreakerQuerySpanId(ui64 shardId, ui64 querySpanId) override {
        if (querySpanId == 0) {
            return;
        }
        auto it = ShardsInfo.find(shardId);
        if (it == ShardsInfo.end()) {
            return;
        }
        AddBreakerQuerySpanId(it->second, querySpanId);
    }

    TVector<ui64> GetShardBreakerQuerySpanIds(ui64 shardId) const override {
        auto it = ShardsInfo.find(shardId);
        if (it != ShardsInfo.end() && !it->second.BreakerQuerySpanIds.empty()) {
            return it->second.BreakerQuerySpanIds;
        }
        return {};
    }

    const THashSet<ui64>& GetShards() const override {
        return ShardsIds;
    }

    ui64 GetShardsCount() const override {
        return ShardsIds.size();
    }

    bool NeedCommit() const override {
        AFL_ENSURE(ActionsCount != 1 || IsSingleShard()); // ActionsCount == 1 then IsSingleShard()
        const bool dontNeedCommit = IsEmpty() || (IsReadOnly() && ((ActionsCount == 1) || HasSnapshot()));
        return !dontNeedCommit;
    }

    virtual ui64 GetCoordinator() const override {
        return Coordinator;
    }

    void StartPrepare() override {
        AFL_ENSURE(!CollectOnly);
        AFL_ENSURE(State == ETransactionState::COLLECTING);
        AFL_ENSURE(NeedCommit());
        AFL_ENSURE(!BrokenLocks());

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
            if (!shardInfo.Locks.empty() || (shardInfo.Flags & EAction::READ)) {
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
        AFL_ENSURE(!BrokenLocks());
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

    const THashSet<ui64>& StartRollback() override {
        AFL_ENSURE(State != ETransactionState::ROLLINGBACK);
        State = ETransactionState::ROLLINGBACK;
        ShardsToWait.clear();
        for (auto& [shardId, shardInfo] : ShardsInfo) {
            if (shardInfo.State != EShardState::ERROR) {
                shardInfo.State = EShardState::FINISHED;
            }
            if (!shardInfo.Locks.empty()) {
                ShardsToWait.insert(shardId);
            }
        }

        return ShardsToWait;
    }

    bool ConsumeRollbackResult(ui64 shardId) override {
        AFL_ENSURE(State == ETransactionState::ROLLINGBACK);
        ShardsToWait.erase(shardId);
        return ShardsToWait.empty();
    }

    bool IsRollBack() const override {
        return State == ETransactionState::ROLLINGBACK;
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
            ui64 VictimQuerySpanId = 0;
        };

        THashMap<TKqpLock::TKey, TLockInfo> Locks;

        bool IsOlap = false;
        THashSet<TStringBuf> Pathes;

        bool Restarting = false;
        bool Reattaching = false;
        TReattachState ReattachState;

        // All QuerySpanIds of queries that wrote to this shard in insertion order.
        TVector<ui64> BreakerQuerySpanIds;
        THashSet<ui64> BreakerQuerySpanIdsSet;
    };

    static void AddBreakerQuerySpanId(TShardInfo& shardInfo, ui64 querySpanId) {
        if (querySpanId != 0 && shardInfo.BreakerQuerySpanIdsSet.emplace(querySpanId).second) {
            shardInfo.BreakerQuerySpanIds.push_back(querySpanId);
        }
    }

    void MakeLocksIssue(const TShardInfo& shardInfo) {
        TStringBuilder message;
        message << "Transaction locks invalidated. ";
        message << (shardInfo.Pathes.size() == 1 ? "Table: " : "Tables: ");
        bool first = true;
        // TODO: add error by pathid
        for (const auto& path : shardInfo.Pathes) {
            if (!first) {
                message << ", ";
            }
            first = false;
            message << "`" << path << "`";
        }
        message << ".";
        if (VictimQuerySpanId_ && *VictimQuerySpanId_ != 0) {
            message << " VictimQuerySpanId: " << *VictimQuerySpanId_ << ".";
        }
        LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    }

    THashSet<ui64> ShardsIds;
    THashMap<ui64, TShardInfo> ShardsInfo;
    std::unordered_set<TString> TablePathes;
    ui64 ActionsCount = 0;

    THashSet<ui32> ParticipantNodes;

    THashMap<TTableId, std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>> TablePartitioning;

    bool AllowVolatile = false;
    bool ReadOnly = true;
    bool ValidSnapshot = false;
    bool HasOlapTableShard = false;
    std::optional<NYql::TIssue> LocksIssue;
    std::optional<ui64> VictimQuerySpanId_;

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
