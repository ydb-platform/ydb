#include "kqp_tx_manager.h"

#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr {
namespace NKqp {

namespace {

struct TKqpLock {
    using TKey = std::tuple<ui64, ui64, ui64, ui64>;
    TKey GetKey() const { return std::make_tuple(Proto.GetLockId(), Proto.GetDataShard(), Proto.GetSchemeShard(), Proto.GetPathId()); }

    bool Invalidated(const TKqpLock& newLock) const {
        YQL_ENSURE(GetKey() == newLock.GetKey());
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
        AFL_ENSURE(State == ETransactionState::COLLECTING);
        ShardsIds.insert(shardId);
        auto& shardInfo = ShardsInfo[shardId];
        shardInfo.IsOlap = isOlap;

        const auto [stringsIter, _] = TablePathes.insert(path);
        const TStringBuf pathBuf = *stringsIter;
        shardInfo.Pathes.insert(pathBuf);
    }

    void AddAction(ui64 shardId, ui8 action) override {
        AFL_ENSURE(State == ETransactionState::COLLECTING);
        ShardsInfo.at(shardId).Flags |= action;
        if (action & EAction::WRITE) {
            ReadOnly = false;
        }
    }

    bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lockProto) override {
        TKqpLock lock(lockProto);
        AFL_ENSURE(State == ETransactionState::COLLECTING);
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
            const auto& lockInfo = shardInfo.Locks.at(lock.GetKey());
            if (lockInfo.LocksAcquireFailure) {
                LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE);
                return false;
            } else if (lockInfo.Invalidated) {
                TStringBuilder message;
                message << "Transaction locks invalidated. Tables: ";
                bool first = true;
                // TODO: add error by lock key (pathid)
                for (const auto& path : shardInfo.Pathes) {
                    if (!first) {
                        message << ", ";
                        first = false;
                    }
                    message << "`" << path << "`";
                }
                LocksIssue = YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
                return false;
            }
            YQL_ENSURE(false);
        }

        return true;
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

    void SetState(ui64 shardId, EShardState state) override {
        ShardsInfo.at(shardId).State = state;
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

    bool IsEmpty() const override {
        return GetShardsCount() == 0;
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

    void StartPrepare() override {
        YQL_ENSURE(!CollectOnly);
        AFL_ENSURE(State == ETransactionState::COLLECTING);
        AFL_ENSURE(!IsReadOnly());

        for (const auto& [shardId, shardInfo] : ShardsInfo) {
            if (shardInfo.Flags & EAction::WRITE) {
                ReceivingShards.insert(shardId);
            }
            if (shardInfo.Flags & EAction::READ) {
                SendingShards.insert(shardId);
            }
        }

        ShardsToWait = ShardsIds;

        MinStep = std::numeric_limits<ui64>::min();
        MaxStep = std::numeric_limits<ui64>::max();
        Coordinator = 0;

        State = ETransactionState::PREPARING;
    }

    TPrepareInfo GetPrepareTransactionInfo() override {
        AFL_ENSURE(State == ETransactionState::PREPARING);

        TPrepareInfo result {
            .SendingShards = SendingShards,
            .ReceivingShards = ReceivingShards,
            .Arbiter = std::nullopt,
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
        YQL_ENSURE(!CollectOnly);
        AFL_ENSURE(State == ETransactionState::PREPARING
                || (State == ETransactionState::COLLECTING
                    && IsSingleShard()));
        AFL_ENSURE(!IsReadOnly());
        State = ETransactionState::EXECUTING;

        for (auto& [_, shardInfo] : ShardsInfo) {
            AFL_ENSURE(shardInfo.State == EShardState::PREPARED
                || (shardInfo.State == EShardState::PROCESSING
                    && IsSingleShard()));
            shardInfo.State = EShardState::EXECUTING;
        }

        ShardsToWait = ShardsIds;
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

            AFL_ENSURE(shardInfo.State == EShardState::PREPARED || shardInfo.State == EShardState::PROCESSING);
            shardInfo.State = EShardState::EXECUTING;
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
    };

    THashSet<ui64> ShardsIds;
    THashMap<ui64, TShardInfo> ShardsInfo;
    std::unordered_set<TString> TablePathes;

    bool ReadOnly = true;
    bool ValidSnapshot = false;
    std::optional<NYql::TIssue> LocksIssue;

    THashSet<ui64> SendingShards;
    THashSet<ui64> ReceivingShards;

    THashSet<ui64> ShardsToWait;

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
