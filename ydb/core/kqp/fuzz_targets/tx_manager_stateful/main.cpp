#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/executer_actor/kqp_locks_helper.h>
#include <ydb/core/tx/locks/sys_tables.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <tuple>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

constexpr size_t MaxSteps = 256;
constexpr size_t MaxShards = 8;

using TLockKey = std::tuple<ui64, ui64, ui64, ui64>;

struct TLockModel {
    NKikimrDataEvents::TLock Proto;
    bool Invalidated = false;
    bool AcquireFailure = false;
    ui64 VictimSpan = 0;
};

struct TShardModel {
    ui8 Flags = 0;
    bool IsOlap = false;
    TString Path;
    IKqpTransactionManager::EShardState State = IKqpTransactionManager::PROCESSING;
    THashMap<TLockKey, TLockModel> Locks;
    TVector<ui64> Breakers;
    THashSet<ui64> BreakersSet;
};

enum class ETxState : ui8 {
    Collecting,
    Preparing,
    Executing,
    RollingBack,
    Error,
};

struct TModel {
    THashMap<ui64, TShardModel> Shards;
    TVector<ui64> ShardOrder;
    THashSet<ui64> Waiting;
    THashSet<ui64> Sending;
    THashSet<ui64> Receiving;
    ETxState State = ETxState::Collecting;
    ui64 ActionsCount = 0;
    bool ReadOnly = true;
    bool HasSnapshot = false;
    bool AllowVolatile = false;
    bool HasOlap = false;
    bool HasLockIssue = false;
    std::optional<ui64> VictimSpan;
    ui64 MinStep = 0;
    ui64 MaxStep = 0;
    ui64 Coordinator = 0;

    bool Empty() const {
        return Shards.empty();
    }

    bool SingleShard() const {
        return Shards.size() == 1;
    }

    bool CanAskNeedCommit() const {
        return ActionsCount != 1 || SingleShard();
    }

    bool NeedCommit() const {
        return !Empty() && !(ReadOnly && (ActionsCount == 1 || HasSnapshot));
    }

    bool BrokenLocksVisible() const {
        return HasLockIssue && !(HasSnapshot && ReadOnly);
    }

    bool IsVolatile() const {
        return AllowVolatile && !HasOlap && !SingleShard();
    }
};

TLockKey KeyOf(const NKikimrDataEvents::TLock& lock) {
    return std::make_tuple(lock.GetLockId(), lock.GetDataShard(), lock.GetSchemeShard(), lock.GetPathId());
}

ui64 PickShard(FuzzedDataProvider& provider, const TModel& model) {
    return model.ShardOrder[provider.ConsumeIntegralInRange<size_t>(0, model.ShardOrder.size() - 1)];
}

NKikimrDataEvents::TLock BuildLock(FuzzedDataProvider& provider, ui64 shardId) {
    NKikimrDataEvents::TLock lock;
    const ui64 lockId = provider.ConsumeIntegralInRange<ui64>(1, 16);
    lock.SetLockId(lockId);
    lock.SetDataShard(provider.ConsumeBool() ? shardId : provider.ConsumeIntegralInRange<ui64>(1, 16));
    lock.SetSchemeShard(provider.ConsumeIntegralInRange<ui64>(1, 4));
    lock.SetPathId(provider.ConsumeIntegralInRange<ui64>(1, 16));
    lock.SetGeneration(provider.ConsumeIntegralInRange<ui64>(1, 8));
    lock.SetCounter(provider.ConsumeIntegralInRange<ui64>(1, 8));
    if (provider.ConsumeIntegralInRange<unsigned>(0, 31) == 0) {
        lock.SetCounter(NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken);
    } else if (provider.ConsumeIntegralInRange<unsigned>(0, 31) == 0) {
        lock.SetCounter(NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken);
    } else if (provider.ConsumeIntegralInRange<unsigned>(0, 31) == 0) {
        lock.SetCounter(NKikimr::TSysTables::TLocksTable::TLock::ErrorMin + 7);
    }
    lock.SetHasWrites(provider.ConsumeBool());
    return lock;
}

void AddBreaker(TShardModel& shard, ui64 span) {
    if (span != 0 && shard.BreakersSet.emplace(span).second) {
        shard.Breakers.push_back(span);
    }
}

void CheckLocks(IKqpTransactionManager& manager, const TModel& model) {
    ui64 expectedBroken = 0;
    ui64 expectedLocks = 0;
    for (const auto& [shardId, shard] : model.Shards) {
        const auto actual = manager.GetLocks(shardId);
        Y_ABORT_UNLESS(actual.size() == shard.Locks.size());
        expectedLocks += shard.Locks.size();

        for (const auto& [key, lockModel] : shard.Locks) {
            expectedBroken += ui64(lockModel.Invalidated || lockModel.AcquireFailure);
            const auto victim = manager.LookupVictimQuerySpanId(shardId, lockModel.Proto);
            if (lockModel.VictimSpan != 0) {
                Y_ABORT_UNLESS(victim && *victim == lockModel.VictimSpan);
            } else {
                Y_ABORT_UNLESS(!victim);
            }
        }
    }

    const auto allLocks = manager.GetLocks();
    Y_ABORT_UNLESS(allLocks.size() == expectedLocks);
    NKikimrMiniKQL::TResult builtLocks;
    BuildLocks(builtLocks, allLocks);
    Y_ABORT_UNLESS(static_cast<size_t>(builtLocks.GetValue().ListSize()) == allLocks.size());
    for (size_t i = 0; i < allLocks.size(); ++i) {
        const NYql::NDq::TMkqlValueRef lockRef(
            builtLocks.GetType().GetList().GetItem(),
            builtLocks.GetValue().GetList(i));
        const auto extracted = ExtractLock(lockRef);
        Y_ABORT_UNLESS(KeyOf(extracted) == KeyOf(allLocks[i]));
        Y_ABORT_UNLESS(extracted.GetCounter() == allLocks[i].GetCounter());
        Y_ABORT_UNLESS(extracted.GetGeneration() == allLocks[i].GetGeneration());
        Y_ABORT_UNLESS(extracted.GetHasWrites() == allLocks[i].GetHasWrites());
    }
    Y_ABORT_UNLESS(manager.GetBrokenLocksCount() == expectedBroken);
    Y_ABORT_UNLESS(manager.BrokenLocks() == model.BrokenLocksVisible());
    Y_ABORT_UNLESS(bool(manager.GetLockIssue()) == model.HasLockIssue);
    if (model.VictimSpan) {
        Y_ABORT_UNLESS(manager.GetVictimQuerySpanId() == model.VictimSpan);
    }
}

void CheckBasic(IKqpTransactionManager& manager, const TModel& model) {
    Y_ABORT_UNLESS(manager.GetShardsCount() == model.Shards.size());
    Y_ABORT_UNLESS(manager.IsEmpty() == model.Empty());
    Y_ABORT_UNLESS(manager.IsSingleShard() == model.SingleShard());
    Y_ABORT_UNLESS(manager.IsReadOnly() == model.ReadOnly);
    Y_ABORT_UNLESS(manager.HasSnapshot() == model.HasSnapshot);
    Y_ABORT_UNLESS(manager.HasOlapTable() == model.HasOlap);
    Y_ABORT_UNLESS(manager.IsVolatile() == model.IsVolatile());
    if (model.CanAskNeedCommit()) {
        Y_ABORT_UNLESS(manager.NeedCommit() == model.NeedCommit());
    }
    CheckLocks(manager, model);
}

void AddShard(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if (model.State != ETxState::Collecting && model.State != ETxState::Error) {
        return;
    }
    const ui64 shardId = provider.ConsumeIntegralInRange<ui64>(1, 16);
    const bool isOlap = provider.ConsumeBool();
    const TString path = TStringBuilder() << "/Root/T" << shardId << "/" << provider.ConsumeIntegralInRange<unsigned>(0, 7);

    manager.AddShard(shardId, isOlap, path);

    auto [it, inserted] = model.Shards.emplace(shardId, TShardModel{});
    if (inserted) {
        model.ShardOrder.push_back(shardId);
    }
    it->second.IsOlap = isOlap;
    it->second.Path = path;
    model.HasOlap |= isOlap;
}

void AddAction(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if ((model.State != ETxState::Collecting && model.State != ETxState::Error) || model.ShardOrder.empty()) {
        return;
    }
    const ui64 shardId = PickShard(provider, model);
    ui8 action = provider.ConsumeBool() ? IKqpTransactionManager::READ : IKqpTransactionManager::WRITE;
    if (provider.ConsumeBool()) {
        action |= provider.ConsumeBool() ? IKqpTransactionManager::READ : IKqpTransactionManager::WRITE;
    }
    const ui64 span = provider.ConsumeIntegralInRange<ui64>(0, 64);

    manager.AddAction(shardId, action, span);
    auto& shard = model.Shards[shardId];
    shard.Flags |= action;
    if (action & IKqpTransactionManager::WRITE) {
        model.ReadOnly = false;
        AddBreaker(shard, span);
    }
    ++model.ActionsCount;
}

void AddLock(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if ((model.State != ETxState::Collecting && model.State != ETxState::Error) || model.ShardOrder.empty()) {
        return;
    }
    const ui64 shardId = PickShard(provider, model);
    auto lock = BuildLock(provider, shardId);
    const ui64 querySpan = provider.ConsumeIntegralInRange<ui64>(0, 64);
    const ui64 deferredSpan = provider.ConsumeIntegralInRange<ui64>(0, 64);

    const bool actual = manager.AddLock(shardId, lock, querySpan, deferredSpan);

    auto& shard = model.Shards[shardId];
    const auto key = KeyOf(lock);
    const bool isError = lock.GetCounter() >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin;
    const bool isInvalidatedError =
        lock.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken ||
        lock.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken;
    const bool isAcquireFailure = isError && !isInvalidatedError;
    const ui64 effectiveVictim = (isError && deferredSpan != 0) ? deferredSpan : querySpan;

    bool invalidated = isInvalidatedError;
    bool acquireFailure = isAcquireFailure;
    auto it = shard.Locks.find(key);
    if (it == shard.Locks.end()) {
        TLockModel lockModel;
        lockModel.Proto = lock;
        lockModel.Invalidated = invalidated;
        lockModel.AcquireFailure = acquireFailure;
        lockModel.VictimSpan = effectiveVictim;
        it = shard.Locks.emplace(key, std::move(lockModel)).first;
    } else {
        if (lock.GetHasWrites()) {
            it->second.Proto.SetHasWrites(true);
        }
        it->second.AcquireFailure |= acquireFailure;
        if (!it->second.AcquireFailure) {
            invalidated = invalidated || it->second.Proto.GetGeneration() != lock.GetGeneration() || it->second.Proto.GetCounter() != lock.GetCounter();
            it->second.Invalidated |= invalidated;
        }
        acquireFailure = it->second.AcquireFailure;
    }

    const bool broken = it->second.Invalidated || it->second.AcquireFailure;
    if (broken && invalidated) {
        ui64 victim = it->second.VictimSpan;
        if (isError && deferredSpan != 0) {
            victim = effectiveVictim;
        } else if (victim == 0 && isError && effectiveVictim != 0) {
            victim = effectiveVictim;
        }
        if (victim != 0 && !model.VictimSpan) {
            model.VictimSpan = victim;
        }
    }

    const bool reportsNewLockIssue = broken && !model.HasLockIssue && model.State != ETxState::Error;
    if (reportsNewLockIssue) {
        model.HasLockIssue = true;
    }
    Y_ABORT_UNLESS(actual == !reportsNewLockIssue);
}

void BreakLock(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if (model.ShardOrder.empty() || model.HasLockIssue) {
        return;
    }
    const ui64 shardId = PickShard(provider, model);
    manager.BreakLock(shardId);
    model.HasLockIssue = true;
}

void StartPrepare(IKqpTransactionManager& manager, TModel& model) {
    if (model.State != ETxState::Collecting || !model.CanAskNeedCommit() || !model.NeedCommit() || model.BrokenLocksVisible()) {
        return;
    }
    manager.StartPrepare();
    model.State = ETxState::Preparing;
    model.Waiting.clear();
    model.Sending.clear();
    model.Receiving.clear();
    for (auto& [shardId, shard] : model.Shards) {
        model.Waiting.insert(shardId);
        if (shard.Flags & IKqpTransactionManager::WRITE) {
            model.Receiving.insert(shardId);
            if (model.IsVolatile()) {
                model.Sending.insert(shardId);
            }
        }
        if (!shard.Locks.empty() || (shard.Flags & IKqpTransactionManager::READ)) {
            model.Sending.insert(shardId);
        }
        shard.State = IKqpTransactionManager::PREPARING;
    }

    const auto info = manager.GetPrepareTransactionInfo();
    for (ui64 shardId : model.Sending) {
        Y_ABORT_UNLESS(info.SendingShards.contains(shardId));
    }
    for (ui64 shardId : model.Receiving) {
        Y_ABORT_UNLESS(info.ReceivingShards.contains(shardId));
    }
    if (info.Arbiter) {
        Y_ABORT_UNLESS(info.SendingShards.contains(*info.Arbiter));
        Y_ABORT_UNLESS(info.ReceivingShards.contains(*info.Arbiter));
    }
    if (info.ArbiterColumnShard) {
        Y_ABORT_UNLESS(model.Shards.at(*info.ArbiterColumnShard).IsOlap);
        Y_ABORT_UNLESS(info.ReceivingShards.contains(*info.ArbiterColumnShard));
    }

    model.MinStep = std::numeric_limits<ui64>::min();
    model.MaxStep = std::numeric_limits<ui64>::max();
    model.Coordinator = 0;
}

void ConsumePrepare(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if (model.State != ETxState::Preparing || model.Waiting.empty()) {
        return;
    }
    const auto it = std::next(model.Waiting.begin(), provider.ConsumeIntegralInRange<size_t>(0, model.Waiting.size() - 1));
    const ui64 shardId = *it;
    const ui64 minStep = provider.ConsumeIntegralInRange<ui64>(1, 1000);
    const ui64 maxStep = provider.ConsumeIntegralInRange<ui64>(minStep, minStep + 1000);
    if (model.Coordinator == 0) {
        model.Coordinator = provider.ConsumeIntegralInRange<ui64>(1, 128);
    }

    const bool done = manager.ConsumePrepareTransactionResult({shardId, minStep, maxStep, model.Coordinator});
    model.Shards[shardId].State = IKqpTransactionManager::PREPARED;
    model.Waiting.erase(shardId);
    model.MinStep = std::max(model.MinStep, minStep);
    model.MaxStep = std::min(model.MaxStep, maxStep);
    Y_ABORT_UNLESS(done == model.Waiting.empty());
}

void StartExecute(IKqpTransactionManager& manager, TModel& model) {
    const bool canExecute =
        ((model.State == ETxState::Preparing && model.Waiting.empty()) ||
         (model.State == ETxState::Collecting && model.SingleShard())) &&
        model.CanAskNeedCommit() && model.NeedCommit() && !model.BrokenLocksVisible();
    if (!canExecute) {
        return;
    }

    manager.StartExecute();
    model.State = ETxState::Executing;
    model.Waiting.clear();
    for (auto& [shardId, shard] : model.Shards) {
        shard.State = IKqpTransactionManager::EXECUTING;
        model.Waiting.insert(shardId);
    }

    const auto info = manager.GetCommitInfo();
    Y_ABORT_UNLESS(info.MinStep == model.MinStep);
    Y_ABORT_UNLESS(info.MaxStep == model.MaxStep);
    Y_ABORT_UNLESS(info.Coordinator == model.Coordinator);
    Y_ABORT_UNLESS(info.ShardsInfo.size() == model.Shards.size());
}

void ConsumeCommit(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if (model.State != ETxState::Executing || model.Waiting.empty()) {
        return;
    }
    const auto it = std::next(model.Waiting.begin(), provider.ConsumeIntegralInRange<size_t>(0, model.Waiting.size() - 1));
    const ui64 shardId = *it;
    const bool done = manager.ConsumeCommitResult(shardId);
    model.Shards[shardId].State = IKqpTransactionManager::FINISHED;
    model.Waiting.erase(shardId);
    Y_ABORT_UNLESS(done == model.Waiting.empty());
}

void StartRollback(IKqpTransactionManager& manager, TModel& model) {
    if (model.State == ETxState::RollingBack) {
        return;
    }
    const auto& rollbackShards = manager.StartRollback();
    model.State = ETxState::RollingBack;
    model.Waiting.clear();
    for (auto& [shardId, shard] : model.Shards) {
        if (!shard.Locks.empty()) {
            model.Waiting.insert(shardId);
            Y_ABORT_UNLESS(rollbackShards.contains(shardId));
        }
        if (shard.State != IKqpTransactionManager::ERROR) {
            shard.State = IKqpTransactionManager::FINISHED;
        }
    }
    Y_ABORT_UNLESS(rollbackShards.size() == model.Waiting.size());
    Y_ABORT_UNLESS(manager.IsRollBack());
}

void ConsumeRollback(IKqpTransactionManager& manager, TModel& model, FuzzedDataProvider& provider) {
    if (model.State != ETxState::RollingBack || model.Waiting.empty()) {
        return;
    }
    const auto it = std::next(model.Waiting.begin(), provider.ConsumeIntegralInRange<size_t>(0, model.Waiting.size() - 1));
    const ui64 shardId = *it;
    const bool done = manager.ConsumeRollbackResult(shardId);
    model.Waiting.erase(shardId);
    Y_ABORT_UNLESS(done == model.Waiting.empty());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    auto manager = CreateKqpTransactionManager();
    TModel model;

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t i = 0; i < steps && provider.remaining_bytes() > 0; ++i) {
        switch (provider.ConsumeIntegralInRange<unsigned>(0, 13)) {
            case 0:
                AddShard(*manager, model, provider);
                break;
            case 1:
            case 2:
                AddAction(*manager, model, provider);
                break;
            case 3:
            case 4:
                AddLock(*manager, model, provider);
                break;
            case 5:
                BreakLock(*manager, model, provider);
                break;
            case 6:
                if (model.State == ETxState::Collecting || model.State == ETxState::Error) {
                    model.HasSnapshot = provider.ConsumeBool();
                    manager->SetHasSnapshot(model.HasSnapshot);
                }
                break;
            case 7:
                model.AllowVolatile = provider.ConsumeBool();
                manager->SetAllowVolatile(model.AllowVolatile);
                break;
            case 8:
                StartPrepare(*manager, model);
                break;
            case 9:
                ConsumePrepare(*manager, model, provider);
                break;
            case 10:
                StartExecute(*manager, model);
                break;
            case 11:
                ConsumeCommit(*manager, model, provider);
                break;
            case 12:
                StartRollback(*manager, model);
                break;
            case 13:
                ConsumeRollback(*manager, model, provider);
                break;
        }
        CheckBasic(*manager, model);
        if (model.ShardOrder.size() > MaxShards) {
            break;
        }
    }

    return 0;
}
