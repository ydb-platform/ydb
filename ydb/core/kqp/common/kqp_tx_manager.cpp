#include "kqp_tx_manager.h"
#include "kqp_shard_meta.h"

#include <algorithm>
#include <ydb/core/tx/locks/sys_tables.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NKqp {

bool SameBorders(const TTableRange& lhs, const TTableRange& rhs) {
    if (lhs.Point != rhs.Point
            || lhs.InclusiveFrom != rhs.InclusiveFrom
            || lhs.InclusiveTo != rhs.InclusiveTo) {
        return false;
    }

    if (lhs.From.size() != rhs.From.size() || lhs.To.size() != rhs.To.size()) {
        return false;
    }

    for (size_t i = 0; i < lhs.From.size(); ++i) {
        const TCell& a = lhs.From[i];
        const TCell& b = rhs.From[i];
        if (a.IsNull() != b.IsNull()
                || a.Size() != b.Size()
                || (a.Size() != 0 && memcmp(a.Data(), b.Data(), a.Size()) != 0)) {
            return false;
        }
    }

    for (size_t i = 0; i < lhs.To.size(); ++i) {
        const TCell& a = lhs.To[i];
        const TCell& b = rhs.To[i];
        if (a.IsNull() != b.IsNull()
                || a.Size() != b.Size()
                || (a.Size() != 0 && memcmp(a.Data(), b.Data(), a.Size()) != 0)) {
            return false;
        }
    }

    return true;
}

namespace {

struct TKqpLock {
    using TKey = std::tuple<ui64, ui64, ui64, ui64, ui64>;
    TKey GetKey() const { return std::make_tuple(Proto.GetLockId(), Proto.GetDataShard(), Proto.GetCreatedOn(), Proto.GetSchemeShard(), Proto.GetPathId()); }

    bool Invalidated(const TKqpLock& newLock) const {
        YDB_LOG_ENSURE(GetKey() == newLock.GetKey());
        return Proto.GetGeneration() != newLock.Proto.GetGeneration() || Proto.GetCounter() != newLock.Proto.GetCounter();
    }

    // A current lock's WSN is monotonic per writer. Ancestor copies can
    // report different branch maxima, so their callers only use the merged WSNs.
    bool MergeWriteSeqNums(const NKikimrDataEvents::TLock& incoming) {
        bool nonDecreasing = true;
        for (const auto& writeSeqNum : incoming.GetWriteSeqNums()) {
            auto* existing = FindIfPtr(*Proto.MutableWriteSeqNums(),
                [&](const auto& entry) { return entry.GetWriterIndex() == writeSeqNum.GetWriterIndex(); });
            if (existing) {
                if (writeSeqNum.GetWriteSeqNum() < existing->GetWriteSeqNum()) {
                    nonDecreasing = false;
                } else {
                    existing->SetWriteSeqNum(writeSeqNum.GetWriteSeqNum());
                }
            } else {
                *Proto.AddWriteSeqNums() = writeSeqNum;
            }
        }
        return nonDecreasing;
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

    void AddShard(ui64 shardId, bool isOlap, const TString& path, const TPathId& pathId, const TShardMeta& meta) override {
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        ShardsIds.insert(shardId);
        auto [shard, inserted] = ShardsInfo.try_emplace(shardId);
        auto& shardInfo = shard->second;
        // Repeated registration of a DataShard is expected. Its tablet kind is fixed.
        // Column tables may share a tablet, so a ColumnShard has no row partition metadata.
        YDB_LOG_ENSURE(inserted || shardInfo.IsOlap == isOlap);
        YDB_LOG_ENSURE(!isOlap || (!meta.IsKnown() && !meta.PathId && meta.Borders.From.empty() && meta.Borders.To.empty()));
        shardInfo.IsOlap = isOlap;
        HasOlapTableShard |= isOlap;

        const auto [stringsIter, _] = TablePathes.insert(path);
        const TStringBuf pathBuf = *stringsIter;
        shardInfo.Pathes.insert(pathBuf);

        if (isOlap) {
            return;
        }
        auto& stored = ShardMetas[shardId];
        if (inserted || !stored.PathId) {
            // A plain read may arrive before the first writer supplies a path, or
            // after this tablet was retired. In the latter case its path is still known.
            if (!stored.PathId) {
                stored.PathId = pathId;
            }
            if (stored.PathId) {
                RowTables[stored.PathId].Shards.insert(shardId);
            }
        }

        // Keep the freshest known metadata: a shard discovered through several
        // paths (write batch, lock echo) may arrive with the same borders from
        // different partitioning snapshots; only the version moves forward.
        // A partitioning can supply borders even when its version is unknown.
        // Those borders still identify the creation range of a later lock echo.
        if (meta.IsKnown() || !meta.Borders.From.empty()) {
            YDB_LOG_ENSURE(!meta.PathId || meta.PathId == stored.PathId,
                {"registered", stored.PathId}, {"incoming", meta.PathId});
            if (stored.PathId && meta.IsKnown()) {
                auto& version = RowTables[stored.PathId].PartitioningVersion;
                version = Max(version, meta.PartitioningVersion);
            }
            if (stored.IsKnown() || !stored.Borders.From.empty()) {
                // Metadata may arrive as borders-with-version or version-only (the
                // caller had no resolved partitioning at hand). Compare borders only
                // when both sides carry them. A version-only meta describes a shard
                // absent from the caller's partitioning snapshot (e.g. a shard removed
                // by a split/merge): it carries no borders, so it must not move the
                // version of a stored meta that has borders — the stored version must
                // keep matching the stored borders to identify an older partitioning
                // when checking the retired participant.
                const bool bordersKnown = stored.Borders.From.size() > 0 || stored.Borders.To.size() > 0;
                const bool incomingBorders = meta.Borders.From.size() > 0 || meta.Borders.To.size() > 0;
                if (bordersKnown && incomingBorders) {
                    YDB_LOG_ENSURE(SameBorders(stored.Borders, meta.Borders),
                        {"shardId", shardId},
                        {"storedFrom", stored.Borders.From.size()},
                        {"storedTo", stored.Borders.To.size()},
                        {"storedVersion", stored.PartitioningVersion},
                        {"incomingFrom", meta.Borders.From.size()},
                        {"incomingTo", meta.Borders.To.size()},
                        {"incomingVersion", meta.PartitioningVersion});
                }
                if (incomingBorders) {
                    // Borders always belong to the partitioning snapshot they came
                    // from: when they are adopted, take the incoming version with
                    // them instead of mixing versions across snapshots. When the
                    // incoming borders match the stored ones (the usual echo of an
                    // older snapshot), the stored version must not move backwards.
                    stored.Borders = meta.Borders;
                    if (bordersKnown) {
                        stored.PartitioningVersion = Max(stored.PartitioningVersion, meta.PartitioningVersion);
                    } else {
                        stored.PartitioningVersion = meta.PartitioningVersion;
                    }
                } else if (!bordersKnown) {
                    stored.PartitioningVersion = Max(stored.PartitioningVersion, meta.PartitioningVersion);
                }
            } else {
                stored.Borders = meta.Borders;
                stored.PartitioningVersion = meta.PartitioningVersion;
            }
        }
    }

    void AddAction(ui64 shardId, ui8 action) override {
        AddAction(shardId, action, 0);
    }

    void AddAction(ui64 shardId, ui8 action, ui64 querySpanId) override {
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        auto& shardInfo = ShardsInfo.at(shardId);
        shardInfo.Flags |= action;
        if (action & (EAction::WRITE | EAction::PESSIMISTIC_LOCK)) {
            ReadOnly = false;
            // Track all QuerySpanIds of queries that wrote to this shard
            AddBreakerQuerySpanId(shardInfo, querySpanId);
        }
        ++ActionsCount;
    }

    void AddTopic(ui64 topicId, const TString& path) override {
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
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

    bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lockProto, ui64 querySpanId,
            ui64 deferredVictimQuerySpanId) override {
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING || State == ETransactionState::ERROR);
        TKqpLock lock(lockProto);
        bool isError = (lock.Proto.GetCounter() >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin);
        bool isInvalidated = (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken)
                            || (lock.Proto.GetCounter() == NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken);
        // CreatedOn identifies the logical lock whose WSN the shard acknowledged.
        YDB_LOG_ENSURE(lock.Proto.GetCreatedOn() || !lock.Proto.WriteSeqNumsSize(),
            {"lockId", lock.Proto.GetLockId()});
        bool isLocksAcquireFailure = isError && !isInvalidated;
        bool broken = false;

        // For broken locks from the shard (error counter), prefer deferredVictimQuerySpanId
        // which the shard computed based on whether the lock was already broken or deferred.
        // For non-error locks, use querySpanId (the current query that set the lock).
        ui64 effectiveVictimSpanId = (isError && deferredVictimQuerySpanId != 0)
            ? deferredVictimQuerySpanId : querySpanId;

        auto& shardInfo = ShardsInfo.at(shardId);
        // Plain read locks may omit CreatedOn. When present, copies with the same
        // owner must agree on whether the chain was created on that owner.
        if (lock.Proto.GetCreatedOn()) {
            const auto ownerKey = std::make_tuple(lock.Proto.GetLockId(), lock.Proto.GetDataShard(),
                lock.Proto.GetSchemeShard(), lock.Proto.GetPathId());
            const bool createdOnOwner = lock.Proto.GetCreatedOn() == lock.Proto.GetDataShard();
            auto [mode, inserted] = IsCreatedOnOwner.emplace(ownerKey, createdOnOwner);
            isInvalidated |= !inserted && mode->second != createdOnOwner;
        }
        if (auto lockPtr = shardInfo.Locks.FindPtr(lock.GetKey()); lockPtr) {
            if (lock.Proto.GetHasWrites()) {
                lockPtr->Lock.Proto.SetHasWrites(true);
            }
            // Only a reply from the creation tablet must be monotonic. Successors
            // can echo different branch maxima for the same writer.
            const bool nonDecreasing = lockPtr->Lock.MergeWriteSeqNums(lock.Proto);
            isInvalidated |= lock.Proto.GetCreatedOn() == shardId && !nonDecreasing;

            lockPtr->LocksAcquireFailure |= isLocksAcquireFailure;
            if (!lockPtr->LocksAcquireFailure) {
                isInvalidated |= lockPtr->Lock.Invalidated(lock);
                lockPtr->Invalidated |= isInvalidated;
            }
            broken = lockPtr->Invalidated || lockPtr->LocksAcquireFailure;

            if (broken && isInvalidated) {
                // Prefer the lock's original VictimQuerySpanId (set when the lock was first created
                // during the read that established it). This matches what the DataShard recorded.
                // Only use effectiveVictimSpanId when the shard explicitly provided a deferred victim
                // (deferredVictimQuerySpanId != 0), or as a fallback when the lock has no SpanId.
                // Without this, commit-phase AddLock would use the write's querySpanId, breaking
                // the linkage between SessionActor and DataShard TLI records.
                ui64 victimSpanId = lockPtr->VictimQuerySpanId;
                if (isError && deferredVictimQuerySpanId != 0) {
                    victimSpanId = effectiveVictimSpanId;
                } else if (victimSpanId == 0 && isError && effectiveVictimSpanId != 0) {
                    victimSpanId = effectiveVictimSpanId;
                }
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
            YDB_LOG_ENSURE(false);
        }

        return true;
    }

    ui64 GetTablePartitioningVersion(const TPathId& pathId) const override {
        const auto* table = RowTables.FindPtr(pathId);
        return table ? table->PartitioningVersion : 0;
    }

    bool UpdateTableParticipants(const TPathId& pathId, const TShardPartitionIndex& index,
            const TVector<NScheme::TTypeInfo>& keyTypes) override {
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING);
        const auto& partitioning = index.GetPartitioning();
        YDB_LOG_ENSURE(partitioning);
        auto& table = RowTables[pathId];
        table.PartitioningVersion = Max(table.PartitioningVersion, partitioning->GetVersion());
        if (table.Shards.empty()) {
            return true;
        }
        // Snapshot ids before adding successors and removing retired tablets.
        // Each table resolve visits only that table's current participants.
        const auto tableShards = table.Shards;
        YDB_LOG_ENSURE(!keyTypes.empty());
        struct TExpectedLock {
            TShardInfo::TLockInfo Info;
            TActionFlags Flags;
            TOwnedTableRange CreationRange{};
        };
        THashMap<TKqpLock::TKey, TExpectedLock> expected;
        THashSet<TStringBuf> paths;
        TVector<ui64> retired;
        // Several physical participants may hold the same logical lock. Merge
        // their observations before replacing any of the old participant entries.
        for (const ui64 shardId : tableShards) {
            auto& shard = ShardsInfo.at(shardId);
            const bool isRetired = index.FindPartitionIndex(shardId) == partitioning->Size();
            for (auto& [key, lock] : shard.Locks) {
                const auto& proto = lock.Lock.Proto;
                const TPathId lockPathId(proto.GetSchemeShard(), proto.GetPathId());
                if (lockPathId != pathId) {
                    // Removing the participant would discard the other table's lock.
                    YDB_LOG_ENSURE(!isRetired, {"shardId", shardId}, {"pathId", pathId}, {"lockPathId", lockPathId});
                    continue;
                }
                // Copy the lock only when this logical key is first seen.
                auto [entry, inserted] = expected.try_emplace(key, lock, shard.Flags);
                if (!inserted) {
                    entry->second.Flags |= shard.Flags;
                    if (!MergeLockInfo(entry->second.Info, lock)) {
                        lock.Invalidated = true;
                        SetVictimQuerySpanId(entry->second.Info.VictimQuerySpanId);
                        MakeLocksIssue(shard);
                        return false;
                    }
                }
                if (lock.Invalidated || lock.LocksAcquireFailure) {
                    lock.Invalidated = true;
                    SetVictimQuerySpanId(entry->second.Info.VictimQuerySpanId);
                    MakeLocksIssue(shard);
                    return false;
                }
                if (inserted) {
                    // Resolve once per logical key, while the old participant set is intact.
                    entry->second.CreationRange = GetCreationRange(proto);
                }
            }
            paths.insert(shard.Pathes.begin(), shard.Pathes.end());
            if (isRetired) {
                if (shard.Flags) {
                    // Roles follow the physical shard range, which may be narrower than a lock's creation range.
                    const auto& meta = GetShardMeta(shardId);
                    YDB_LOG_ENSURE(!meta.Borders.From.empty(), {"shardId", shardId}, {"pathId", pathId});
                    const auto version = partitioning->GetVersion();
                    YDB_LOG_ENSURE(!meta.IsKnown() || !version || meta.PartitioningVersion < version,
                        {"shardId", shardId}, {"shardVersion", meta.PartitioningVersion}, {"resolvedVersion", version});
                }
                retired.push_back(shardId);
            }
        }
        if (expected.empty() && retired.empty()) {
            return true;
        }

        YDB_LOG_ENSURE(!paths.empty());
        const TString path(*paths.begin());
        THashSet<ui64> registeredTargets;
        auto targetFor = [&](ui64 shardId) -> TShardInfo& {
            // Roles and many logical locks may select the same target. Register
            // its current borders once, including targets with no writer batch.
            if (registeredTargets.insert(shardId).second) {
                AddShard(shardId, false, path, pathId, MakeShardMeta(pathId, index, shardId, keyTypes.size()));
            }
            return ShardsInfo.at(shardId);
        };

        for (const ui64 shardId : retired) {
            auto from = std::move(ShardsInfo.at(shardId));
            ShardsInfo.erase(shardId);
            ShardsIds.erase(shardId);
            YDB_LOG_ENSURE(table.Shards.erase(shardId));
            YDB_LOG_ENSURE(!from.IsOlap && from.State == EShardState::PROCESSING);
            if (from.Flags) {
                // Roles and their query spans follow the retired tablet's physical
                // range, even when no lock or pending write covers a successor.
                const auto targets = partitioning->GetIntersectionWithRange(keyTypes, GetShardMeta(shardId).Borders);
                YDB_LOG_ENSURE(!targets.empty());
                for (const auto& target : targets) {
                    auto& to = targetFor(target.ShardId);
                    to.Flags |= from.Flags;
                    to.Pathes.insert(from.Pathes.begin(), from.Pathes.end());
                    for (const ui64 spanId : from.BreakerQuerySpanIds) {
                        AddBreakerQuerySpanId(to, spanId);
                    }
                }
                ShardsTransferred = true;
            }
        }
        if (expected.empty()) {
            return true;
        }
        // Surviving tablets keep their roles. Replace their copies only after
        // validating the whole table, so a rejected merge can still roll back.
        for (const ui64 shardId : tableShards) {
            if (auto* shard = ShardsInfo.FindPtr(shardId)) {
                for (auto it = shard->Locks.begin(); it != shard->Locks.end();) {
                    const auto current = it++;
                    const auto& proto = current->second.Lock.Proto;
                    if (TPathId(proto.GetSchemeShard(), proto.GetPathId()) == pathId) {
                        shard->Locks.erase(current);
                    }
                }
            }
        }
        for (const auto& [key, lock] : expected) {
            // CreatedOn is part of the logical key, so every copy uses the same
            // original range, including partitions with no pending rows to write.
            const auto intersections = partitioning->GetIntersectionWithRange(keyTypes, lock.CreationRange);
            YDB_LOG_ENSURE(!intersections.empty());
            for (const auto& intersection : intersections) {
                auto& target = targetFor(intersection.ShardId);
                target.Pathes.insert(paths.begin(), paths.end());
                target.Locks.emplace(key, lock.Info);
                // Empty copies need the same commit role as the original lock.
                auto flags = lock.Flags;
                if (!flags) {
                    flags = lock.Info.Lock.Proto.GetHasWrites() ? EAction::WRITE : EAction::READ;
                }
                target.Flags |= flags;
                ReadOnly &= !(flags & (EAction::WRITE | EAction::PESSIMISTIC_LOCK));
            }
        }
        ShardsTransferred = true;
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

    const TShardMeta& GetShardMeta(ui64 shardId) const override {
        if (const auto* meta = ShardMetas.FindPtr(shardId)) {
            return *meta;
        }
        static const TShardMeta unknown;
        return unknown;
    }

    EShardState GetState(ui64 shardId) const override {
        return ShardsInfo.at(shardId).State;
    }

    bool HasShard(ui64 shardId) const override {
        return ShardsInfo.contains(shardId);
    }

    void SetError(ui64 shardId) override {
        auto shardIter = ShardsInfo.find(shardId);
        if (shardIter != std::end(ShardsInfo)) {
            shardIter->second.State = EShardState::ERROR;
        }
    }

    void SetError() override {
        State = ETransactionState::ERROR;
    }

    void SetTableMeta(const TTableId tableId, TTableMeta tableMeta) override {
        TableMetas[tableId] = std::move(tableMeta);
    }

    TTableMeta GetTableMeta(const TTableId tableId) const override {
        auto iterator = TableMetas.find(tableId);
        if (iterator != std::end(TableMetas)) {
            return iterator->second;
        }
        return {};
    }

    void AddParticipantNode(const ui32 nodeId) override {
        ParticipantNodes.insert(nodeId);
    }

    const THashSet<ui32>& GetParticipantNodes() const override {
        return ParticipantNodes;
    }

    void SetTopicOperations(NTopic::TTopicOperations&& topicOperations) override {
        YDB_LOG_ENSURE(TopicOperations.GetSize() == 0);
        TopicOperations = std::move(topicOperations);
        TopicOperations.SetSkipConflictCheck(SkipTopicsConflictCheck);
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

    void SetSkipTopicsConflictCheck(bool skipConflictCheck) override {
        SkipTopicsConflictCheck = skipConflictCheck;
        TopicOperations.SetSkipConflictCheck(SkipTopicsConflictCheck);
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
            if (!ParticipatesInCommit(shardInfo)) {
                continue;
            }
            if (shardInfo.State != EShardState::PREPARED) {
                return false;
            }
        }
        return true;
    }

    bool IsTxFinished() const override {
        for (const auto& [_, shardInfo] : ShardsInfo) {
            if (!ParticipatesInCommit(shardInfo)) {
                continue;
            }
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
        return GetParticipatingShardsCount() == 1;
    }

    bool HasOlapTable() const override {
        return HasOlapTableShard;
    }

    bool IsEmpty() const override {
        return GetParticipatingShardsCount() == 0;
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

    void SetIsolationLevel(NKqpProto::EIsolationLevel level) override {
        IsolationLevel = level;
    }

    NKqpProto::EIsolationLevel GetIsolationLevel() const override {
        return IsolationLevel;
    }

    bool CanUseImmediateCommit() const override {
        return IsSingleShard() && !HasOlapTable()
            && GetTopicOperations().GetSize() <= 1
            && IsolationLevel != NKqpProto::ISOLATION_LEVEL_STRICT_SERIALIZABLE;
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

    THashSet<ui64> GetParticipatingShards() const override {
        THashSet<ui64> result;
        for (const auto& [shardId, shardInfo] : ShardsInfo) {
            if (ParticipatesInCommit(shardInfo)) {
                result.insert(shardId);
            }
        }
        return result;
    }

    ui64 GetShardsCount() const override {
        return ShardsIds.size();
    }

    bool NeedCommit() const override {
        YDB_LOG_ENSURE(ActionsCount != 1 || IsSingleShard() || ShardsTransferred);
        YDB_LOG_ENSURE(HasSnapshot() || IsolationLevel != NKqpProto::ISOLATION_LEVEL_READ_COMMITTED_RW);
        const bool dontNeedCommit = IsEmpty() || (IsReadOnly() && ((ActionsCount == 1) || HasSnapshot()));
        return !dontNeedCommit;
    }

    virtual ui64 GetCoordinator() const override {
        return Coordinator;
    }

    void StartPrepare() override {
        YDB_LOG_ENSURE(!CollectOnly);
        YDB_LOG_ENSURE(State == ETransactionState::COLLECTING);
        YDB_LOG_ENSURE(NeedCommit());
        YDB_LOG_ENSURE(!BrokenLocks());

        THashSet<ui64> sendingColumnShardsSet;
        THashSet<ui64> receivingColumnShardsSet;

        // IsVolatile() may count all participants. Their membership is fixed during prepare.
        const bool isVolatile = IsVolatile();
        for (auto& [shardId, shardInfo] : ShardsInfo) {
            if (!ParticipatesInCommit(shardInfo)) {
                // Phantom shard holds no reads/writes/locks and does not take
                // part in the distributed commit; it stays in PROCESSING.
                YDB_LOG_ENSURE(shardInfo.State == EShardState::PROCESSING);
                continue;
            }
            if (shardInfo.Flags & (EAction::WRITE | EAction::PESSIMISTIC_LOCK)) {
                ReceivingShards.insert(shardId);
                if (shardInfo.IsOlap) {
                    receivingColumnShardsSet.insert(shardId);
                }
                if (isVolatile) {
                    SendingShards.insert(shardId);
                }
            }
            if (!shardInfo.Locks.empty() || (shardInfo.Flags & EAction::READ)) {
                SendingShards.insert(shardId);
                if (shardInfo.IsOlap) {
                    sendingColumnShardsSet.insert(shardId);
                }
            }

            YDB_LOG_ENSURE(shardInfo.State == EShardState::PROCESSING);
            shardInfo.State = EShardState::PREPARING;
        }

        constexpr size_t minArbiterMeshSize = 5;
        if (isVolatile && ReceivingShards.size() >= minArbiterMeshSize) {
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
            YDB_LOG_ENSURE(!isVolatile);
            const auto& shards = receivingColumnShardsSet.empty()
                ? sendingColumnShardsSet
                : receivingColumnShardsSet;

            const ui32 index = RandomNumber<ui32>(shards.size());
            auto arbiterIterator = std::begin(shards);
            std::advance(arbiterIterator, index);
            ArbiterColumnShard = *arbiterIterator;
            ReceivingShards.insert(*ArbiterColumnShard);
        }

        ShardsToWait = GetParticipatingShards();

        MinStep = std::numeric_limits<ui64>::min();
        MaxStep = std::numeric_limits<ui64>::max();
        Coordinator = 0;

        State = ETransactionState::PREPARING;
    }

    TPrepareInfo GetPrepareTransactionInfo() override {
        YDB_LOG_ENSURE(State == ETransactionState::PREPARING);
        YDB_LOG_ENSURE(!ReceivingShards.empty() || !SendingShards.empty());

        TPrepareInfo result {
            .SendingShards = SendingShards,
            .ReceivingShards = ReceivingShards,
            .Arbiter = Arbiter,
            .ArbiterColumnShard = ArbiterColumnShard,
        };

        return result;
    }

    bool ConsumePrepareTransactionResult(TPrepareResult&& result) override {
        YDB_LOG_ENSURE(State == ETransactionState::PREPARING);
        auto& shardInfo = ShardsInfo.at(result.ShardId);
        YDB_LOG_ENSURE(shardInfo.State == EShardState::PREPARING);
        shardInfo.State = EShardState::PREPARED;

        ShardsToWait.erase(result.ShardId);

        MinStep = std::max(MinStep, result.MinStep);
        MaxStep = std::min(MaxStep, result.MaxStep);

        if (result.Coordinator && !Coordinator) {
            Coordinator = result.Coordinator;
        }

        YDB_LOG_ENSURE(Coordinator && Coordinator == result.Coordinator,
            {"prevCoordinator", Coordinator},
            {"newCoordinator", result.Coordinator});

        return ShardsToWait.empty();
    }

    void StartExecute() override {
        YDB_LOG_ENSURE(!CollectOnly);
        YDB_LOG_ENSURE(State == ETransactionState::PREPARING
                || (State == ETransactionState::COLLECTING
                    && IsSingleShard()));
        YDB_LOG_ENSURE(NeedCommit());
        YDB_LOG_ENSURE(!BrokenLocks());
        // The chosen PREPARE path can have one receiver, including a lone
        // external lock owner. Validate the entry state of this commit path.
        YDB_LOG_ENSURE(State == ETransactionState::PREPARING || ReceivingShards.empty());
        State = ETransactionState::EXECUTING;

        for (auto& [_, shardInfo] : ShardsInfo) {
            if (!ParticipatesInCommit(shardInfo)) {
                YDB_LOG_ENSURE(shardInfo.State == EShardState::PROCESSING);
                continue;
            }
            YDB_LOG_ENSURE(shardInfo.State == EShardState::PREPARED
                || (shardInfo.State == EShardState::PROCESSING
                    && IsSingleShard()));
            shardInfo.State = EShardState::EXECUTING;
        }

        ShardsToWait = GetParticipatingShards();

    }

    TCommitInfo GetCommitInfo() override {
        YDB_LOG_ENSURE(State == ETransactionState::EXECUTING);
        TCommitInfo result;
        result.MinStep = MinStep;
        result.MaxStep = MaxStep;
        result.Coordinator = Coordinator;

        for (auto& [shardId, shardInfo] : ShardsInfo) {
            if (!ParticipatesInCommit(shardInfo)) {
                YDB_LOG_ENSURE(shardInfo.State == EShardState::PROCESSING);
                continue;
            }

            // The coordinator's affected-set flags only distinguish read (1) and
            // write (2), so PESSIMISTIC_LOCK must not leak through: it is
            // replaced with WRITE (breaks its own locks on commit), READ is
            // kept when the shard also read.
            ui32 flags = shardInfo.Flags & (EAction::READ | EAction::WRITE);
            // Every participant holds at least one action; a lock-only shard
            // always registers an action together with its locks (see AddLock
            // call sites).
            YDB_LOG_ENSURE(shardInfo.Flags != 0,
                {"shardId", shardId},
                {"lockCount", shardInfo.Locks.size()});
            flags |= (shardInfo.Flags & EAction::PESSIMISTIC_LOCK) ? EAction::WRITE : 0;
            const ui32 affectedFlags = flags;

            result.ShardsInfo.push_back(TCommitShardInfo{
                .ShardId = shardId,
                .AffectedFlags = affectedFlags,
            });

            YDB_LOG_ENSURE(shardInfo.State == EShardState::EXECUTING);
        }
        return result;
    }

    bool ConsumeCommitResult(ui64 shardId) override {
        YDB_LOG_ENSURE(State == ETransactionState::EXECUTING);
        auto& shardInfo = ShardsInfo.at(shardId);
        YDB_LOG_ENSURE(shardInfo.State == EShardState::EXECUTING);
        shardInfo.State = EShardState::FINISHED;

        ShardsToWait.erase(shardId);
        return ShardsToWait.empty();
    }

    const THashSet<ui64>& StartRollback() override {
        YDB_LOG_ENSURE(State != ETransactionState::ROLLINGBACK);
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
        YDB_LOG_ENSURE(State == ETransactionState::ROLLINGBACK);
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
        // Accumulate direct and inherited roles, even when repartitioning
        // leaves this participant without a local lock copy or pending writes.
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

    // CreatedOn can name a retired tablet. Its range is retained independently
    // of the current participant set and table partitioning.
    const TOwnedTableRange& GetCreationRange(const NKikimrDataEvents::TLock& lock) const {
        const ui64 createdOn = lock.GetCreatedOn() ? lock.GetCreatedOn() : lock.GetDataShard();
        const auto& range = GetShardMeta(createdOn).Borders;
        YDB_LOG_ENSURE(!range.From.empty(), {"lockId", lock.GetLockId()}, {"createdOn", createdOn});
        return range;
    }

    static bool MergeLockInfo(TShardInfo::TLockInfo& stored, const TShardInfo::TLockInfo& incoming) {
        stored.Invalidated |= incoming.Invalidated || stored.Lock.Invalidated(incoming.Lock);
        stored.LocksAcquireFailure |= incoming.LocksAcquireFailure;
        stored.Lock.Proto.SetHasWrites(stored.Lock.Proto.GetHasWrites() || incoming.Lock.Proto.GetHasWrites());
        stored.Lock.MergeWriteSeqNums(incoming.Lock.Proto);
        if (!stored.VictimQuerySpanId) {
            stored.VictimQuerySpanId = incoming.VictimQuerySpanId;
        }
        return !stored.Invalidated && !stored.LocksAcquireFailure;
    }

    static void AddBreakerQuerySpanId(TShardInfo& shardInfo, ui64 querySpanId) {
        if (querySpanId != 0 && shardInfo.BreakerQuerySpanIdsSet.emplace(querySpanId).second) {
            shardInfo.BreakerQuerySpanIds.push_back(querySpanId);
        }
    }

    static bool ParticipatesInCommit(const TShardInfo& shardInfo) {
        // Only shards with reads/writes/locks.
        return shardInfo.Flags != 0 || !shardInfo.Locks.empty();
    }

    ui64 GetParticipatingShardsCount() const {
        ui64 count = 0;
        for (const auto& [_, shardInfo] : ShardsInfo) {
            if (ParticipatesInCommit(shardInfo)) {
                ++count;
            }
        }
        return count;
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
    // Group participants by stable table path so ALTER does not split one
    // table's locks into separate buckets by schema version.
    struct TRowTable {
        THashSet<ui64> Shards;
        // Writers and lock actors resolve independently. A delayed reply must
        // not retire participants registered against a newer partitioning.
        ui64 PartitioningVersion = 0;
    };
    THashMap<TPathId, TRowTable> RowTables;
    // One set of borders per row tablet, including retired ones: a successor's
    // first lock echo may name a tablet whose last write ACK was lost before split.
    THashMap<ui64, TShardMeta> ShardMetas;

    std::unordered_set<TString> TablePathes;
    ui64 ActionsCount = 0;
    bool ShardsTransferred = false;

    THashSet<ui32> ParticipantNodes;

    THashMap<TTableId, TTableMeta> TableMetas;
    // Keyed by (LockId, Owner, SchemeShard, PathId), across all physical copies.
    // True means CreatedOn == Owner. A different mode for the same key invalidates
    // the transaction. Retain this check even after the original shard is retired.
    THashMap<std::tuple<ui64, ui64, ui64, ui64>, bool> IsCreatedOnOwner;

    bool AllowVolatile = false;
    bool ReadOnly = true;
    bool ValidSnapshot = false;
    bool HasOlapTableShard = false;
    NKqpProto::EIsolationLevel IsolationLevel = NKqpProto::ISOLATION_LEVEL_UNDEFINED;
    std::optional<NYql::TIssue> LocksIssue;
    std::optional<ui64> VictimQuerySpanId_;

    THashSet<ui64> SendingShards;
    THashSet<ui64> ReceivingShards;
    std::optional<ui64> Arbiter;
    std::optional<ui64> ArbiterColumnShard;

    THashSet<ui64> ShardsToWait;

    NTopic::TTopicOperations TopicOperations;
    bool SkipTopicsConflictCheck = false;

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
