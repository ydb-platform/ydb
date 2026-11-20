#include "datashard_snapshots.h"

#include "datashard_impl.h"
#include "datashard_write_operation.h"
#include "snapshot_retention/snapshot_retention.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>

#include <util/stream/output.h>

#include <algorithm>

namespace NKikimr {
namespace NDataShard {

void TSnapshotManager::Reset() {
    MinWriteVersion = TRowVersion::Min();

    KeepSnapshotTimeout = 0;
    IncompleteEdge = TRowVersion::Min();
    CompleteEdge = TRowVersion::Min();
    LowWatermark = TRowVersion::Min();
    SnapshotWriteSafetyBoundary = TRowVersion::Min();
    ImmediateWriteEdge = TRowVersion::Min();
    ImmediateWriteEdgeReplied = TRowVersion::Min();
    UnprotectedReadEdge = TRowVersion::Min();

    CommittedCompleteEdge = TRowVersion::Min();
    FollowerReadEdge = TRowVersion::Min();
    FollowerReadEdgeRepeatable = false;
    Self->SetCounter(COUNTER_MVCC_SNAPSHOT_REGISTRY_WAITING, 0);
    Self->SetCounter(COUNTER_MVCC_SNAPSHOT_REGISTRY_CLEANUP_LAG_MS, 0);

    Snapshots.clear();
}

bool TSnapshotManager::Reload(NIceDb::TNiceDb& db) {
    // First-time MVCC migration needs the persistent points before it can
    // record which legacy history is unavailable.
    if (!ReloadSnapshots(db) || !ReloadSys(db)) {
        return false;
    }

    Self->SetCounter(COUNTER_MVCC_ENABLED, 1);
    return true;
}

bool TSnapshotManager::ReloadSys(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    bool ready = true;

    TRowVersion minWriteVersion = TRowVersion::Min();
    TRowVersion completeEdge = TRowVersion::Min();
    TRowVersion incompleteEdge = TRowVersion::Min();
    TRowVersion lowWatermark = TRowVersion::Min();
    TRowVersion snapshotWriteSafetyBoundary = TRowVersion::Min();
    TRowVersion immediateWriteEdge = TRowVersion::Min();
    TRowVersion followerReadEdge = TRowVersion::Min();

    ui32 mvccState = 0;
    ui64 keepSnapshotTimeout = 0;
    ui64 unprotectedReads = 0;
    ui64 followerReadEdgeRepeatable = 0;
    ui64 snapshotWriteSafetyGeneration = 0;

    ready &= Self->SysGetUi64(db, Schema::Sys_MinWriteVersionStep, minWriteVersion.Step);
    ready &= Self->SysGetUi64(db, Schema::Sys_MinWriteVersionTxId, minWriteVersion.TxId);

    ready &= Self->SysGetUi64(db, Schema::SysMvcc_State, mvccState);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_KeepSnapshotTimeout, keepSnapshotTimeout);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_UnprotectedReads, unprotectedReads);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_CompleteEdgeStep, completeEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_CompleteEdgeTxId, completeEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_IncompleteEdgeStep, incompleteEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_IncompleteEdgeTxId, incompleteEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_LowWatermarkStep, lowWatermark.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_LowWatermarkTxId, lowWatermark.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_SnapshotWriteSafetyStep, snapshotWriteSafetyBoundary.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_SnapshotWriteSafetyTxId, snapshotWriteSafetyBoundary.TxId);
    if (!Self->IsFollower()) {
        ready &= Self->SysGetUi64(db, Schema::SysMvcc_SnapshotWriteSafetyGeneration, snapshotWriteSafetyGeneration);
    }
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_ImmediateWriteEdgeStep, immediateWriteEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_ImmediateWriteEdgeTxId, immediateWriteEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeStep, followerReadEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeTxId, followerReadEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeRepeatable, followerReadEdgeRepeatable);

    if (ready) {
        // We have a consistent view of settings, apply
        MinWriteVersion = minWriteVersion;
        KeepSnapshotTimeout = keepSnapshotTimeout;
        CompleteEdge = completeEdge;
        IncompleteEdge = incompleteEdge;
        LowWatermark = lowWatermark;
        SnapshotWriteSafetyBoundary = snapshotWriteSafetyBoundary;
        ImmediateWriteEdge = immediateWriteEdge;
        if (ImmediateWriteEdge.Step <= Max(CompleteEdge.Step, IncompleteEdge.Step)) {
            ImmediateWriteEdgeReplied = immediateWriteEdge;
        } else {
            // We cannot be sure which writes we have replied to
            // Datashard will restore mediator state and decide
            ImmediateWriteEdgeReplied.Step = Max(CompleteEdge.Step, IncompleteEdge.Step);
            ImmediateWriteEdgeReplied.TxId = Max<ui64>();
        }
        CommittedCompleteEdge = completeEdge;
        FollowerReadEdge = followerReadEdge;
        FollowerReadEdgeRepeatable = (followerReadEdgeRepeatable != 0);

        if (!Self->IsFollower()) {
            // Handle ancient shards that didn't have mvcc enabled for some reason
            // Note: all shards should have switched to mvcc in previous versions,
            // but there could be some frozen etc. shards that didn't. This code
            // is not very precise, but should be good enough.
            if (mvccState != (ui32)EMvccState::MvccEnabled) {
                // Next time we restart mvcc will be enabled
                Self->PersistSys(db, Schema::SysMvcc_State, (ui32)EMvccState::MvccEnabled);
                // Choose the maximum version this shard could have written at
                TRowVersion nextVersion = Max(
                    Self->LastCompleteTxVersion(),
                    MinWriteVersion,
                    CompleteEdge,
                    IncompleteEdge,
                    ImmediateWriteEdge);
                if (nextVersion) {
                    nextVersion.TxId = Max<ui64>();
                }
                SetCompleteEdge(db, nextVersion);
                SetIncompleteEdge(db, nextVersion);
                SetImmediateWriteEdge(db, nextVersion);
                // Legacy rows may be unversioned. Record unavailable history
                // below the new watermark even when the registry is unready.
                // Explicit persistent snapshots remain readable.
                RemoveRowVersions(db.GetDatabase(), TRowVersion::Min(), nextVersion);
                SetLowWatermark(db, nextVersion);
                ImmediateWriteEdgeReplied = ImmediateWriteEdge;
            }

            const ui64 generation = Self->Generation();
            if (!snapshotWriteSafetyGeneration || snapshotWriteSafetyGeneration + 1 < generation) {
                // Before this generation, an old compactor may have removed
                // the last committed version of an absent key. Even a retained
                // read snapshot cannot prove there were no subsequent writes.
                const auto lastPlanned = Self->Pipeline.GetLastPlannedTx();
                RaiseSnapshotWriteSafetyBoundary(db, Max(LowWatermark, MinWriteVersion,
                    CompleteEdge, IncompleteEdge, ImmediateWriteEdge, Self->LastCompleteTxVersion(),
                    TRowVersion(lastPlanned.Step, lastPlanned.TxId)));
            }
            if (snapshotWriteSafetyGeneration != generation) {
                Self->PersistSys(db, Schema::SysMvcc_SnapshotWriteSafetyGeneration, generation);
            }

            // We had UnprotectedReads enabled by default for a long time, but
            // delayed persistently marking shard until the first snapshot read.
            // We assume unprotected reads unconditionally now, but make sure
            // order versions start up correctly too.
            if (!unprotectedReads && Self->IsStateNewReadAllowed()) {
                Self->PersistUnprotectedReadsEnabled(db);
            }
        }
    }

    return ready;
}

bool TSnapshotManager::ReloadSnapshots(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    TSnapshotMap snapshots;

    auto rowset = db.Table<Schema::Snapshots>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        ui64 oid = rowset.GetValue<Schema::Snapshots::Oid>();
        ui64 tid = rowset.GetValue<Schema::Snapshots::Tid>();
        ui64 step = rowset.GetValue<Schema::Snapshots::Step>();
        ui64 txId = rowset.GetValue<Schema::Snapshots::TxId>();
        TString name = rowset.GetValue<Schema::Snapshots::Name>();
        ui64 flags = rowset.GetValue<Schema::Snapshots::Flags>();
        ui64 timeout_ms = rowset.GetValue<Schema::Snapshots::TimeoutMs>();

        TSnapshotKey key(oid, tid, step, txId);

        auto res = snapshots.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(key, std::move(name), flags, TDuration::MilliSeconds(timeout_ms)));
        Y_ENSURE(res.second, "Unexpected duplicate snapshot: " << key);

        if (!rowset.Next()) {
            return false;
        }
    }

    Snapshots = std::move(snapshots);
    MarkSnapshotsWithRemovedHistory(db.GetDatabase());
    return true;
}

void TSnapshotManager::MarkSnapshotsWithRemovedHistory(NTable::TDatabase& db) {
    // A legacy TRUNCATE could destroy history without discarding its persistent record.
    // The leader's normal startup cleanup erases records marked here.
    for (auto& [key, snapshot] : Snapshots) {
        if (snapshot.HasFlags(TSnapshot::FlagRemoved) || key.OwnerId != Self->GetPathOwnerId()) {
            continue;
        }
        const auto* table = Self->GetUserTables().FindPtr(key.PathId);
        const TRowVersion version(key.Step, key.TxId);
        if (table && db.GetRemovedRowVersions((*table)->LocalTid).Contains(version, version.Next())) {
            snapshot.Flags |= TSnapshot::FlagRemoved;
            if (ExpireQueue.Has(&snapshot)) {
                ExpireQueue.Remove(&snapshot);
            }
        }
    }
}

void TSnapshotManager::InitExpireQueue(TInstant now) {
    if (ExpireQueue) {
        // Remove all snapshots already in the queue
        while (auto* snapshot = ExpireQueue.Top()) {
            ExpireQueue.Remove(snapshot);
        }
    }

    for (auto& kv : Snapshots) {
        if (kv.second.HasFlags(TSnapshot::FlagTimeout) &&
            !kv.second.HasFlags(TSnapshot::FlagRemoved) &&
            !References.contains(kv.first))
        {
            kv.second.ExpireTime = now + kv.second.Timeout;
            ExpireQueue.Add(&kv.second);
        }
    }
}

TRowVersion TSnapshotManager::GetMinWriteVersion() const {
    return MinWriteVersion;
}

void TSnapshotManager::SetMinWriteVersion(NIceDb::TNiceDb& db, TRowVersion writeVersion) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::Sys_MinWriteVersionStep, writeVersion.Step);
    Self->PersistSys(db, Schema::Sys_MinWriteVersionTxId, writeVersion.TxId);
    MinWriteVersion = writeVersion;
}

TRowVersion TSnapshotManager::GetCompleteEdge() const {
    return CompleteEdge;
}

TRowVersion TSnapshotManager::GetCommittedCompleteEdge() const {
    return CommittedCompleteEdge;
}

void TSnapshotManager::SetCompleteEdge(NIceDb::TNiceDb& db, const TRowVersion& version) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::SysMvcc_CompleteEdgeStep, version.Step);
    Self->PersistSys(db, Schema::SysMvcc_CompleteEdgeTxId, version.TxId);
    CompleteEdge = version;
}

bool TSnapshotManager::PromoteCompleteEdge(const TRowVersion& version, TTransactionContext& txc) {
    if (CompleteEdge >= version)
        return false;

    NIceDb::TNiceDb db(txc.DB);
    SetCompleteEdge(db, version);
    txc.DB.OnPersistent([this, edge = CompleteEdge] {
        this->CommittedCompleteEdge = edge;
    });

    return true;
}

bool TSnapshotManager::PromoteCompleteEdge(TOperation* op, TTransactionContext& txc) {
    Y_ASSERT(op && (op->IsMvccSnapshotRead() || op->GetStep()));

    const TRowVersion version = op->IsMvccSnapshotRead()
        ? op->GetMvccSnapshot()
        : TRowVersion(op->GetStep(), op->GetTxId());

    return PromoteCompleteEdge(version, txc);
}

bool TSnapshotManager::PromoteCompleteEdge(ui64 writeStep, TTransactionContext& txc) {
    return PromoteCompleteEdge(TRowVersion(writeStep, 0), txc);
}

TRowVersion TSnapshotManager::GetIncompleteEdge() const {
    return IncompleteEdge;
}

void TSnapshotManager::SetIncompleteEdge(NIceDb::TNiceDb& db, const TRowVersion& version) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::SysMvcc_IncompleteEdgeStep, version.Step);
    Self->PersistSys(db, Schema::SysMvcc_IncompleteEdgeTxId, version.TxId);
    IncompleteEdge = version;
}

bool TSnapshotManager::PromoteIncompleteEdge(TOperation* op, TTransactionContext& txc) {
    Y_ASSERT(op && op->GetStep());

    if (TRowVersion version(op->GetStep(), op->GetTxId()); version > IncompleteEdge) {
        NIceDb::TNiceDb db(txc.DB);
        SetIncompleteEdge(db, version);

        return true;
    }

    return false;
}

TRowVersion TSnapshotManager::GetImmediateWriteEdge() const {
    return ImmediateWriteEdge;
}

TRowVersion TSnapshotManager::GetImmediateWriteEdgeReplied() const {
    return ImmediateWriteEdgeReplied;
}

void TSnapshotManager::SetImmediateWriteEdge(NIceDb::TNiceDb& db, const TRowVersion& version) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::SysMvcc_ImmediateWriteEdgeStep, version.Step);
    Self->PersistSys(db, Schema::SysMvcc_ImmediateWriteEdgeTxId, version.TxId);
    ImmediateWriteEdge = version;
}

bool TSnapshotManager::PromoteImmediateWriteEdge(const TRowVersion& version, TTransactionContext& txc) {
    if (version > ImmediateWriteEdge) {
        NIceDb::TNiceDb db(txc.DB);
        SetImmediateWriteEdge(db, version);

        return true;
    }

    return false;
}

bool TSnapshotManager::PromoteImmediateWriteEdgeReplied(const TRowVersion& version) {
    if (version > ImmediateWriteEdgeReplied) {
        ImmediateWriteEdgeReplied = version;
        return true;
    }

    return false;
}

void TSnapshotManager::RestoreImmediateWriteEdge(const TRowVersion& version, const TRowVersion& replied) {
    // Note: ImmediateWriteEdge is persistent, we must not overwrite it
    Y_UNUSED(version);
    ImmediateWriteEdgeReplied = replied;
}

TRowVersion TSnapshotManager::GetUnprotectedReadEdge() const {
    return UnprotectedReadEdge;
}

bool TSnapshotManager::PromoteUnprotectedReadEdge(const TRowVersion& version) {
    if (UnprotectedReadEdge < version) {
        UnprotectedReadEdge = version;
        return true;
    }

    return false;
}

void TSnapshotManager::RestoreUnprotectedReadEdge(const TRowVersion& version) {
    UnprotectedReadEdge = version;
}

std::pair<TRowVersion, bool> TSnapshotManager::GetFollowerReadEdge() const {
    return { FollowerReadEdge, FollowerReadEdgeRepeatable };
}

bool TSnapshotManager::PromoteFollowerReadEdge(const TRowVersion& version, bool repeatable, TTransactionContext& txc) {
    using Schema = TDataShard::Schema;

    if (FollowerReadEdge < version) {
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSys(db, Schema::SysMvcc_FollowerReadEdgeStep, version.Step);
        Self->PersistSys(db, Schema::SysMvcc_FollowerReadEdgeTxId, version.TxId);
        Self->PersistSys(db, Schema::SysMvcc_FollowerReadEdgeRepeatable, ui64(repeatable ? 1 : 0));
        FollowerReadEdge = version;
        FollowerReadEdgeRepeatable = repeatable;
        return true;
    } else if (FollowerReadEdge == version && repeatable && !FollowerReadEdgeRepeatable) {
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSys(db, Schema::SysMvcc_FollowerReadEdgeRepeatable, ui64(repeatable ? 1 : 0));
        FollowerReadEdgeRepeatable = repeatable;
        return true;
    }

    return false;
}

void TSnapshotManager::SetKeepSnapshotTimeout(NIceDb::TNiceDb& db, ui64 keepSnapshotTimeout) {
    using Schema = TDataShard::Schema;

    // TODO validate new value
    Self->PersistSys(db, Schema::SysMvcc_KeepSnapshotTimeout, keepSnapshotTimeout);
    KeepSnapshotTimeout = keepSnapshotTimeout;
}

TRowVersion TSnapshotManager::GetLowWatermark() const {
    return LowWatermark;
}

void TSnapshotManager::SetLowWatermark(NIceDb::TNiceDb& db, TRowVersion watermark) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::SysMvcc_LowWatermarkStep, watermark.Step);
    Self->PersistSys(db, Schema::SysMvcc_LowWatermarkTxId, watermark.TxId);

    LowWatermark = watermark;
}

bool TSnapshotManager::AdvanceWatermark(NTable::TDatabase& db, const TRowVersion& to) {
    if (LowWatermark >= to)
        return false;

    // Schema operations such as TRUNCATE invalidate old history regardless of
    // registry retention. Periodic MVCC cleanup uses CleanupRowVersions instead.
    RemoveRowVersions(db, LowWatermark, to);

    NIceDb::TNiceDb nicedb(db);
    SetLowWatermark(nicedb, to);
    db.OnCommit([self = Self] { self->PruneHnswIndexes(); });

    return true;
}

void TSnapshotManager::RemoveRowVersions(NTable::TDatabase& db, const TRowVersion& from, const TRowVersion& to) {
    for (auto& it : Self->GetUserTables()) {
        auto begin = Snapshots.lower_bound(TSnapshotKey(Self->GetPathOwnerId(), it.first, from.Step, from.TxId));
        auto end = Snapshots.upper_bound(TSnapshotKey(Self->GetPathOwnerId(), it.first, to.Step, to.TxId));
        TRowVersion from0 = from;
        for (auto it0 = begin; it0 != end && from0 < to; it0++) {
            auto to0 = TRowVersion(it0->first.Step, it0->first.TxId);
            if (from0 < to0)
                db.RemoveRowVersions(it.second->LocalTid, from0, to0);
            from0 = to0.Next();
        }
        if (from0 < to)
            db.RemoveRowVersions(it.second->LocalTid, from0, to);
    }
}

bool TSnapshotManager::IsSnapshotRetentionEnabled() const {
    return AppData()->FeatureFlags.GetEnableSnapshotsLocking()
        && AppData()->FeatureFlags.GetEnableDataShardSnapshotLocking();
}

void TSnapshotManager::RaiseSnapshotWriteSafetyBoundary(NIceDb::TNiceDb& db, const TRowVersion& version) {
    using Schema = TDataShard::Schema;
    if (SnapshotWriteSafetyBoundary < version) {
        Self->PersistSys(db, Schema::SysMvcc_SnapshotWriteSafetyStep, version.Step);
        Self->PersistSys(db, Schema::SysMvcc_SnapshotWriteSafetyTxId, version.TxId);
        SnapshotWriteSafetyBoundary = version;
    }
}

TSnapshotRetentionPolicy TSnapshotManager::GetSnapshotRetentionPolicy() const {
    return TSnapshotRetentionPolicy(IsSnapshotRetentionEnabled(),
        AppData()->SnapshotRegistryHolder, AppData()->LongTxServiceConfig);
}

void TSnapshotManager::SerializeRemovedRowVersions(const NTable::TRowVersionRanges& ranges,
    NKikimrTxDataShard::TMvccRemovedRowVersions& proto)
{
    for (const auto& range : ranges) {
        auto* item = proto.AddRanges();
        range.Lower.ToProto(item->MutableLowerInclusive());
        range.Upper.ToProto(item->MutableUpperExclusive());
    }
}

void TSnapshotManager::RestoreRemovedRowVersions(NTable::TDatabase& db, ui32 localTableId,
    const NKikimrTxDataShard::TMvccRemovedRowVersions& proto)
{
    for (const auto& range : proto.GetRanges()) {
        db.RemoveRowVersions(localTableId, TRowVersion::FromProto(range.GetLowerInclusive()),
            TRowVersion::FromProto(range.GetUpperExclusive()));
    }
}

bool TSnapshotManager::IsSnapshotReadable(NTable::TDatabase& db, const TTableId& tableId,
    const TRowVersion& version, bool locallyAcquired) const
{
    if (version >= LowWatermark) {
        return true;
    }

    // Followers keep their existing read window. Their local registry does not
    // prove that the leader preserved this version.
    if (Self->IsFollower()) {
        return false;
    }

    if (!locallyAcquired && !GetSnapshotRetentionPolicy().ShouldRetain(tableId, version)) {
        return false;
    }

    // LocalPathId is unique only within its owner. Retention or an existing
    // acquisition cannot make a foreign/stale path an alias for a local table,
    // even when its numeric id matches and physical history is still present.
    const auto* table = Self->GetUserTables().FindPtr(tableId.PathId.LocalPathId);
    if (tableId.PathId.OwnerId != Self->GetPathOwnerId() || !table) {
        return false;
    }

    return !db.GetRemovedRowVersions((*table)->LocalTid).Contains(version, version.Next());
}

TRowVersion TSnapshotManager::GetLeastAcquiredPipelineSnapshot() const {
    TRowVersion leastAcquired = TRowVersion::Max();
    auto acquire = [&](const TOperation::TPtr& op) {
        if (op->IsMvccSnapshotRead()) {
            leastAcquired = Min(leastAcquired, op->GetMvccSnapshot());
        }
        if (const auto* writeOp = TWriteOperation::TryCastWriteOperation(op)) {
            if (const auto& snapshot = writeOp->GetReadSnapshot()) {
                leastAcquired = Min(leastAcquired, *snapshot);
            }
        }
    };
    for (const auto& [txId, op] : Self->Pipeline.GetImmediateOps()) {
        acquire(op);
    }
    // Prepared transactions need their older read version before planning and
    // after their validated data has been released, including after a restart.
    for (const auto& [txId, op] : Self->TransQueue.GetTxsInFly()) {
        acquire(op);
    }
    return leastAcquired;
}

bool TSnapshotManager::CleanupRowVersions(NTable::TDatabase& db, const TRowVersion& from,
    const TRowVersion& proposed, const TSnapshotRetentionPolicy& policy)
{
    const TRowVersion to = Min(policy.GetCleanupBoundary(proposed), GetLeastAcquiredPipelineSnapshot());
    if (from >= to) {
        return false;
    }

    bool removed = false;
    // Reuse storage across tables for points from all retention sources.
    TVector<TRowVersion> retainedSnapshots;
    for (const auto& [pathId, table] : Self->GetUserTables()) {
        const TTableId tableId(Self->GetPathOwnerId(), pathId);
        retainedSnapshots.clear();
        policy.ForEachRetainedSnapshot(tableId, from, to, [&](const TRowVersion& version) {
            retainedSnapshots.push_back(version);
        });
        const size_t registryCount = retainedSnapshots.size();
        const auto persistentEnd = Snapshots.lower_bound(TSnapshotKey(tableId.PathId, to.Step, to.TxId));
        for (auto it = Snapshots.lower_bound(TSnapshotKey(tableId.PathId, from.Step, from.TxId));
            it != persistentEnd; ++it)
        {
            retainedSnapshots.emplace_back(it->first.Step, it->first.TxId);
        }
        // Registry and persistent points are already sorted.
        std::inplace_merge(retainedSnapshots.begin(), retainedSnapshots.begin() + registryCount,
            retainedSnapshots.end());
        const size_t sortedCount = retainedSnapshots.size();

        // Keep acquired points until their iterators drain, even if disabled.
        for (const auto& [readId, state] : Self->ReadIterators) {
            if (state.SnapshotAcquired && !state.IsFinished && state.PathId == tableId.PathId
                && from <= state.ReadVersion && state.ReadVersion < to)
            {
                retainedSnapshots.push_back(state.ReadVersion);
            }
        }
        // Iterator points are unordered; sort only their suffix before merging.
        std::sort(retainedSnapshots.begin() + sortedCount, retainedSnapshots.end());
        std::inplace_merge(retainedSnapshots.begin(), retainedSnapshots.begin() + sortedCount,
            retainedSnapshots.end());
        retainedSnapshots.erase(
            std::unique(retainedSnapshots.begin(), retainedSnapshots.end()), retainedSnapshots.end());

        const auto& ranges = db.GetRemovedRowVersions(table->LocalTid);
        auto removeRange = [&](const TRowVersion& lower, const TRowVersion& upper) {
            if (lower < upper && !ranges.Contains(lower, upper)) {
                db.RemoveRowVersions(table->LocalTid, lower, upper);
                removed = true;
            }
        };

        TRowVersion lower = from;
        for (const auto& version : retainedSnapshots) {
            removeRange(lower, version);
            lower = version.Next();
        }
        removeRange(lower, to);
    }
    return removed;
}

ui64 TSnapshotManager::GetKeepSnapshotTimeout() const {
    return KeepSnapshotTimeout ? KeepSnapshotTimeout : AppData()->DataShardConfig.GetKeepSnapshotTimeout();
}

TDuration TSnapshotManager::GetCleanupSnapshotPeriod() const {
    return TDuration::MilliSeconds(AppData()->DataShardConfig.GetCleanupSnapshotPeriod());
}

const TSnapshot* TSnapshotManager::FindAvailable(const TSnapshotKey& key) const {
    auto it = Snapshots.find(key);
    if (it != Snapshots.end() && !it->second.HasFlags(TSnapshot::FlagRemoved)) {
        return &it->second;
    }
    return nullptr;
}

TSnapshotKey TSnapshotManager::ExpandSnapshotKey(ui64 ownerId, ui64 pathId, ui64 step, ui64 txId) const {
    if (step == 0) {
        for (const auto& kv : GetSnapshots(TSnapshotTableKey(ownerId, pathId))) {
            // TODO: maybe make it more efficient
            if (kv.first.TxId == txId) {
                step = kv.first.Step;
                break;
            }
        }
    }

    return TSnapshotKey(ownerId, pathId, step, txId);
}

bool TSnapshotManager::AcquireReference(const TSnapshotKey& key) {
    auto it = Snapshots.find(key);
    if (it == Snapshots.end() || it->second.HasFlags(TSnapshot::FlagRemoved)) {
        return false;
    }

    // Snapshots don't expire while acquired
    if (ExpireQueue.Has(&it->second)) {
        ExpireQueue.Remove(&it->second);
    }

    ++References[key];
    return true;
}

bool TSnapshotManager::ReleaseReference(const TSnapshotKey& key, NTable::TDatabase& db, TInstant now) {
    auto refIt = References.find(key);

    if (Y_UNLIKELY(refIt == References.end() || refIt->second <= 0)) {
        Y_DEBUG_ABORT("ReleaseReference underflow, check acquire/release pairs");
        return false;
    }

    if (--refIt->second) {
        // There is some other reference
        return false;
    }

    References.erase(refIt);

    auto it = Snapshots.find(key);
    if (it == Snapshots.end()) {
        Y_DEBUG_ABORT("ReleaseReference on an already deleted snapshot");
        return false;
    }

    if (!it->second.HasFlags(TSnapshot::FlagRemoved)) {
        // Snapshot still valid, add back to expire queue if needed
        if (it->second.HasFlags(TSnapshot::FlagTimeout) &&
            !ExpireQueue.Has(&it->second))
        {
            it->second.ExpireTime = now + it->second.Timeout;
            ExpireQueue.Add(&it->second);
        }
        return false;
    }

    DoRemoveSnapshot(db, key);
    return true;
}

bool TSnapshotManager::AddSnapshot(NTable::TDatabase& db, const TSnapshotKey& key, const TString& name, ui64 flags, TDuration timeout) {
    if (auto it = Snapshots.find(key); it != Snapshots.end()) {
        Y_VERIFY_DEBUG_S(
            it->second.Name == name &&
            it->second.Flags == flags,
            "DataShard " << Self->TabletID() << " adding duplicate snapshot " << key
            << " with name=" << name << " and flags=" << flags
            << ", expected name=" << it->second.Name << " and flags=" << it->second.Flags);

        return false;
    }

    const TRowVersion oldVersion = MinWriteVersion;
    const TRowVersion newVersion(key.Step, key.TxId);

    // N.B. no colocated tables support for now
    Y_ENSURE(Self->GetUserTables().size() == 1, "Multiple co-located tables not supported");

    Y_ENSURE(oldVersion <= newVersion,
        "DataShard " << Self->TabletID()
        << " adding new snapshot " << newVersion
        << " below previous min write version " << oldVersion);

    NIceDb::TNiceDb nicedb(db);

    PersistAddSnapshot(nicedb, key, name, flags, timeout);

    // All future writes must happen past the snapshot version
    SetMinWriteVersion(nicedb, newVersion.Next());

    return true;
}

bool TSnapshotManager::RemoveSnapshot(NTable::TDatabase& db, const TSnapshotKey& key) {
    auto it = Snapshots.find(key);
    if (it == Snapshots.end() || it->second.HasFlags(TSnapshot::FlagRemoved)) {
        // Snapshot logically doesn't exist
        return false;
    }

    if (References.contains(key)) {
        NIceDb::TNiceDb nicedb(db);

        // Delay removal until some later time
        it->second.Flags |= TSnapshot::FlagRemoved;

        PersistUpdateSnapshotFlags(nicedb, key);

        return true;
    }

    DoRemoveSnapshot(db, key);

    return true;
}

void TSnapshotManager::DoRemoveSnapshot(NTable::TDatabase& db, const TSnapshotKey& key) {
    NIceDb::TNiceDb nicedb(db);

    // Release persistent retention. The common row-version cleanup reclaims
    // the point while respecting any remaining retention sources.
    PersistRemoveSnapshot(nicedb, key);
}

bool TSnapshotManager::CleanupRemovedSnapshots(NTable::TDatabase& db) {
    bool removed = false;

    for (auto it = Snapshots.begin(); it != Snapshots.end();) {
        if (!it->second.HasFlags(TSnapshot::FlagRemoved)) {
            ++it;
            continue;
        }

        const TSnapshotKey key = (it++)->first;
        if (References.contains(key)) {
            continue;
        }

        DoRemoveSnapshot(db, key);
        removed = true;
    }

    return removed;
}

void TSnapshotManager::InitSnapshotExpireTime(const TSnapshotKey& key, TInstant now) {
    auto it = Snapshots.find(key);
    if (it != Snapshots.end() &&
        it->second.HasFlags(TSnapshot::FlagTimeout) &&
        !it->second.HasFlags(TSnapshot::FlagRemoved) &&
        !ExpireQueue.Has(&it->second))
    {
        it->second.ExpireTime = now + it->second.Timeout;
        ExpireQueue.Add(&it->second);
    }
}

bool TSnapshotManager::RefreshSnapshotExpireTime(const TSnapshotKey& key, TInstant now) {
    auto it = Snapshots.find(key);
    if (it != Snapshots.end() &&
        ExpireQueue.Has(&it->second))
    {
        it->second.ExpireTime = now + it->second.Timeout;
        return ExpireQueue.Update(&it->second);
    }

    return false;
}

TDuration TSnapshotManager::CleanupTimeout() const {
    TInstant now = NActors::TActivationContext::Now();

    TDuration snapshotTimeout = TDuration::Max();
    TDuration mvccGcTimeout = TDuration::Max();

    if (auto* snapshot = ExpireQueue.Top())
        snapshotTimeout = snapshot->ExpireTime - now;

    if (LowWatermark >= CompleteEdge) {
        mvccGcTimeout = GetCleanupSnapshotPeriod();
    } else {
        mvccGcTimeout = TInstant::MilliSeconds(LowWatermark.Step + GetKeepSnapshotTimeout() + 1) - now;
        if (LastAdvanceWatermark) {
            // On startup we want to cleanup as soon as needed
            // But later on we don't want to have too frequent cleanups
            TMonotonic currentMonotonic = NActors::TActivationContext::Monotonic();
            TMonotonic nextAdvanceWatermark = LastAdvanceWatermark + GetCleanupSnapshotPeriod();
            if (currentMonotonic < nextAdvanceWatermark) {
                mvccGcTimeout = Max(mvccGcTimeout, nextAdvanceWatermark - currentMonotonic);
            }
        }
    }

    return Min(snapshotTimeout, mvccGcTimeout);
}

bool TSnapshotManager::RemoveExpiredSnapshots(TInstant now, TTransactionContext& txc) {
    bool removed = false;
    while (auto* snapshot = ExpireQueue.Top()) {
        if (now < snapshot->ExpireTime) {
            break;
        }

        const TSnapshotKey key = snapshot->Key;
        ExpireQueue.Remove(snapshot);
        snapshot = nullptr;

        if (RemoveSnapshot(txc.DB, key)) {
            removed = true;
        }
    }

    ui64 keepSnapshotTimeout = GetKeepSnapshotTimeout();
    TRowVersion proposed = TRowVersion(Max(now.MilliSeconds(), keepSnapshotTimeout) - keepSnapshotTimeout, 0);

    TRowVersion leastPlanned = TRowVersion::Max();
    if (auto it = Self->Pipeline.GetPlan().begin(); it != Self->Pipeline.GetPlan().end())
        leastPlanned = TRowVersion(it->Step, it->TxId);

    // Running pipeline operations still limit cleanup. Detached iterators are
    // protected as individual points by CleanupRowVersions.
    const TRowVersion leastAcquired = GetLeastAcquiredPipelineSnapshot();

    if (CompleteEdge.Step < ImmediateWriteEdgeReplied.Step) {
        PromoteCompleteEdge(ImmediateWriteEdgeReplied.Step, txc);
        Self->PromoteFollowerReadEdge(txc);
    }

    // Calculate the maximum version where we may have written something
    // Cleaning beyond this point would be a waste of log bandwidth
    TRowVersion maxWriteVersion(CompleteEdge.Step, Max<ui64>());
    if (maxWriteVersion < ImmediateWriteEdge) {
        maxWriteVersion = ImmediateWriteEdge;
    }

    // Make sure we don't leave followers without any repeatable read version
    TRowVersion maxRepeatableRead = TRowVersion::Max();
    if (Self->HasFollowers()) {
        maxRepeatableRead = FollowerReadEdge;
        if (maxRepeatableRead && !FollowerReadEdgeRepeatable) {
            maxRepeatableRead = maxRepeatableRead.Prev();
        }
    }

    const auto policy = GetSnapshotRetentionPolicy();
    const TRowVersion cleanupCandidate = Min(proposed, leastPlanned, leastAcquired, maxWriteVersion, maxRepeatableRead);
    const TRowVersion to = policy.GetCleanupBoundary(cleanupCandidate);
    const bool waitingForRegistry = policy.IsWaitingForRegistry();
    const bool hasUserTables = !Self->GetUserTables().empty();
    Self->SetCounter(COUNTER_MVCC_SNAPSHOT_REGISTRY_WAITING,
        hasUserTables && waitingForRegistry && cleanupCandidate > TRowVersion::Min());
    const ui64 cleanupLagMs = hasUserTables && !waitingForRegistry && cleanupCandidate.Step > to.Step
        ? cleanupCandidate.Step - to.Step : 0;
    Self->SetCounter(COUNTER_MVCC_SNAPSHOT_REGISTRY_CLEANUP_LAG_MS, cleanupLagMs);

    // Revisit retained points even when the watermark cannot advance. Removed
    // ranges are persisted by the executor, so this also works after a restart.
    removed |= CleanupRowVersions(txc.DB, TRowVersion::Min(), to, policy);
    if (LowWatermark < to) {
        NIceDb::TNiceDb db(txc.DB);
        SetLowWatermark(db, to);
        removed = true;
    }
    LastAdvanceWatermark = NActors::TActivationContext::Monotonic();

    return removed;
}

void TSnapshotManager::PersistAddSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key, const TString& name, ui64 flags, TDuration timeout) {
    using Schema = TDataShard::Schema;

    auto res = Snapshots.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(key),
        std::forward_as_tuple(key, name, flags, timeout));
    Y_ENSURE(res.second, "Unexpected duplicate snapshot: " << key);
    Self->UpdateHnswSnapshotPin(key, true);

    db.Table<Schema::Snapshots>()
        .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
        .Update<Schema::Snapshots::Name, Schema::Snapshots::Flags, Schema::Snapshots::TimeoutMs>(
            name, flags, timeout.MilliSeconds());
}

void TSnapshotManager::PersistRemoveSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key) {
    using Schema = TDataShard::Schema;

    auto it = Snapshots.find(key);
    Y_ENSURE(it != Snapshots.end(), "Removing unknown snapshot: " << key);

    if (ExpireQueue.Has(&it->second)) {
        ExpireQueue.Remove(&it->second);
    }

    Snapshots.erase(it);
    Self->UpdateHnswSnapshotPin(key, false);

    db.Table<Schema::Snapshots>()
        .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
        .Delete();
}

void TSnapshotManager::PersistUpdateSnapshotFlags(NIceDb::TNiceDb& db, const TSnapshotKey& key) {
    using Schema = TDataShard::Schema;

    auto it = Snapshots.find(key);
    Y_ENSURE(it != Snapshots.end(), "Updating unknown snapshot: " << key);

    if (it->second.HasFlags(TSnapshot::FlagRemoved)) {
        if (ExpireQueue.Has(&it->second)) {
            ExpireQueue.Remove(&it->second);
        }
    }

    db.Table<Schema::Snapshots>()
        .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
        .Update<Schema::Snapshots::Flags>(it->second.Flags);
}

void TSnapshotManager::PersistRemoveAllSnapshots(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    for (auto& kv : Snapshots) {
        const auto& key = kv.first;

        if (ExpireQueue.Has(&kv.second)) {
            ExpireQueue.Remove(&kv.second);
        }

        db.Table<Schema::Snapshots>()
            .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
            .Delete();
    }

    Snapshots.clear();
}

void TSnapshotManager::CleanupUnretainedRowVersions(NTable::TDatabase& db,
    const TRowVersion& from, const TRowVersion& to)
{
    const auto policy = GetSnapshotRetentionPolicy();
    CleanupRowVersions(db, from, to, policy);
}

void TSnapshotManager::RenameSnapshots(NTable::TDatabase& db, const TPathId& prevTableId, const TPathId& newTableId) {
    TSnapshotTableKey prevTableKey(prevTableId.OwnerId, prevTableId.LocalPathId);
    TSnapshotTableKey newTableKey(newTableId.OwnerId, newTableId.LocalPathId);

    NIceDb::TNiceDb nicedb(db);

    auto it = Snapshots.lower_bound(prevTableKey);
    while (it != Snapshots.end() && it->first == prevTableKey) {
        TSnapshotKey oldKey = it->first;
        TSnapshotKey newKey(newTableKey.OwnerId, newTableKey.PathId, oldKey.Step, oldKey.TxId);

        Y_DEBUG_ABORT_UNLESS(!References.contains(oldKey), "Unexpected reference to snapshot during rename");

        PersistAddSnapshot(nicedb, newKey, it->second.Name, it->second.Flags, it->second.Timeout);

        if (ExpireQueue.Has(&it->second)) {
            auto& newSnapshot = Snapshots.at(newKey);
            newSnapshot.ExpireTime = it->second.ExpireTime;
            ExpireQueue.Add(&newSnapshot);
        }

        ++it;
        PersistRemoveSnapshot(nicedb, oldKey);
    }
}

}   // namespace NDataShard
}   // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TSnapshotTableKey, stream, value) {
    stream << "{ table " << value.OwnerId << ":" << value.PathId << " }";
}

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TSnapshotKey, stream, value) {
    stream << "{ table " << value.OwnerId << ":" << value.PathId << " version " << value.Step << "/" << value.TxId << " }";
}
