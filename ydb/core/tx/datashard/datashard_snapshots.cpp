#include "datashard_snapshots.h"

#include "datashard_impl.h"

#include <ydb/core/protos/datashard_config.pb.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NDataShard {

void TSnapshotManager::Reset() {
    MinWriteVersion = TRowVersion::Min();

    MvccState = EMvccState::MvccUnspecified;
    KeepSnapshotTimeout = 0;
    IncompleteEdge = TRowVersion::Min();
    CompleteEdge = TRowVersion::Min();
    LowWatermark = TRowVersion::Min();
    ImmediateWriteEdge = TRowVersion::Min();
    ImmediateWriteEdgeReplied = TRowVersion::Min();
    UnprotectedReadEdge = TRowVersion::Min();

    CommittedCompleteEdge = TRowVersion::Min();
    FollowerReadEdge = TRowVersion::Min();
    FollowerReadEdgeRepeatable = false;

    Snapshots.clear();
}

bool TSnapshotManager::Reload(NIceDb::TNiceDb& db) {
    bool ready = true;

    ready &= ReloadSys(db);
    ready &= ReloadSnapshots(db);

    if (ready) {
        Self->SetCounter(COUNTER_MVCC_ENABLED, IsMvccEnabled() ? 1 : 0);
    }

    return ready;
}

bool TSnapshotManager::ReloadSys(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    bool ready = true;

    TRowVersion minWriteVersion = TRowVersion::Min();
    TRowVersion completeEdge = TRowVersion::Min();
    TRowVersion incompleteEdge = TRowVersion::Min();
    TRowVersion lowWatermark = TRowVersion::Min();
    TRowVersion immediateWriteEdge = TRowVersion::Min();
    TRowVersion followerReadEdge = TRowVersion::Min();

    ui32 mvccState = 0;
    ui64 keepSnapshotTimeout = 0;
    ui64 unprotectedReads = 0;
    ui64 followerReadEdgeRepeatable = 0;

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
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_ImmediateWriteEdgeStep, immediateWriteEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_ImmediateWriteEdgeTxId, immediateWriteEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeStep, followerReadEdge.Step);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeTxId, followerReadEdge.TxId);
    ready &= Self->SysGetUi64(db, Schema::SysMvcc_FollowerReadEdgeRepeatable, followerReadEdgeRepeatable);

    if (ready) {
        // We have a consistent view of settings, apply
        MinWriteVersion = minWriteVersion;
        MvccState = static_cast<EMvccState>(mvccState);
        KeepSnapshotTimeout = keepSnapshotTimeout;
        PerformedUnprotectedReads = (unprotectedReads != 0);
        CompleteEdge = completeEdge;
        IncompleteEdge = incompleteEdge;
        LowWatermark = lowWatermark;
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
        Y_VERIFY_S(res.second, "Unexpected duplicate snapshot: " << key);

        if (!rowset.Next()) {
            return false;
        }
    }

    Snapshots = std::move(snapshots);
    return true;
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
    if (!IsMvccEnabled())
        return false;

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
    if (!IsMvccEnabled())
        return false;

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
    if (!IsMvccEnabled())
        return false;

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

void TSnapshotManager::SetImmediateWriteEdge(const TRowVersion& version, TTransactionContext& txc) {
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    Self->PersistSys(db, Schema::SysMvcc_ImmediateWriteEdgeStep, version.Step);
    Self->PersistSys(db, Schema::SysMvcc_ImmediateWriteEdgeTxId, version.TxId);
    ImmediateWriteEdge = version;
}

bool TSnapshotManager::PromoteImmediateWriteEdge(const TRowVersion& version, TTransactionContext& txc) {
    if (!IsMvccEnabled())
        return false;

    if (version > ImmediateWriteEdge) {
        SetImmediateWriteEdge(version, txc);

        return true;
    }

    return false;
}

bool TSnapshotManager::PromoteImmediateWriteEdgeReplied(const TRowVersion& version) {
    if (!IsMvccEnabled())
        return false;

    if (version > ImmediateWriteEdgeReplied) {
        ImmediateWriteEdgeReplied = version;
        return true;
    }

    return false;
}

TRowVersion TSnapshotManager::GetUnprotectedReadEdge() const {
    return UnprotectedReadEdge;
}

bool TSnapshotManager::PromoteUnprotectedReadEdge(const TRowVersion& version) {
    if (IsMvccEnabled() && UnprotectedReadEdge < version) {
        UnprotectedReadEdge = version;
        return true;
    }

    return false;
}

bool TSnapshotManager::GetPerformedUnprotectedReads() const {
    return PerformedUnprotectedReads;
}

bool TSnapshotManager::IsPerformedUnprotectedReadsCommitted() const {
    return PerformedUnprotectedReadsUncommitted == 0;
}

void TSnapshotManager::SetPerformedUnprotectedReads(bool performedUnprotectedReads, TTransactionContext& txc) {
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    Self->PersistSys(db, Schema::SysMvcc_UnprotectedReads, ui64(performedUnprotectedReads ? 1 : 0));
    PerformedUnprotectedReads = performedUnprotectedReads;
    PerformedUnprotectedReadsUncommitted++;

    txc.DB.OnPersistent([this] {
        this->PerformedUnprotectedReadsUncommitted--;
    });
}

std::pair<TRowVersion, bool> TSnapshotManager::GetFollowerReadEdge() const {
    return { FollowerReadEdge, FollowerReadEdgeRepeatable };
}

bool TSnapshotManager::PromoteFollowerReadEdge(const TRowVersion& version, bool repeatable, TTransactionContext& txc) {
    using Schema = TDataShard::Schema;

    if (IsMvccEnabled()) {
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

    RemoveRowVersions(db, LowWatermark, to);

    NIceDb::TNiceDb nicedb(db);
    SetLowWatermark(nicedb, to);

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

ui64 TSnapshotManager::GetKeepSnapshotTimeout() const {
    return KeepSnapshotTimeout ? KeepSnapshotTimeout : AppData()->DataShardConfig.GetKeepSnapshotTimeout();
}

TDuration TSnapshotManager::GetCleanupSnapshotPeriod() const {
    return TDuration::MilliSeconds(AppData()->DataShardConfig.GetCleanupSnapshotPeriod());
}

bool TSnapshotManager::ChangeMvccState(ui64 step, ui64 txId, TTransactionContext& txc, EMvccState state) {
    Y_ABORT_UNLESS(state != EMvccState::MvccUnspecified);

    if (MvccState == state)
        return false;

    using Schema = TDataShard::Schema;
    const TRowVersion opVersion(step, txId);

    // We need to choose a version that is at least as large as all previous edges
    TRowVersion nextVersion = Max(opVersion, MinWriteVersion, CompleteEdge, IncompleteEdge, ImmediateWriteEdge);

    // This must be a version that we may have previously written to, and which
    // must not be a snapshot. We don't know if there have been any immediate
    // mvcc writes before the switch, but they would have happened at the end
    // of the step.
    if (nextVersion) {
        nextVersion.TxId = Max<ui64>();
    }

    if (IsMvccEnabled()) {
        RemoveRowVersions(txc.DB, LowWatermark, nextVersion);
    } else {
        RemoveRowVersions(txc.DB, MinWriteVersion, nextVersion);
    }

    switch (state) {
        case EMvccState::MvccEnabled: {
            NIceDb::TNiceDb nicedb(txc.DB);

            Self->PersistSys(nicedb, Schema::SysMvcc_State, (ui32)state);
            MvccState = state;

            SetCompleteEdge(nicedb, nextVersion);
            SetIncompleteEdge(nicedb, nextVersion);
            SetImmediateWriteEdge(nextVersion, txc);
            SetLowWatermark(nicedb, nextVersion);
            ImmediateWriteEdgeReplied = ImmediateWriteEdge;

            break;
        }

        case EMvccState::MvccDisabled: {
            NIceDb::TNiceDb nicedb(txc.DB);

            SetMinWriteVersion(nicedb, nextVersion);

            Self->PersistSys(nicedb, Schema::SysMvcc_State, (ui32)state);
            MvccState = state;

            const auto minVersion = TRowVersion::Min();
            SetCompleteEdge(nicedb, minVersion);
            SetIncompleteEdge(nicedb, minVersion);
            SetImmediateWriteEdge(minVersion, txc);
            SetLowWatermark(nicedb, minVersion);
            ImmediateWriteEdgeReplied = ImmediateWriteEdge;

            break;
        }

        default:
            Y_ABORT("Unexpected mvcc state# %d", (ui32)state);
    }

    txc.DB.OnPersistent([this, edge = CompleteEdge] {
        this->CommittedCompleteEdge = edge;
    });

    Self->SetCounter(COUNTER_MVCC_ENABLED, IsMvccEnabled() ? 1 : 0);

    return true;
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

    auto* tablePtr = Self->GetUserTables().FindPtr(key.PathId);
    Y_VERIFY_S(tablePtr && *tablePtr, "DataShard " << Self->TabletID() << " missing table " << key.PathId);
    const ui32 localTableId = (**tablePtr).LocalTid;

    const TRowVersion oldVersion = MinWriteVersion;
    const TRowVersion newVersion(key.Step, key.TxId);

    // N.B. no colocated tables support for now
    Y_ABORT_UNLESS(Self->GetUserTables().size() == 1, "Multiple co-located tables not supported");

    Y_VERIFY_S(oldVersion <= newVersion,
        "DataShard " << Self->TabletID()
        << " adding new snapshot " << newVersion
        << " below previous min write version " << oldVersion);

    NIceDb::TNiceDb nicedb(db);

    PersistAddSnapshot(nicedb, key, name, flags, timeout);

    if (!IsMvccEnabled() && oldVersion < newVersion) {
        // Everything from oldVersion to newVersion (not inclusive) is not readable
        db.RemoveRowVersions(localTableId, oldVersion, newVersion);
    }

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
    auto* tablePtr = Self->GetUserTables().FindPtr(key.PathId);
    Y_VERIFY_S(tablePtr && *tablePtr, "DataShard " << Self->TabletID() << " missing table " << key.PathId);
    const ui32 localTableId = (*tablePtr)->LocalTid;

    NIceDb::TNiceDb nicedb(db);

    // Remove snapshot from local db and memory
    PersistRemoveSnapshot(nicedb, key);

    // Mark snapshot as no longer accessible in local database
    const TRowVersion rowVersion(key.Step, key.TxId);
    if (!IsMvccEnabled() || rowVersion < LowWatermark)
        db.RemoveRowVersions(localTableId, rowVersion, rowVersion.Next());
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

    if (IsMvccEnabled()) {
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
    }

    return Min(snapshotTimeout, mvccGcTimeout);
}

bool TSnapshotManager::HasExpiringSnapshots() const {
    return IsMvccEnabled() || bool(ExpireQueue);
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

    if (!IsMvccEnabled())
        return removed;

    ui64 keepSnapshotTimeout = GetKeepSnapshotTimeout();
    TRowVersion proposed = TRowVersion(Max(now.MilliSeconds(), keepSnapshotTimeout) - keepSnapshotTimeout, 0);

    TRowVersion leastPlanned = TRowVersion::Max();
    if (auto it = Self->Pipeline.GetPlan().begin(); it != Self->Pipeline.GetPlan().end())
        leastPlanned = TRowVersion(it->Step, it->TxId);

    // holds current snapshot operations
    TRowVersion leastAcquired = TRowVersion::Max();
    for (auto &it : Self->Pipeline.GetImmediateOps()) {
        if (it.second->IsMvccSnapshotRead())
            leastAcquired = Min(leastAcquired, it.second->GetMvccSnapshot());
    }

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

    removed |= AdvanceWatermark(txc.DB, Min(proposed, leastPlanned, leastAcquired, maxWriteVersion, maxRepeatableRead));
    LastAdvanceWatermark = NActors::TActivationContext::Monotonic();

    return removed;
}

void TSnapshotManager::PersistAddSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key, const TString& name, ui64 flags, TDuration timeout) {
    using Schema = TDataShard::Schema;

    auto res = Snapshots.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(key),
        std::forward_as_tuple(key, name, flags, timeout));
    Y_VERIFY_S(res.second, "Unexpected duplicate snapshot: " << key);

    db.Table<Schema::Snapshots>()
        .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
        .Update<Schema::Snapshots::Name, Schema::Snapshots::Flags, Schema::Snapshots::TimeoutMs>(
            name, flags, timeout.MilliSeconds());
}

void TSnapshotManager::PersistRemoveSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key) {
    using Schema = TDataShard::Schema;

    auto it = Snapshots.find(key);
    Y_VERIFY_S(it != Snapshots.end(), "Removing unknown snapshot: " << key);

    if (ExpireQueue.Has(&it->second)) {
        ExpireQueue.Remove(&it->second);
    }

    Snapshots.erase(it);

    db.Table<Schema::Snapshots>()
        .Key(key.OwnerId, key.PathId, key.Step, key.TxId)
        .Delete();
}

void TSnapshotManager::PersistUpdateSnapshotFlags(NIceDb::TNiceDb& db, const TSnapshotKey& key) {
    using Schema = TDataShard::Schema;

    auto it = Snapshots.find(key);
    Y_VERIFY_S(it != Snapshots.end(), "Updating unknown snapshot: " << key);

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

void TSnapshotManager::Fix_KIKIMR_12289(NTable::TDatabase& db) {
    if (IsMvccEnabled()) {
        return;
    }

    EnsureRemovedRowVersions(db, TRowVersion::Min(), MinWriteVersion);
}

void TSnapshotManager::Fix_KIKIMR_14259(NTable::TDatabase& db) {
    if (!IsMvccEnabled()) {
        return;
    }

    EnsureRemovedRowVersions(db, TRowVersion::Min(), LowWatermark);
}

void TSnapshotManager::EnsureRemovedRowVersions(NTable::TDatabase& db, const TRowVersion& fromArg, const TRowVersion& toArg) {
    for (auto& it : Self->GetUserTables()) {
        auto tid = it.second->LocalTid;
        auto ranges = db.GetRemovedRowVersions(tid);

        auto from = fromArg;
        auto to = toArg;

        auto begin = Snapshots.lower_bound(TSnapshotKey(Self->GetPathOwnerId(), it.first, from.Step, from.TxId));
        auto end = Snapshots.upper_bound(TSnapshotKey(Self->GetPathOwnerId(), it.first, to.Step, to.TxId));
        for (auto it = begin; it != end && from < to; ++it) {
            auto mid = TRowVersion(it->first.Step, it->first.TxId);
            if (from < mid && !ranges.Contains(from, mid)) {
                db.RemoveRowVersions(tid, from, mid);
            }
            from = mid.Next();
        }
        if (from < to && !ranges.Contains(from, to)) {
            db.RemoveRowVersions(tid, from, to);
        }
    }
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
