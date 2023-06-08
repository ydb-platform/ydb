#pragma once

#include "const.h"
#include "snapshot_key.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NDataShard {

using NTabletFlatExecutor::TTransactionContext;

class TOperation;
class TDataShard;

class TSnapshot {
    friend class TSnapshotExpireQueue;
    friend class TSnapshotManager;

public:
    // Flag values
    enum : ui64 {
        // Snapshot is managed by user, e.g. may be deleted by a user tx
        FlagUser = (1ULL << 0),
        // Snapshot has a timeout and must be pinged to stay alive
        FlagTimeout = (1ULL << 1),
        // Snapshot created using a scheme tx (only another scheme tx may delete it)
        FlagScheme = (1ULL << 2),
        // Flag for removed (but not yet deleted) snapshots
        FlagRemoved = (1ULL << 63),
    };

public:
    TSnapshot(const TSnapshotKey& key, TString name, ui64 flags, TDuration timeout)
        : Key(key)
        , Name(std::move(name))
        , Flags(flags)
        , Timeout(timeout)
    { }

    bool HasFlags(ui64 flags) const {
        return (Flags & flags) == flags;
    }

    bool InExpireQueue() const {
        return ExpireHeapIndex != size_t(-1);
    }

public:
    TSnapshotKey Key;
    TString Name;
    ui64 Flags;
    TDuration Timeout;

    TInstant ExpireTime = TInstant::Max();
    size_t ExpireHeapIndex = -1;

    struct THeapIndexByExpireTime {
        size_t& operator()(TSnapshot& snapshot) const {
            return snapshot.ExpireHeapIndex;
        }
    };

    struct TCompareByExpireTime {
        bool operator()(const TSnapshot& a, const TSnapshot& b) const {
            return a.ExpireTime < b.ExpireTime;
        }
    };
};

enum class EMvccState {
    MvccUnspecified = 0,
    MvccEnabled = 1,
    MvccDisabled = 2
};

class TSnapshotManager {
public:
    using TSnapshotMap = TMap<TSnapshotKey, TSnapshot, TLess<void>>;

    class TSnapshotsView {
    public:
        using const_iterator = TSnapshotMap::const_iterator;

        TSnapshotsView(const_iterator begin, const_iterator end)
            : Begin(begin)
            , End(end)
        { }

        const_iterator begin() const { return Begin; }
        const_iterator end() const { return End; }

    private:
        const const_iterator Begin;
        const const_iterator End;
    };

    explicit TSnapshotManager(TDataShard* self)
        : Self(self)
    { }

    void Reset();

    bool Reload(NIceDb::TNiceDb& db);
    bool ReloadSys(NIceDb::TNiceDb& db);
    bool ReloadSnapshots(NIceDb::TNiceDb& db);

    void InitExpireQueue(TInstant now);

    TRowVersion GetMinWriteVersion() const;

    void SetMinWriteVersion(NIceDb::TNiceDb& db, TRowVersion writeVersion);

    TRowVersion GetCompleteEdge() const;
    TRowVersion GetCommittedCompleteEdge() const;

    void SetCompleteEdge(NIceDb::TNiceDb& db, const TRowVersion& version);

    bool PromoteCompleteEdge(const TRowVersion& version, TTransactionContext& txc);
    bool PromoteCompleteEdge(TOperation* op, TTransactionContext& txc);
    bool PromoteCompleteEdge(ui64 writeStep, TTransactionContext& txc);

    TRowVersion GetIncompleteEdge() const;

    void SetIncompleteEdge(NIceDb::TNiceDb& db, const TRowVersion& version);

    bool PromoteIncompleteEdge(TOperation* op, TTransactionContext& txc);

    TRowVersion GetImmediateWriteEdge() const;
    TRowVersion GetImmediateWriteEdgeReplied() const;
    void SetImmediateWriteEdge(const TRowVersion& version, TTransactionContext& txc);
    bool PromoteImmediateWriteEdge(const TRowVersion& version, TTransactionContext& txc);
    bool PromoteImmediateWriteEdgeReplied(const TRowVersion& version);

    TRowVersion GetUnprotectedReadEdge() const;
    bool PromoteUnprotectedReadEdge(const TRowVersion& version);

    bool GetPerformedUnprotectedReads() const;
    bool IsPerformedUnprotectedReadsCommitted() const;
    void SetPerformedUnprotectedReads(bool performedUnprotectedReads, TTransactionContext& txc);

    std::pair<TRowVersion, bool> GetFollowerReadEdge() const;
    bool PromoteFollowerReadEdge(const TRowVersion& version, bool repeatable, TTransactionContext& txc);

    EMvccState GetMvccState() const {
        return MvccState;
    }

    bool IsMvccEnabled() const {
        // Note: mvcc is disabled during MvccUnspecified
        return MvccState == EMvccState::MvccEnabled;
    }

    bool ChangeMvccState(ui64 step, ui64 txId, TTransactionContext& txc, EMvccState state);

    ui64 GetKeepSnapshotTimeout() const;
    TDuration GetCleanupSnapshotPeriod() const;

    void SetKeepSnapshotTimeout(NIceDb::TNiceDb& db, ui64 keepSnapshotTimeout);

    TRowVersion GetLowWatermark() const;

    void SetLowWatermark(NIceDb::TNiceDb &db, TRowVersion watermark);

    bool AdvanceWatermark(NTable::TDatabase& db, const TRowVersion& to);

    void RemoveRowVersions(NTable::TDatabase &db, const TRowVersion &from, const TRowVersion &to);

    const TSnapshotMap& GetSnapshots() const {
        return Snapshots;
    }

    /**
     * Returns a view of snapshots for a single table
     */
    TSnapshotsView GetSnapshots(const TSnapshotTableKey& key) const {
        auto res = Snapshots.equal_range(key);
        return TSnapshotsView(res.first, res.second);
    }

    const TSnapshot* FindAvailable(const TSnapshotKey& key) const;

    TSnapshotKey ExpandSnapshotKey(ui64 ownerId, ui64 pathId, ui64 step, ui64 txId) const;

    bool AcquireReference(const TSnapshotKey& key);
    bool ReleaseReference(const TSnapshotKey& key, NTable::TDatabase& db, TInstant now);

    bool AddSnapshot(NTable::TDatabase& db, const TSnapshotKey& key, const TString& name, ui64 flags, TDuration timeout);
    bool RemoveSnapshot(NTable::TDatabase& db, const TSnapshotKey& key);
    bool CleanupRemovedSnapshots(NTable::TDatabase& db);

    void InitSnapshotExpireTime(const TSnapshotKey& key, TInstant now);
    bool RefreshSnapshotExpireTime(const TSnapshotKey& key, TInstant now);

    TDuration CleanupTimeout() const;
    bool HasExpiringSnapshots() const;
    bool RemoveExpiredSnapshots(TInstant now, TTransactionContext& txc);

    void PersistAddSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key, const TString& name, ui64 flags, TDuration timeout);
    void PersistRemoveSnapshot(NIceDb::TNiceDb& db, const TSnapshotKey& key);
    void PersistUpdateSnapshotFlags(NIceDb::TNiceDb& db, const TSnapshotKey& key);
    void PersistRemoveAllSnapshots(NIceDb::TNiceDb& db);

    void Fix_KIKIMR_12289(NTable::TDatabase& db);
    void Fix_KIKIMR_14259(NTable::TDatabase& db);
    void EnsureRemovedRowVersions(NTable::TDatabase& db, const TRowVersion& from, const TRowVersion& to);

    void RenameSnapshots(NTable::TDatabase& db, const TPathId& prevTableId, const TPathId& newTableId);

private:
    void DoRemoveSnapshot(NTable::TDatabase& db, const TSnapshotKey& key);

private:
    TDataShard* const Self;
    TRowVersion MinWriteVersion;

    EMvccState MvccState = EMvccState::MvccUnspecified;
    ui64 KeepSnapshotTimeout = 0;
    bool PerformedUnprotectedReads = false;
    ui64 PerformedUnprotectedReadsUncommitted = 0;
    TRowVersion IncompleteEdge = TRowVersion::Min();
    TRowVersion CompleteEdge = TRowVersion::Min();
    TRowVersion LowWatermark = TRowVersion::Min();
    TRowVersion ImmediateWriteEdge = TRowVersion::Min();
    TRowVersion ImmediateWriteEdgeReplied = TRowVersion::Min();
    TRowVersion UnprotectedReadEdge = TRowVersion::Min();

    TRowVersion CommittedCompleteEdge = TRowVersion::Min();
    TRowVersion FollowerReadEdge = TRowVersion::Min();
    bool FollowerReadEdgeRepeatable = false;

    TSnapshotMap Snapshots;

    using TSnapshotExpireQueue = TIntrusiveHeap<TSnapshot, TSnapshot::THeapIndexByExpireTime, TSnapshot::TCompareByExpireTime>;
    TSnapshotExpireQueue ExpireQueue;

    THashMap<TSnapshotKey, size_t> References;

    TMonotonic LastAdvanceWatermark{};
};

}   // namespace NDataShard
}   // namespace NKikimr
