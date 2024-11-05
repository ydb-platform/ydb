#pragma once

#include "sys_tables.h"
#include "range_treap.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <util/generic/list.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>

#include <util/system/valgrind.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NDataShard {

class ILocksDb {
protected:
    ~ILocksDb() = default;

public:
    struct TLockRange {
        ui64 RangeId;
        TPathId TableId;
        ui64 Flags;
        TString Data;
    };

    struct TLockRow {
        ui64 LockId;
        ui32 LockNodeId;
        ui32 Generation;
        ui64 Counter;
        ui64 CreateTs;
        ui64 Flags;

        TVector<TLockRange> Ranges;
        TVector<ui64> Conflicts;
        TVector<ui64> VolatileDependencies;
    };

    virtual bool Load(TVector<TLockRow>& rows) = 0;

    // Returns true when a new lock may be added with the given lockId
    // Sometimes new lock cannot be added, e.g. when it had uncommitted changes
    // in the past, and adding anything with the same lockId would conflict
    // with previous decisions.
    virtual bool MayAddLock(ui64 lockId) = 0;

    // Persist adding/removing a lock info
    virtual void PersistAddLock(ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, ui64 createTs, ui64 flags = 0) = 0;
    virtual void PersistLockCounter(ui64 lockId, ui64 counter) = 0;
    virtual void PersistLockFlags(ui64 lockId, ui64 flags) = 0;
    virtual void PersistRemoveLock(ui64 lockId) = 0;

    // Persist adding/removing info on locked ranges
    virtual void PersistAddRange(ui64 lockId, ui64 rangeId, const TPathId& tableId, ui64 flags = 0, const TString& data = {}) = 0;
    virtual void PersistRangeFlags(ui64 lockId, ui64 rangeId, ui64 flags) = 0;
    virtual void PersistRemoveRange(ui64 lockId, ui64 rangeId) = 0;

    // Persist a conflict, i.e. this lock must break some other lock on commit
    virtual void PersistAddConflict(ui64 lockId, ui64 otherLockId) = 0;
    virtual void PersistRemoveConflict(ui64 lockId, ui64 otherLockId) = 0;

    // Persist volatile dependencies, i.e. which undecided transactions must be waited for on commit
    virtual void PersistAddVolatileDependency(ui64 lockId, ui64 txId) = 0;
    virtual void PersistRemoveVolatileDependency(ui64 lockId, ui64 txId) = 0;
};

class TLocksDataShard {
public:
    virtual ~TLocksDataShard() = default;

    virtual void IncCounter(ECumulativeCounters counter,
                            ui64 num = 1) const = 0;
    virtual void IncCounter(EPercentileCounters counter,
                            ui64 num) const = 0;
    virtual void IncCounter(EPercentileCounters counter,
                            const TDuration& latency) const = 0;

    virtual ui64 TabletID() const = 0;
    virtual bool IsUserTable(const TTableId& tableId) const = 0;
    virtual ui32 Generation() const = 0;
    virtual TRowVersion LastCompleteTxVersion() const = 0;
};

template <typename T>
class TLocksDataShardAdapter : public TLocksDataShard
{
public:
    TLocksDataShardAdapter(const T *self)
        : Self(self)
    {
    }

    void IncCounter(ECumulativeCounters counter,
                    ui64 num = 1) const override
    {
        return Self->IncCounter(counter, num);
    }

    void IncCounter(EPercentileCounters counter,
                    ui64 num) const override
    {
        return Self->IncCounter(counter, num);
    }

    void IncCounter(EPercentileCounters counter,
                    const TDuration& latency) const override
    {
        return Self->IncCounter(counter, latency);
    }

    ui64 TabletID() const override
    {
        return Self->TabletID();
    }

    bool IsUserTable(const TTableId& tableId) const override
    {
        return Self->IsUserTable(tableId);
    }

    ui32 Generation() const override
    {
        return Self->Generation();
    }

    TRowVersion LastCompleteTxVersion() const override
    {
        return Self->LastCompleteTxVersion();
    }

private:
    const T *Self;
};

class TLockInfo;
class TTableLocks;
class TLockLocker;
class TSysLocks;

///
struct TPointKey {
    TIntrusivePtr<TTableLocks> Table;
    TOwnedCellVec Key;

    TOwnedTableRange ToOwnedTableRange() const {
        return TOwnedTableRange(Key);
    }
};

///
struct TRangeKey {
    TIntrusivePtr<TTableLocks> Table;
    TOwnedCellVec From;
    TOwnedCellVec To;
    bool InclusiveFrom;
    bool InclusiveTo;

    TOwnedTableRange ToOwnedTableRange() const {
        return TOwnedTableRange(From, InclusiveFrom, To, InclusiveTo);
    }
};

struct TVersionedLockId {
    TVersionedLockId(ui64 lockId, TRowVersion version)
        : LockId(lockId)
        , Version(version) {}

    ui64 LockId;
    TRowVersion Version;

    bool operator<(const TVersionedLockId& other) const {
        return Version < other.Version;
    }
};

struct TPendingSubscribeLock {
    ui64 LockId = 0;
    ui32 LockNodeId = 0;

    TPendingSubscribeLock() = default;

    TPendingSubscribeLock(ui64 lockId, ui32 lockNodeId)
        : LockId(lockId)
        , LockNodeId(lockNodeId)
    { }

    explicit operator bool() const {
        return LockId != 0;
    }
};

// ELockFlags type safe enum

enum class ELockFlags : ui64 {
    None = 0,
    Frozen = 1,
};

using ELockFlagsRaw = std::underlying_type<ELockFlags>::type;

inline ELockFlags operator|(ELockFlags a, ELockFlags b) { return ELockFlags(ELockFlagsRaw(a) | ELockFlagsRaw(b)); }
inline ELockFlags operator&(ELockFlags a, ELockFlags b) { return ELockFlags(ELockFlagsRaw(a) & ELockFlagsRaw(b)); }
inline ELockFlags& operator|=(ELockFlags& a, ELockFlags b) { return a = a | b; }
inline ELockFlags& operator&=(ELockFlags& a, ELockFlags b) { return a = a & b; }
inline bool operator!(ELockFlags c) { return ELockFlagsRaw(c) == 0; }

// ELockConflictFlags type safe enum

enum class ELockConflictFlags : ui8 {
    None = 0,
    BreakThemOnOurCommit = 1,
    BreakUsOnTheirCommit = 2,
};

using ELockConflictFlagsRaw = std::underlying_type<ELockConflictFlags>::type;

inline ELockConflictFlags operator|(ELockConflictFlags a, ELockConflictFlags b) { return ELockConflictFlags(ELockConflictFlagsRaw(a) | ELockConflictFlagsRaw(b)); }
inline ELockConflictFlags operator&(ELockConflictFlags a, ELockConflictFlags b) { return ELockConflictFlags(ELockConflictFlagsRaw(a) & ELockConflictFlagsRaw(b)); }
inline ELockConflictFlags& operator|=(ELockConflictFlags& a, ELockConflictFlags b) { return a = a | b; }
inline ELockConflictFlags& operator&=(ELockConflictFlags& a, ELockConflictFlags b) { return a = a & b; }
inline bool operator!(ELockConflictFlags c) { return ELockConflictFlagsRaw(c) == 0; }

// ELockRangeFlags type safe enum

enum class ELockRangeFlags : ui8 {
    None = 0,
    Read = 1,
    Write = 2,
};

using ELockRangeFlagsRaw = std::underlying_type<ELockRangeFlags>::type;

inline ELockRangeFlags operator|(ELockRangeFlags a, ELockRangeFlags b) { return ELockRangeFlags(ELockRangeFlagsRaw(a) | ELockRangeFlagsRaw(b)); }
inline ELockRangeFlags operator&(ELockRangeFlags a, ELockRangeFlags b) { return ELockRangeFlags(ELockRangeFlagsRaw(a) | ELockRangeFlagsRaw(b)); }
inline ELockRangeFlags& operator|=(ELockRangeFlags& a, ELockRangeFlags b) { return a = a | b; }
inline ELockRangeFlags& operator&=(ELockRangeFlags& a, ELockRangeFlags b) { return a = a & b; }
inline bool operator!(ELockRangeFlags c) { return ELockRangeFlagsRaw(c) == 0; }

// Tags for various intrusive lists
struct TLockInfoBreakListTag {};
struct TLockInfoEraseListTag {};
struct TLockInfoReadConflictListTag {};
struct TLockInfoWriteConflictListTag {};
struct TLockInfoBrokenListTag {};
struct TLockInfoBrokenPersistentListTag {};
struct TLockInfoExpireListTag {};

/// Aggregates shard, point and range locks
class TLockInfo
    : public TSimpleRefCount<TLockInfo>
    , public TIntrusiveListItem<TLockInfo, TLockInfoBreakListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoEraseListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoReadConflictListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoWriteConflictListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoBrokenListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoBrokenPersistentListTag>
    , public TIntrusiveListItem<TLockInfo, TLockInfoExpireListTag>
{
    friend class TTableLocks;
    friend class TLockLocker;
    friend class TSysLocks;

public:
    using TPtr = TIntrusivePtr<TLockInfo>;

    TLockInfo(TLockLocker * locker, ui64 lockId, ui32 lockNodeId);
    TLockInfo(TLockLocker * locker, const ILocksDb::TLockRow& row);
    ~TLockInfo();

    bool Empty() const {
        return !(
            IsPersistent() ||
            !ReadTables.empty() ||
            !WriteTables.empty() ||
            IsBroken());
    }

    template<class TTag>
    bool IsInList() const {
        using TItem = TIntrusiveListItem<TLockInfo, TTag>;
        return !static_cast<const TItem*>(this)->Empty();
    }

    template<class TTag>
    void UnlinkFromList() {
        using TItem = TIntrusiveListItem<TLockInfo, TTag>;
        static_cast<TItem*>(this)->Unlink();
    }

    ui32 GetGeneration() const { return Generation; }
    ui64 GetCounter(const TRowVersion& at = TRowVersion::Max()) const { return !BreakVersion || at < *BreakVersion ? Counter : Max<ui64>(); }
    bool IsBroken(const TRowVersion& at = TRowVersion::Max()) const { return GetCounter(at) == Max<ui64>(); }

    size_t NumPoints() const { return Points.size(); }
    size_t NumRanges() const { return Ranges.size(); }
    bool IsShardLock() const { return ShardLock; }
    bool IsWriteLock() const { return !WriteTables.empty(); }
    bool IsPersistent() const { return Persistent; }
    bool HasUnpersistedRanges() const { return UnpersistedRanges; }
    //ui64 MemorySize() const { return 1; } // TODO

    bool MayHavePointsAndRanges() const { return !ShardLock && (!BreakVersion || *BreakVersion); }

    ui64 GetLockId() const { return LockId; }
    ui32 GetLockNodeId() const { return LockNodeId; }

    TInstant GetCreationTime() const { return CreationTime; }

    ELockFlags GetFlags() const { return Flags; }

    const THashSet<TPathId>& GetReadTables() const { return ReadTables; }
    const THashSet<TPathId>& GetWriteTables() const { return WriteTables; }

    const TVector<TPointKey>& GetPoints() const { return Points; }
    const TVector<TRangeKey>& GetRanges() const { return Ranges; }

    void PersistLock(ILocksDb* db);
    void PersistBrokenLock(ILocksDb* db);
    void PersistRemoveLock(ILocksDb* db);

    void PersistRanges(ILocksDb* db);

    void AddConflict(TLockInfo* otherLock, ILocksDb* db);
    void AddVolatileDependency(ui64 txId, ILocksDb* db);
    void PersistConflicts(ILocksDb* db);
    void CleanupConflicts();

    void RestorePersistentRange(const ILocksDb::TLockRange& rangeRow);
    void RestorePersistentConflict(TLockInfo* otherLock);
    void RestorePersistentVolatileDependency(ui64 txId);

    template<class TCallback>
    void ForAllConflicts(TCallback&& callback) {
        for (auto& pr : ConflictLocks) {
            callback(pr.first);
        }
    }

    template<class TCallback>
    void ForAllVolatileDependencies(TCallback&& callback) {
        for (auto& item : VolatileDependencies) {
            callback(item);
        }
    }

    ui64 GetLastOpId() const { return LastOpId; }
    void SetLastOpId(ui64 opId) { LastOpId = opId; }

    bool IsFrozen() const { return !!(Flags & ELockFlags::Frozen); }
    void SetFrozen(ILocksDb* db = nullptr);

private:
    void MakeShardLock();
    bool AddShardLock(const TPathId& pathId);
    bool AddPoint(const TPointKey& point);
    bool AddRange(const TRangeKey& range);
    bool AddWriteLock(const TPathId& pathId);
    void SetBroken(TRowVersion at);
    void OnRemoved();

    void PersistAddRange(const TPathId& tableId, ELockRangeFlags flags, ILocksDb* db);

private:
    struct TPersistentRange {
        ui64 Id;
        TPathId TableId;
        ELockRangeFlags Flags;
    };

private:
    TLockLocker * Locker;
    ui64 LockId;
    ui32 LockNodeId;
    ui32 Generation;
    ui64 Counter;
    TInstant CreationTime;
    ELockFlags Flags = ELockFlags::None;
    THashSet<TPathId> ReadTables;
    THashSet<TPathId> WriteTables;
    TVector<TPointKey> Points;
    TVector<TRangeKey> Ranges;
    bool ShardLock = false;
    bool Persistent = false;
    bool UnpersistedRanges = false;
    bool InBrokenLocks = false;

    std::optional<TRowVersion> BreakVersion;

    // A set of locks we must break on commit
    THashMap<TLockInfo*, ELockConflictFlags> ConflictLocks;
    absl::flat_hash_set<ui64> VolatileDependencies;
    TVector<TPersistentRange> PersistentRanges;

    ui64 LastOpId = 0;
};

struct TTableLocksReadListTag {};
struct TTableLocksWriteListTag {};
struct TTableLocksAffectedListTag {};
struct TTableLocksBreakShardListTag {};
struct TTableLocksBreakRangeListTag {};
struct TTableLocksWriteConflictShardListTag {};

///
class TTableLocks
    : public TSimpleRefCount<TTableLocks>
    , public TIntrusiveListItem<TTableLocks, TTableLocksReadListTag>
    , public TIntrusiveListItem<TTableLocks, TTableLocksWriteListTag>
    , public TIntrusiveListItem<TTableLocks, TTableLocksAffectedListTag>
    , public TIntrusiveListItem<TTableLocks, TTableLocksBreakShardListTag>
    , public TIntrusiveListItem<TTableLocks, TTableLocksBreakRangeListTag>
    , public TIntrusiveListItem<TTableLocks, TTableLocksWriteConflictShardListTag>
{
    friend class TSysLocks;

public:
    using TPtr = TIntrusivePtr<TTableLocks>;

    static constexpr ui32 SavedKeys = 64;

    TTableLocks(const TPathId& tableId)
        : TableId(tableId)
    {}

    template<class TTag>
    bool IsInList() const {
        using TItem = TIntrusiveListItem<TTableLocks, TTag>;
        return !static_cast<const TItem*>(this)->Empty();
    }

    TPathId GetTableId() const { return TableId; }

    void AddShardLock(TLockInfo* lock);
    void AddPointLock(const TPointKey& point, TLockInfo* lock);
    void AddRangeLock(const TRangeKey& range, TLockInfo* lock);
    void AddWriteLock(TLockInfo* lock);
    void RemoveReadLock(TLockInfo* lock);
    void RemoveShardLock(TLockInfo* lock);
    void RemoveRangeLock(TLockInfo* lock);
    void RemoveWriteLock(TLockInfo* lock);

    ui64 NumKeyColumns() const {
        return KeyColumnTypes.size();
    }

    NScheme::TTypeInfo GetKeyColumnType(ui32 pos) const {
        Y_ABORT_UNLESS(pos < KeyColumnTypes.size());
        return KeyColumnTypes[pos];
    }

    void UpdateKeyColumnsTypes(const TVector<NScheme::TTypeInfo>& keyTypes) {
        Y_ABORT_UNLESS(KeyColumnTypes.size() <= keyTypes.size());
        if (KeyColumnTypes.size() < keyTypes.size()) {
            KeyColumnTypes = keyTypes;
            Ranges.SetKeyTypes(keyTypes);
        }
    }

    bool HasShardLocks() const { return !ShardLocks.empty(); }
    bool HasRangeLocks() const { return Ranges.Size() > 0; }
    ui64 RangeCount() const { return Ranges.Size(); }

    void Clear() {
        Ranges.Clear();
        ShardLocks.clear();
        WriteLocks.clear();
    }

    template<class TCallback>
    void ForEachRangeLock(TCallback&& callback) {
        Ranges.EachRange([&callback](const TRangeTreeBase::TRange&, TLockInfo* lock) {
            callback(lock);
        });
    }

    template<class TCallback>
    void ForEachShardLock(TCallback&& callback) {
        for (TLockInfo* lock : ShardLocks) {
            callback(lock);
        }
    }

    template<class TCallback>
    void ForEachWriteLock(TCallback&& callback) {
        for (TLockInfo* lock : WriteLocks) {
            callback(lock);
        }
    }

private:
    const TPathId TableId;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TRangeTreap<TLockInfo*> Ranges;
    THashSet<TLockInfo*> ShardLocks;
    THashSet<TLockInfo*> WriteLocks;
};

/// Owns and manages locks
class TLockLocker {
    friend class TSysLocks;

public:
    /// Prevent unlimited lock's count growth
    static constexpr ui64 LockLimit() {
        // Valgrind and sanitizers are too slow
        // Some tests cannot exhaust default limit in under 5 minutes
        return NValgrind::PlainOrUnderValgrind(
            NSan::PlainOrUnderSanitizer(
                16 * 1024,
                1024),
            1024);
    }

    /// We don't expire locks until this time limit after they are created
    static constexpr TDuration LockTimeLimit() { return TDuration::Minutes(5); }

    template <typename T>
    TLockLocker(const T * self)
        : Self(new TLocksDataShardAdapter<T>(self))
    {}

    ~TLockLocker() {
        for (auto& t : Tables)
            t.second->Clear();
        Tables.clear();
    }

    void AddPointLock(const TLockInfo::TPtr& lock, const TPointKey& key);
    void AddRangeLock(const TLockInfo::TPtr& lock, const TRangeKey& key);
    void AddShardLock(const TLockInfo::TPtr& lock, TIntrusiveList<TTableLocks, TTableLocksReadListTag>& readTables);
    void AddWriteLock(const TLockInfo::TPtr& lock, TIntrusiveList<TTableLocks, TTableLocksWriteListTag>& writeTables);

    TLockInfo::TPtr GetLock(ui64 lockTxId, const TRowVersion& at) const;

    ui64 LocksCount() const { return Locks.size(); }
    ui64 BrokenLocksCount() const { return BrokenLocksCount_; }

    void BreakLocks(TIntrusiveList<TLockInfo, TLockInfoBreakListTag>& locks, const TRowVersion& at);
    void ForceBreakLock(ui64 lockId);
    void RemoveLock(ui64 lockTxId, ILocksDb* db);

    TLockInfo* FindLockPtr(ui64 lockId) const {
        auto it = Locks.find(lockId);
        if (it != Locks.end()) {
            return it->second.Get();
        } else {
            return nullptr;
        }
    }

    TTableLocks* FindTablePtr(const TTableId& tableId) const {
        auto it = Tables.find(tableId.PathId);
        if (it != Tables.end()) {
            return it->second.Get();
        } else {
            return nullptr;
        }
    }

    bool TableHasRangeLocks(const TTableId& tableId) const {
        auto it = Tables.find(tableId.PathId);
        if (it == Tables.end())
            return false;
        return it->second->HasRangeLocks();
    }

    TPointKey MakePoint(const TTableId& tableId, TConstArrayRef<TCell> point) const {
        return TPointKey{
            GetTableLocks(tableId),
            TOwnedCellVec(point),
        };
    }

    TRangeKey MakeRange(const TTableId& tableId, const TTableRange& range) const {
        Y_ABORT_UNLESS(!range.Point);
        return TRangeKey{
            GetTableLocks(tableId),
            TOwnedCellVec(range.From),
            TOwnedCellVec(range.To),
            range.InclusiveFrom,
            range.InclusiveTo,
        };
    }

    void UpdateSchema(const TPathId& tableId, const TVector<NScheme::TTypeInfo>& keyColumnTypes);
    void RemoveSchema(const TPathId& tableId, ILocksDb* db);
    bool ForceShardLock(const TPathId& tableId) const;
    bool ForceShardLock(const TIntrusiveList<TTableLocks, TTableLocksReadListTag>& readTables) const;

    void ScheduleBrokenLock(TLockInfo* lock);
    void ScheduleRemoveBrokenRanges(ui64 lockId, const TRowVersion& at);

    TPendingSubscribeLock NextPendingSubscribeLock() {
        TPendingSubscribeLock result;
        if (!PendingSubscribeLocks.empty()) {
            result = PendingSubscribeLocks.front();
            PendingSubscribeLocks.pop_front();
        }
        return result;
    }

    void RemoveSubscribedLock(ui64 lockId, ILocksDb* db);

    ui32 Generation() const { return Self->Generation(); }
    ui64 IncCounter() { return Counter++; };

    void Clear() {
        for (auto& pr : Tables) {
            pr.second->Clear();
        }
        Locks.clear();
        ShardLocks.clear();
        BrokenLocks.Clear();
        CleanupPending.clear();
        CleanupCandidates.clear();
        PendingSubscribeLocks.clear();
    }

    const THashMap<ui64, TLockInfo::TPtr>& GetLocks() const {
        return Locks;
    }

private:
    const THolder<TLocksDataShard> Self;
    THashMap<ui64, TLockInfo::TPtr> Locks; // key is LockId
    THashMap<TPathId, TTableLocks::TPtr> Tables;
    THashSet<ui64> ShardLocks;
    // A list of locks that may be removed when enough time passes
    TIntrusiveList<TLockInfo, TLockInfoExpireListTag> ExpireQueue;
    // A list of broken, but not yet removed locks
    TIntrusiveList<TLockInfo, TLockInfoBrokenPersistentListTag> BrokenPersistentLocks;
    TIntrusiveList<TLockInfo, TLockInfoBrokenListTag> BrokenLocks;
    size_t BrokenLocksCount_ = 0;
    // A queue of locks that need their ranges to be cleaned up
    TVector<ui64> CleanupPending;
    TPriorityQueue<TVersionedLockId> CleanupCandidates;
    TList<TPendingSubscribeLock> PendingSubscribeLocks;
    ui64 Counter = 0;

    TTableLocks::TPtr GetTableLocks(const TTableId& table) const {
        auto it = Tables.find(table.PathId);
        Y_ABORT_UNLESS(it != Tables.end());
        return it->second;
    }

    void RemoveBrokenRanges();

    TLockInfo::TPtr GetOrAddLock(ui64 lockId, ui32 lockNodeId);
    TLockInfo::TPtr AddLock(const ILocksDb::TLockRow& row);
    void RemoveOneLock(ui64 lockId, ILocksDb* db = nullptr);

    void SaveBrokenPersistentLocks(ILocksDb* db);
};

/// A portion of locks update
struct TLocksUpdate {
    ui64 LockTxId = 0;
    ui32 LockNodeId = 0;
    TLockInfo::TPtr Lock;

    TStackVec<TPointKey, 4> PointLocks;
    TStackVec<TRangeKey, 4> RangeLocks;

    TIntrusiveList<TTableLocks, TTableLocksReadListTag> ReadTables;
    TIntrusiveList<TTableLocks, TTableLocksWriteListTag> WriteTables;
    TIntrusiveList<TTableLocks, TTableLocksAffectedListTag> AffectedTables;

    TIntrusiveList<TLockInfo, TLockInfoBreakListTag> BreakLocks;
    TIntrusiveList<TTableLocks, TTableLocksBreakShardListTag> BreakShardLocks;
    TIntrusiveList<TTableLocks, TTableLocksBreakRangeListTag> BreakRangeLocks;

    TIntrusiveList<TLockInfo, TLockInfoReadConflictListTag> ReadConflictLocks;
    TIntrusiveList<TLockInfo, TLockInfoWriteConflictListTag> WriteConflictLocks;
    TIntrusiveList<TTableLocks, TTableLocksWriteConflictShardListTag> WriteConflictShardLocks;
    absl::flat_hash_set<ui64> VolatileDependencies;

    TIntrusiveList<TLockInfo, TLockInfoEraseListTag> EraseLocks;

    TRowVersion CheckVersion = TRowVersion::Max();
    TRowVersion BreakVersion = TRowVersion::Min();

    bool BreakOwn = false;

    ~TLocksUpdate();

    bool HasLocks() const {
        return bool(AffectedTables) || bool(ReadConflictLocks) || bool(WriteConflictLocks);
    }

    void AddRangeLock(const TRangeKey& range) {
        ReadTables.PushBack(range.Table.Get());
        AffectedTables.PushBack(range.Table.Get());
        RangeLocks.push_back(range);
    }

    void AddPointLock(const TPointKey& key) {
        ReadTables.PushBack(key.Table.Get());
        AffectedTables.PushBack(key.Table.Get());
        PointLocks.push_back(key);
    }

    void AddWriteLock(TTableLocks* table) {
        WriteTables.PushBack(table);
        AffectedTables.PushBack(table);
    }

    void AddBreakLock(TLockInfo* lock) {
        BreakLocks.PushBack(lock);
    }

    void AddBreakShardLocks(TTableLocks* table) {
        BreakShardLocks.PushBack(table);
    }

    void AddBreakRangeLocks(TTableLocks* table) {
        BreakRangeLocks.PushBack(table);
    }

    void FlattenBreakLocks() {
        while (BreakShardLocks) {
            TTableLocks* table = BreakShardLocks.PopFront();
            table->ForEachShardLock([this](TLockInfo* lock) {
                BreakLocks.PushBack(lock);
            });
        }
        while (BreakRangeLocks) {
            TTableLocks* table = BreakRangeLocks.PopFront();
            table->ForEachRangeLock([this](TLockInfo* lock) {
                BreakLocks.PushBack(lock);
            });
        }
    }

    void AddReadConflictLock(TLockInfo* lock) {
        ReadConflictLocks.PushBack(lock);
    }

    void AddWriteConflictLock(TLockInfo* lock) {
        WriteConflictLocks.PushBack(lock);
    }

    void AddWriteConflictShardLocks(TTableLocks* table) {
        WriteConflictShardLocks.PushBack(table);
    }

    void AddVolatileDependency(ui64 txId) {
        VolatileDependencies.insert(txId);
    }

    void AddEraseLock(TLockInfo* lock) {
        EraseLocks.PushBack(lock);
    }

    void BreakSetLocks() {
        BreakOwn = true;
    }
};

struct TLocksCache {
    THashMap<ui64, TSysTables::TLocksTable::TLock> Locks;
};

enum class EEnsureCurrentLock {
    // New lock was created or an existing unbroken lock was found.
    Success,
    // Lock was already broken, this is not an error for some operations, e.g.
    // readonly snapshot operations may usually continue until they write.
    Broken,
    // Some constraint prevents adding new lock, e.g. there are too many locks
    // or not enough memory. This is usually similar to Broken.
    TooMany,
    // Operation must abort due to temporal violation. This may happen when
    // transaction was already marked committed or aborted in the transaction
    // map, but not yet fully compacted. New reads and especially writes may
    // cause inconsistencies or data corruption and cannot be performed.
    Abort,
};

/// /sys/locks table logic
class TSysLocks {
public:
    using TLocksTable = TSysTables::TLocksTable;
    using TLock = TLocksTable::TLock;

    template <typename T>
    TSysLocks(const T * self)
        : Self(new TLocksDataShardAdapter<T>(self))
        , Locker(self)
    {}

    void SetupUpdate(TLocksUpdate* update, ILocksDb* db = nullptr) noexcept {
        Y_ABORT_UNLESS(!Update, "Cannot setup a recursive update");
        Y_ABORT_UNLESS(update, "Cannot setup a nullptr update");
        Update = update;
        Db = db;
    }

    void ResetUpdate() noexcept {
        if (Y_LIKELY(Update)) {
            if (Update->Lock && Update->Lock->Empty()) {
                Locker.RemoveLock(Update->LockTxId, nullptr);
            }
            Update = nullptr;
            Db = nullptr;
        }
    }

    void SetAccessLog(TLocksCache* log) {
        AccessLog = log;
    }

    void SetCache(TLocksCache* cache) {
        Cache = cache;
    }

    ui64 CurrentLockTxId() const {
        Y_ABORT_UNLESS(Update);
        return Update->LockTxId;
    }

    void UpdateSchema(const TPathId& tableId, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
        Locker.UpdateSchema(tableId, keyColumnTypes);
    }

    void RemoveSchema(const TPathId& tableId, ILocksDb* db) {
        Locker.RemoveSchema(tableId, db);
    }

    TVector<TLock> ApplyLocks();
    ui64 ExtractLockTxId(const TArrayRef<const TCell>& syslockKey) const;
    TLock GetLock(const TArrayRef<const TCell>& syslockKey) const;
    void EraseLock(ui64 lockId);
    void EraseLock(const TArrayRef<const TCell>& syslockKey);
    void CommitLock(const TArrayRef<const TCell>& syslockKey);
    void SetLock(const TTableId& tableId, const TArrayRef<const TCell>& key);
    void SetLock(const TTableId& tableId, const TTableRange& range);
    void SetWriteLock(const TTableId& tableId, const TArrayRef<const TCell>& key);
    void BreakLock(ui64 lockId);
    void BreakLocks(const TTableId& tableId, const TArrayRef<const TCell>& key);
    void AddReadConflict(ui64 conflictId);
    void AddWriteConflict(ui64 conflictId);
    void AddWriteConflict(const TTableId& tableId, const TArrayRef<const TCell>& key);
    void AddVolatileDependency(ui64 txId);
    void BreakAllLocks(const TTableId& tableId);
    void BreakSetLocks();
    bool IsMyKey(const TArrayRef<const TCell>& key) const;
    bool HasCurrentWriteLock(const TTableId& tableId) const;
    bool HasCurrentWriteLocks() const;
    bool HasWriteLocks(const TTableId& tableId) const;

    /**
     * Ensures current update has a valid lock pointer
     *
     * Prerequisites: TSetupSysLocks with LockId and LocksDb is active, and
     *                operation is planning to set read or write locks.
     *
     * Returns Success when a new lock is allocated or an existing (unbroken)
     * lock is found. Returns Broken when a lock is likely to fail, e.g. due
     * to memory or other constraints. Returns Abort when operation must abort
     * early, e.g. because the given LockId cannot be reused.
     */
    EEnsureCurrentLock EnsureCurrentLock();

    ui64 LocksCount() const { return Locker.LocksCount(); }
    ui64 BrokenLocksCount() const { return Locker.BrokenLocksCount(); }

    TLockInfo::TPtr GetRawLock(ui64 lockTxId, const TRowVersion& at = TRowVersion::Max()) const {
        return Locker.GetLock(lockTxId, at);
    }

    bool IsBroken(ui64 lockTxId, const TRowVersion& at = TRowVersion::Max()) const {
        TLockInfo::TPtr txLock = Locker.GetLock(lockTxId, at);
        if (txLock)
            return txLock->IsBroken(at);
        return true;
    }

    TPendingSubscribeLock NextPendingSubscribeLock() {
        return Locker.NextPendingSubscribeLock();
    }

    void RemoveSubscribedLock(ui64 lockId, ILocksDb* db) {
        Locker.RemoveSubscribedLock(lockId, db);
    }

    void UpdateCounters();
    void UpdateCounters(ui64 counter);

    bool Load(ILocksDb& db);

    const THashMap<ui64, TLockInfo::TPtr>& GetLocks() const {
        return Locker.GetLocks();
    }

private:
    THolder<TLocksDataShard> Self;
    TLockLocker Locker;
    TLocksUpdate* Update = nullptr;
    TLocksCache* AccessLog = nullptr;
    TLocksCache* Cache = nullptr;
    ILocksDb* Db = nullptr;

    TLock MakeLock(ui64 lockTxId, ui32 generation, ui64 counter, const TPathId& pathId, bool hasWrites) const;
    TLock MakeAndLogLock(ui64 lockTxId, ui32 generation, ui64 counter, const TPathId& pathId, bool hasWrites) const;

    static ui64 GetLockId(const TArrayRef<const TCell>& key) {
        ui64 lockId;
        bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::LockId, lockId);
        Y_ABORT_UNLESS(ok);
        return lockId;
    }
};

} // namespace NDataShard
} // namespace NKikimr
