#pragma once

#include "sys_tables.h"
#include "range_treap.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/cache/cache.h>
#include <util/generic/list.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>

#include <util/system/valgrind.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NDataShard {

struct TUserTable;

class TLocksDataShard {
public:
    TLocksDataShard(TTabletCountersBase* const &tabletCounters)
        : TabletCounters(tabletCounters)
    {
    }

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

    TTabletCountersBase* const &TabletCounters;
};

template <typename T>
class TLocksDataShardAdapter : public TLocksDataShard
{
public:
    TLocksDataShardAdapter(const T *self)
        : TLocksDataShard(self->TabletCounters)
        , Self(self)
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

/// Aggregates shard, point and range locks
class TLockInfo : public TSimpleRefCount<TLockInfo> {
    friend class TTableLocks;
    friend class TLockLocker;

public:
    using TPtr = TIntrusivePtr<TLockInfo>;

    TLockInfo(TLockLocker * locker, ui64 lockId, ui32 lockNodeId);
    ~TLockInfo();

    ui64 GetCounter(const TRowVersion& at = TRowVersion::Max()) const { return !BreakVersion || at < *BreakVersion ? Counter : Max<ui64>(); }
    bool IsBroken(const TRowVersion& at = TRowVersion::Max()) const { return GetCounter(at) == Max<ui64>(); }

    size_t NumPoints() const { return Points.size(); }
    size_t NumRanges() const { return Ranges.size(); }
    bool IsShardLock() const { return ShardLock; }
    //ui64 MemorySize() const { return 1; } // TODO

    bool MayHavePointsAndRanges() const { return !ShardLock && (!BreakVersion || *BreakVersion); }

    ui64 GetLockId() const { return LockId; }
    ui32 GetLockNodeId() const { return LockNodeId; }

    TInstant GetCreationTime() const { return CreationTime; }
    const THashSet<TPathId>& GetAffectedTables() const { return AffectedTables; }

    const TVector<TPointKey>& GetPoints() const { return Points; }
    const TVector<TRangeKey>& GetRanges() const { return Ranges; }

private:
    void AddShardLock(const THashSet<TPathId>& affectedTables);
    bool AddPoint(const TPointKey& point);
    bool AddRange(const TRangeKey& range);
    void SetBroken(const TRowVersion& at);

private:
    TLockLocker * Locker;
    ui64 LockId;
    ui32 LockNodeId;
    ui64 Counter;
    TInstant CreationTime;
    THashSet<TPathId> AffectedTables;
    TVector<TPointKey> Points;
    TVector<TRangeKey> Ranges;
    bool ShardLock = false;

    std::optional<TRowVersion> BreakVersion;
};

///
class TTableLocks : public TSimpleRefCount<TTableLocks> {
public:
    using TPtr = TIntrusivePtr<TTableLocks>;

    static constexpr ui32 SavedKeys = 64;

    TTableLocks(const TPathId& tableId)
        : TableId(tableId)
    {}

    TPathId GetTableId() const { return TableId; }

    void AddPointLock(const TPointKey& point, const TLockInfo::TPtr& lock);
    void AddRangeLock(const TRangeKey& range, const TLockInfo::TPtr& lock);
    void RemoveLock(const TLockInfo::TPtr& lock);
    void BreakLocks(TConstArrayRef<TCell> key, const TRowVersion& at);
    void BreakAllLocks(const TRowVersion& at);

    ui64 NumKeyColumns() const {
        return KeyColumnTypes.size();
    }

    NScheme::TTypeId GetKeyColumnType(ui32 pos) const {
        Y_VERIFY(pos < KeyColumnTypes.size());
        return KeyColumnTypes[pos];
    }

    void UpdateKeyColumnsTypes(const TVector<NScheme::TTypeId>& keyTypes) {
        Y_VERIFY(KeyColumnTypes.size() <= keyTypes.size());
        if (KeyColumnTypes.size() < keyTypes.size()) {
            KeyColumnTypes = keyTypes;
            Ranges.SetKeyTypes(keyTypes);
        }
    }

    bool HasLocks() const { return Ranges.Size() > 0; }
    ui64 RangeCount() const { return Ranges.Size(); }

    void Clear() {
        Ranges.Clear();
    }

private:
    const TPathId TableId;
    TVector<NScheme::TTypeId> KeyColumnTypes;
    TRangeTreap<TLockInfo*> Ranges;
};

/// Owns and manages locks
class TLockLocker {
public:
    /// Prevent unlimited lock's count growth
    class TLockLimiter {
    public:
        static constexpr ui32 TimeLimitMSec() { return 5 * 60 * 1000; }
        static constexpr ui64 LockLimit() {
            // Valgrind and sanitizers are too slow
            // Some tests cannot exhaust default limit in under 5 minutes
            return NValgrind::PlainOrUnderValgrind(
                NSan::PlainOrUnderSanitizer(
                    16 * 1024,
                    1024),
                1024);
        }

        TLockLimiter(TLockLocker * parent)
            : Parent(parent)
            , LocksQueue(2 * LockLimit()) // it should be greater than LockLimit
        {}

        ui64 LocksCount() const { return Parent->LocksCount(); }

        TLockInfo::TPtr TryAddLock(ui64 lockId, ui32 lockNodeId);
        void RemoveLock(ui64 lockId);
        void TouchLock(ui64 lockId);

        // TODO: AddPoint, AddRange

    private:
        TLockLocker * Parent;
        TLRUCache<ui64, TInstant> LocksQueue;
    };

    template <typename T>
    TLockLocker(const T * self)
        : Self(new TLocksDataShardAdapter<T>(self))
        , Limiter(this)
        , Counter(0)
    {}

    ~TLockLocker() {
        for (auto& t : Tables)
            t.second->Clear();
        Tables.clear();
    }

    TLockInfo::TPtr AddShardLock(ui64 lockTxId, ui32 lockNodeId, const THashSet<TPathId>& affectedTables, const TRowVersion& at);
    TLockInfo::TPtr AddPointLock(ui64 lockTxId, ui32 lockNodeId, const TPointKey& key, const TRowVersion& at);
    TLockInfo::TPtr AddRangeLock(ui64 lockTxId, ui32 lockNodeId, const TRangeKey& key, const TRowVersion& at);
    TLockInfo::TPtr GetLock(ui64 lockTxId, const TRowVersion& at) const;

    ui64 LocksCount() const { return Locks.size(); }
    ui64 BrokenLocksCount() const { return BrokenLocks.size() + BrokenCandidates.size(); }

    void BreakShardLocks(const TRowVersion& at);
    void BreakLocks(const TPointKey& point, const TRowVersion& at);
    void BreakAllLocks(const TPathId& pathId, const TRowVersion& at);
    void BreakLock(ui64 lockTxId, const TRowVersion& at);
    void RemoveLock(ui64 lockTxId);

    bool TableHasLocks(const TTableId& tableId) const {
        auto it = Tables.find(tableId.PathId);
        if (it == Tables.end())
            return false;
        return it->second->HasLocks();
    }

    TPointKey MakePoint(const TTableId& tableId, TConstArrayRef<TCell> point) const {
        return TPointKey{
            GetTableLocks(tableId),
            TOwnedCellVec(point),
        };
    }

    TRangeKey MakeRange(const TTableId& tableId, const TTableRange& range) const {
        Y_VERIFY(!range.Point);
        return TRangeKey{
            GetTableLocks(tableId),
            TOwnedCellVec(range.From),
            TOwnedCellVec(range.To),
            range.InclusiveFrom,
            range.InclusiveTo,
        };
    }

    void UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo);
    void RemoveSchema(const TPathId& tableId);
    bool ForceShardLock(const THashSet<TPathId>& rangeTables) const;

    // optimisation: set to remove broken lock at next Remove()
    void ScheduleLockCleanup(ui64 lockId, const TRowVersion& at);

    TPendingSubscribeLock NextPendingSubscribeLock() {
        TPendingSubscribeLock result;
        if (!PendingSubscribeLocks.empty()) {
            result = PendingSubscribeLocks.front();
            PendingSubscribeLocks.pop_front();
        }
        return result;
    }

    void RemoveSubscribedLock(ui64 lockId);

    ui64 IncCounter() { return Counter++; };

private:
    THolder<TLocksDataShard> Self;
    THashMap<ui64, TLockInfo::TPtr> Locks; // key is LockId
    THashMap<TPathId, TTableLocks::TPtr> Tables;
    THashSet<ui64> ShardLocks;
    TVector<ui64> BrokenLocks; // LockIds of broken locks (optimisation)
    TVector<ui64> CleanupPending; // LockIds of broken locks with pending cleanup
    TPriorityQueue<TVersionedLockId> BrokenCandidates;
    TPriorityQueue<TVersionedLockId> CleanupCandidates;
    TList<TPendingSubscribeLock> PendingSubscribeLocks;
    TLockLimiter Limiter;
    ui64 Counter;

    TTableLocks::TPtr GetTableLocks(const TTableId& table) const {
        auto it = Tables.find(table.PathId);
        Y_VERIFY(it != Tables.end());
        return it->second;
    }

    void RemoveBrokenRanges();

    TLockInfo::TPtr GetOrAddLock(ui64 lockId, ui32 lockNodeId);
    void RemoveOneLock(ui64 lockId);
    void RemoveBrokenLocks();
};

/// A portion of locks update
struct TLocksUpdate {
    ui64 LockTxId = 0;
    ui32 LockNodeId = 0;
    TVector<TPointKey> PointLocks;
    TVector<TRangeKey> RangeLocks;
    TVector<TPointKey> PointBreaks;
    THashSet<TPathId> AllBreaks;
    TVector<ui64> Erases;
    bool ShardLock = false;
    bool ShardBreak = false;
    THashSet<TPathId> AffectedTables;
    THashSet<TPathId> RangeTables;

    TRowVersion CheckVersion = TRowVersion::Max();
    TRowVersion BreakVersion = TRowVersion::Min();

    bool BreakOwn = false;

    void Clear() {
        LockTxId = 0;
        LockNodeId = 0;
        ShardLock = false;
        ShardBreak = false;
        PointLocks.clear();
        PointBreaks.clear();
        AllBreaks.clear();
        Erases.clear();
    }

    bool HasLocks() const {
        return ShardLock || PointLocks.size() || RangeLocks.size();
    }

    void SetLock(const TTableId& tableId, const TRangeKey& range, ui64 lockId, ui32 lockNodeId) {
        Y_VERIFY(LockTxId == lockId && LockNodeId == lockNodeId);
        AffectedTables.insert(tableId.PathId);
        RangeTables.insert(tableId.PathId);
        RangeLocks.push_back(range);
    }

    void SetLock(const TTableId& tableId, const TPointKey& key, ui64 lockId, ui32 lockNodeId) {
        Y_VERIFY(LockTxId == lockId && LockNodeId == lockNodeId);
        AffectedTables.insert(tableId.PathId);
        PointLocks.push_back(key);
    }

    void BreakLocks(const TPointKey& key) {
        PointBreaks.push_back(key);
    }

    void BreakAllLocks(const TTableId& tableId) {
        AllBreaks.insert(tableId.PathId);
    }

    void BreakShardLock() {
        ShardBreak = true;
    }

    void EraseLock(ui64 lockId) {
        Erases.push_back(lockId);
    }

    void BreakSetLocks(ui64 lockId, ui32 lockNodeId) {
        Y_VERIFY(LockTxId == lockId && LockNodeId == lockNodeId);
        BreakOwn = true;
    }
};

struct TLocksCache {
    THashMap<ui64, TSysTables::TLocksTable::TLock> Locks;
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
        , Update(nullptr)
        , AccessLog(nullptr)
        , Cache(nullptr)
    {}

    void SetTxUpdater(TLocksUpdate * up) {
        Update = up;
    }

    void SetAccessLog(TLocksCache *log) {
        AccessLog = log;
    }

    void SetCache(TLocksCache *cache) {
        Cache = cache;
    }

    ui64 CurrentLockTxId() const {
        Y_VERIFY(Update);
        return Update->LockTxId;
    }

    void UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo) {
        Locker.UpdateSchema(tableId, tableInfo);
    }

    void RemoveSchema(const TPathId& tableId) {
        Locker.RemoveSchema(tableId);
    }

    TVector<TLock> ApplyLocks();
    ui64 ExtractLockTxId(const TArrayRef<const TCell>& syslockKey) const;
    TLock GetLock(const TArrayRef<const TCell>& syslockKey) const;
    void EraseLock(const TArrayRef<const TCell>& syslockKey);
    void SetLock(const TTableId& tableId, const TArrayRef<const TCell>& key, ui64 lockTxId, ui32 lockNodeId);
    void SetLock(const TTableId& tableId, const TTableRange& range, ui64 lockTxId, ui32 lockNodeId);
    void BreakLock(const TTableId& tableId, const TArrayRef<const TCell>& key);
    void BreakAllLocks(const TTableId& tableId);
    void BreakSetLocks(ui64 lockTxId, ui32 lockNodeId);
    bool IsMyKey(const TArrayRef<const TCell>& key) const;

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

    void RemoveSubscribedLock(ui64 lockId) {
        Locker.RemoveSubscribedLock(lockId);
    }

private:
    THolder<TLocksDataShard> Self;
    TLockLocker Locker;
    TLocksUpdate * Update;
    TLocksCache *AccessLog;
    TLocksCache *Cache;

    TLock MakeLock(ui64 lockTxId, ui64 counter, const TPathId& pathId) const;
    TLock MakeAndLogLock(ui64 lockTxId, ui64 counter, const TPathId& pathId) const;

    static ui64 GetLockId(const TArrayRef<const TCell>& key) {
        ui64 lockId;
        bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::LockId, lockId);
        Y_VERIFY(ok);
        return lockId;
    }
};

} // namespace NDataShard
} // namespace NKikimr
