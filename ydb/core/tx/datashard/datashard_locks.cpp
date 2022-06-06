#include "datashard_locks.h"
#include "datashard_impl.h"
#include "datashard_counters.h"
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NDataShard {

// TLockInfo

TLockInfo::TLockInfo(TLockLocker * locker, ui64 lockId)
    : Locker(locker)
    , LockId(lockId)
    , Counter(locker->IncCounter())
    , CreationTime(TAppData::TimeProvider->Now())
{}

TLockInfo::~TLockInfo() {
    // nothing
}

void TLockInfo::AddShardLock(const THashSet<TPathId>& affectedTables) {
    AffectedTables.insert(affectedTables.begin(), affectedTables.end());
    ShardLock = true;
    Points.clear();
    Ranges.clear();
}

bool TLockInfo::AddPoint(const TPointKey& point) {
    AffectedTables.insert(point.Table->GetTableId());
    if (!ShardLock) {
        Points.emplace_back(point);
    }
    return !ShardLock;
}

bool TLockInfo::AddRange(const TRangeKey& range) {
    AffectedTables.insert(range.Table->GetTableId());
    if (!ShardLock) {
        Ranges.emplace_back(range);
    }
    return !ShardLock;
}

void TLockInfo::SetBroken(const TRowVersion& at) {
#if 1 // optimisation: remove at next Remove
    if (!IsBroken(at))
        Locker->ScheduleLockCleanup(LockId, at);
#endif

    if (!BreakVersion || at < *BreakVersion)
        BreakVersion.emplace(at.Step, at.TxId);

    if (at)
        return; // if break version is not TrowVersion::Min() we will postpone actual ranges removal

    Points.clear();
    Ranges.clear();
}

// TTableLocks

void TTableLocks::AddPointLock(const TPointKey& point, const TLockInfo::TPtr& lock) {
    Y_VERIFY(lock->MayHavePointsAndRanges());
    Y_VERIFY(point.Table == this);
    TRangeTreeBase::TOwnedRange added(
            point.Key,
            true,
            point.Key,
            true);
    Ranges.AddRange(std::move(added), lock.Get());
}

void TTableLocks::AddRangeLock(const TRangeKey& range, const TLockInfo::TPtr& lock) {
    Y_VERIFY(lock->MayHavePointsAndRanges());
    Y_VERIFY(range.Table == this);
    // FIXME: we have to force empty From/To to be inclusive due to outdated
    // scripts/tests assuming missing columns are +inf, and that expect
    // non-inclusive +inf to include everything. This clashes with the new
    // notion of missing border columns meaning "any", thus non-inclusive
    // empty key would not include anything. Thankfully when there's at least
    // one column present engines tend to use inclusive for partial keys.
    TRangeTreeBase::TOwnedRange added(
            range.From,
            range.InclusiveFrom || !range.From,
            range.To,
            range.InclusiveTo || !range.To);
    Ranges.AddRange(std::move(added), lock.Get());
}

void TTableLocks::RemoveLock(const TLockInfo::TPtr& lock) {
    Ranges.RemoveRanges(lock.Get());
}

void TTableLocks::BreakLocks(TConstArrayRef<TCell> key, const TRowVersion& at) {
    Ranges.EachIntersection(key, [&](const TRangeTreeBase::TRange&, TLockInfo* lock) {
        lock->SetBroken(at);
    });
}

void TTableLocks::BreakAllLocks(const TRowVersion& at) {
    Ranges.EachRange([&](const TRangeTreeBase::TRange&, TLockInfo* lock) {
        lock->SetBroken(at);
    });
}

// TLockLocker

TLockInfo::TPtr TLockLocker::AddShardLock(ui64 lockTxId, const THashSet<TPathId>& affectedTables, const TRowVersion& at) {
    TLockInfo::TPtr lock = GetOrAddLock(lockTxId);
    if (!lock || lock->IsBroken(at))
        return lock;

    ShardLocks.insert(lockTxId);
    for (const TPathId& tableId : lock->GetAffectedTables()) {
        Tables.at(tableId)->RemoveLock(lock);
    }
    lock->AddShardLock(affectedTables);
    return lock;
}

TLockInfo::TPtr TLockLocker::AddPointLock(ui64 lockId, const TPointKey& point, const TRowVersion& at) {
    TLockInfo::TPtr lock = GetOrAddLock(lockId);
    if (!lock || lock->IsBroken(at))
        return lock;

    if (lock->AddPoint(point)) {
        point.Table->AddPointLock(point, lock);
    }
    return lock;
}

TLockInfo::TPtr TLockLocker::AddRangeLock(ui64 lockId, const TRangeKey& range, const TRowVersion& at) {
    TLockInfo::TPtr lock = GetOrAddLock(lockId);
    if (!lock || lock->IsBroken(at))
        return lock;

    if (lock->AddRange(range)) {
        range.Table->AddRangeLock(range, lock);
    }
    return lock;
}

TLockInfo::TPtr TLockLocker::GetLock(ui64 lockTxId, const TRowVersion& at) const {
    auto it = Locks.find(lockTxId);
    if (it != Locks.end()) {
        TLockInfo::TPtr lock = it->second;
        if (!lock->IsBroken(at))
            return lock;
    }
    return nullptr;
}

void TLockLocker::BreakShardLocks(const TRowVersion& at) {
    for (ui64 lockId : ShardLocks) {
        auto it = Locks.find(lockId);
        if (it != Locks.end()) {
            it->second->SetBroken(at);
        }
    }
    if (!at)
        ShardLocks.clear();
    RemoveBrokenRanges();
}

void TLockLocker::BreakLocks(const TPointKey& point, const TRowVersion& at) {
    point.Table->BreakLocks(point.Key, at);
    RemoveBrokenRanges();
}

void TLockLocker::BreakAllLocks(const TPathId& pathId, const TRowVersion& at) {
    auto it = Tables.find(pathId);
    if (it != Tables.end()) {
        it->second->BreakAllLocks(at);
        RemoveBrokenRanges();

    }
}

void TLockLocker::RemoveBrokenRanges() {
    for (ui64 lockId : CleanupPending) {
        auto it = Locks.find(lockId);
        if (it != Locks.end()) {
            const TLockInfo::TPtr& lock = it->second;

            if (!lock->IsShardLock()) {
                for (const TPathId& tableId : lock->GetAffectedTables()) {
                    Tables.at(tableId)->RemoveLock(lock);
                }
            } else {
                ShardLocks.erase(lockId);
            }
        }
    }
    CleanupPending.clear();

    if (CleanupCandidates.empty())
        return;

    auto till = Self->LastCompleteTxVersion();
    while (!CleanupCandidates.empty() && CleanupCandidates.top().Version <= till) {
        ui64 lockId = CleanupCandidates.top().LockId;
        CleanupCandidates.pop();

        auto it = Locks.find(lockId);
        if (it != Locks.end()) {
            const TLockInfo::TPtr& lock = it->second;

            lock->BreakVersion = TRowVersion::Min();
            lock->Points.clear();
            lock->Ranges.clear();

            if (!lock->IsShardLock()) {
                for (const TPathId& tableId : lock->GetAffectedTables()) {
                    Tables.at(tableId)->RemoveLock(lock);
                }
            } else {
                ShardLocks.erase(lockId);
            }
        }
    }
}

TLockInfo::TPtr TLockLocker::GetOrAddLock(ui64 lockId) {
    auto it = Locks.find(lockId);
    if (it != Locks.end()) {
        Limiter.TouchLock(lockId);
        return it->second;
    }

    TLockInfo::TPtr lock = Limiter.TryAddLock(lockId);
    if (lock)
        Locks[lockId] = lock;
    return lock;
}

void TLockLocker::RemoveOneLock(ui64 lockTxId) {
    auto it = Locks.find(lockTxId);
    if (it != Locks.end()) {
        TLockInfo::TPtr txLock = it->second;

        TDuration lifetime = TAppData::TimeProvider->Now() - txLock->GetCreationTime();
        if (Self->TabletCounters) {
            Self->IncCounter(COUNTER_LOCKS_LIFETIME, lifetime);
            Self->IncCounter(COUNTER_LOCKS_REMOVED);
        }

        if (!txLock->IsShardLock()) {
            for (const TPathId& tableId : txLock->GetAffectedTables()) {
                Tables.at(tableId)->RemoveLock(txLock);
            }
        } else {
            ShardLocks.erase(lockTxId);
        }
        Limiter.RemoveLock(lockTxId);
        Locks.erase(it);
    }
}

void TLockLocker::RemoveBrokenLocks() {
    for (ui64 lockId : BrokenLocks) {
        RemoveOneLock(lockId);
    }
    BrokenLocks.clear();

    if (BrokenCandidates.empty())
        return;

    auto till = Self->LastCompleteTxVersion();
    while (!BrokenCandidates.empty() && BrokenCandidates.top().Version <= till) {
        RemoveOneLock(BrokenCandidates.top().LockId);
        BrokenCandidates.pop();
    }
}

void TLockLocker::BreakLock(ui64 lockId, const TRowVersion& at) {
    if (auto lock = GetLock(lockId, at))
        lock->SetBroken(at);

    RemoveBrokenLocks();
}

void TLockLocker::RemoveLock(ui64 lockId) {
    RemoveBrokenLocks();
    RemoveOneLock(lockId);
}

void TLockLocker::UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo) {
    TTableLocks::TPtr& table = Tables[tableId];
    if (!table)
        table.Reset(new TTableLocks(tableId));
    table->UpdateKeyColumnsTypes(tableInfo.KeyColumnTypes);
}

void TLockLocker::RemoveSchema(const TPathId& tableId) {
    Tables.erase(tableId);
    Y_VERIFY(Tables.empty());
    Locks.clear();
    ShardLocks.clear();
    BrokenLocks.clear();
    CleanupPending.clear();
    BrokenCandidates.clear();
    CleanupCandidates.clear();
}


bool TLockLocker::ForceShardLock(const THashSet<TPathId>& rangeTables) const {
    for (const TPathId& tableId : rangeTables) {
        auto it = Tables.find(tableId);
        Y_VERIFY(it != Tables.end());
        if (it->second->RangeCount() > TLockLimiter::LockLimit())
            return true;
    }
    return false;
}

void TLockLocker::ScheduleLockCleanup(ui64 lockId, const TRowVersion& at) {
    if (at) {
        BrokenCandidates.emplace(lockId, at);
        CleanupCandidates.emplace(lockId, at);
    } else {
        BrokenLocks.push_back(lockId);
        CleanupPending.push_back(lockId);
    }

    if (Self->TabletCounters) {
        Self->IncCounter(COUNTER_LOCKS_BROKEN);
    }
}

// TLockLocker.TLockLimiter

TLockInfo::TPtr TLockLocker::TLockLimiter::TryAddLock(ui64 lockId) {
#if 1
    if (LocksQueue.Size() >= LockLimit()) {
        Parent->RemoveBrokenLocks();
    }
#endif
    if (LocksQueue.Size() >= LockLimit()) {
        TInstant forgetTime = TAppData::TimeProvider->Now() - TDuration::MilliSeconds(TimeLimitMSec());
        auto oldest = LocksQueue.FindOldest();
        if (oldest.Value() >= forgetTime)
            return nullptr;

        if (Parent->Self->TabletCounters) {
            Parent->Self->IncCounter(COUNTER_LOCKS_EVICTED);
        }

        Parent->RemoveOneLock(oldest.Key()); // erase LocksQueue inside
    }

    LocksQueue.Insert(lockId, TAppData::TimeProvider->Now());
    return TLockInfo::TPtr(new TLockInfo(Parent, lockId));
}

void TLockLocker::TLockLimiter::RemoveLock(ui64 lockId) {
    auto it = LocksQueue.FindWithoutPromote(lockId);
    if (it != LocksQueue.End())
        LocksQueue.Erase(it);
}

void TLockLocker::TLockLimiter::TouchLock(ui64 lockId) {
    LocksQueue.Find(lockId);
}

// TSysLocks

TVector<TSysLocks::TLock> TSysLocks::ApplyLocks() {
    Y_VERIFY(Update);

    TMicrosecTimerCounter measureApplyLocks(*Self, COUNTER_APPLY_LOCKS_USEC);

    auto &checkVersion = Update->CheckVersion;
    auto &breakVersion = Update->BreakVersion;

    if (Update->ShardBreak) {
        Locker.BreakShardLocks(breakVersion);
    }

    if (Update->PointBreaks.size()) {
        for (const auto& key : Update->PointBreaks) {
            Locker.BreakLocks(key, breakVersion);
        }
    }

    if (Update->AllBreaks.size()) {
        for (const auto& pathId : Update->AllBreaks) {
            Locker.BreakAllLocks(pathId, breakVersion);
        }
    }

    if (!Update->Erases.empty() && Self->TabletCounters) {
        Self->IncCounter(COUNTER_LOCKS_ERASED, Update->Erases.size());
    }

    for (ui64 lockId : Update->Erases) {
        Y_VERIFY(!Update->HasLocks(), "Can't erase and set locks in one Tx");
        if (breakVersion)
            Locker.BreakLock(lockId, breakVersion);
        else
            Locker.RemoveLock(lockId);
    }

    if (!Update->HasLocks())
        return TVector<TLock>();

    if (Locker.ForceShardLock(Update->RangeTables))
        Update->ShardLock = true;

    ui64 counter = TLock::ErrorNotSet;
    ui32 numNotSet = 0;
    if (Update->BreakOwn) {
        counter = TLock::ErrorAlreadyBroken;
    } else if (Update->ShardLock) {
        TLockInfo::TPtr lock = Locker.AddShardLock(Update->LockTxId, Update->AffectedTables, checkVersion);
        if (lock) {
            Y_VERIFY(counter == lock->GetCounter(checkVersion) || TLock::IsNotSet(counter));
            counter = lock->GetCounter(checkVersion);
        } else {
            ++numNotSet;
        }
    } else {
        for (const auto& key : Update->PointLocks) {
            TLockInfo::TPtr lock = Locker.AddPointLock(Update->LockTxId, key, checkVersion);
            if (lock) {
                Y_VERIFY(counter == lock->GetCounter(checkVersion) || TLock::IsNotSet(counter));
                counter = lock->GetCounter(checkVersion);
            } else {
                ++numNotSet;
            }
        }

        for (const auto& key : Update->RangeLocks) {
            TLockInfo::TPtr lock = Locker.AddRangeLock(Update->LockTxId, key, checkVersion);
            if (lock) {
                Y_VERIFY(counter == lock->GetCounter(checkVersion) || TLock::IsNotSet(counter));
                counter = lock->GetCounter(checkVersion);
            } else {
                ++numNotSet;
            }
        }
    }

    if (numNotSet) {
        counter = TLock::ErrorTooMuch;
    }

    if (Self->TabletCounters) {
        Self->IncCounter(COUNTER_LOCKS_ACTIVE_PER_SHARD, LocksCount());
        Self->IncCounter(COUNTER_LOCKS_BROKEN_PER_SHARD, BrokenLocksCount());
        if (Update->ShardLock) {
            Self->IncCounter(COUNTER_LOCKS_WHOLE_SHARD);
        }

        if (TLock::IsError(counter)) {
            if (TLock::IsBroken(counter)) {
                Self->IncCounter(COUNTER_LOCKS_REJECT_BROKEN);
            } else {
                Self->IncCounter(COUNTER_LOCKS_REJECTED);
            }
        } else {
            Self->IncCounter(COUNTER_LOCKS_ACQUIRED);
        }
    }

    // We have to tell client that there were some locks (even if we don't set them)
    TVector<TLock> out;
    out.reserve(Update->AffectedTables.size());
    for (const TPathId& pathId : Update->AffectedTables) {
        out.emplace_back(MakeLock(Update->LockTxId, counter, pathId));
    }
    return out;
}

ui64 TSysLocks::ExtractLockTxId(const TArrayRef<const TCell>& key) const {
    ui64 lockTxId, tabletId;
    bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::LockId, lockTxId);
    ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::DataShard, tabletId);
    Y_VERIFY(ok && Self->TabletID() == tabletId);
    return lockTxId;
}

TSysLocks::TLock TSysLocks::GetLock(const TArrayRef<const TCell>& key) const {
    ui64 lockTxId, tabletId;
    bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::LockId, lockTxId);
    ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::DataShard, tabletId);
    Y_VERIFY(ok && Self->TabletID() == tabletId);

    if (Cache) {
        auto it = Cache->Locks.find(lockTxId);
        if (it != Cache->Locks.end())
            return it->second;
        return TLock();
    }

    Y_VERIFY(Update);

    auto &checkVersion = Update->CheckVersion;
    TLockInfo::TPtr txLock = Locker.GetLock(lockTxId, checkVersion);
    if (txLock) {
        const auto& tableIds = txLock->GetAffectedTables();
        if (key.size() == 2) { // locks v1
            Y_VERIFY(tableIds.size() == 1);
            return MakeAndLogLock(lockTxId, txLock->GetCounter(checkVersion), *tableIds.begin());
        } else { // locks v2
            Y_VERIFY(key.size() == 4);
            TPathId tableId;
            ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::SchemeShard, tableId.OwnerId);
            ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::PathId, tableId.LocalPathId);
            if (ok && tableId && tableIds.contains(tableId))
                return MakeAndLogLock(lockTxId, txLock->GetCounter(checkVersion), tableId);
        }
    }

    Self->IncCounter(COUNTER_LOCKS_LOST);
    return TLock();
}

void TSysLocks::EraseLock(const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update);
    Update->EraseLock(GetLockId(key));
}

void TSysLocks::SetLock(const TTableId& tableId, const TArrayRef<const TCell>& key, ui64 lockTxId) {
    Y_VERIFY(!TSysTables::IsSystemTable(tableId));
    if (!Self->IsUserTable(tableId))
        return;

    if (lockTxId) {
        Y_VERIFY(Update);
        Update->SetLock(tableId, Locker.MakePoint(tableId, key), lockTxId);
    }
}

void TSysLocks::SetLock(const TTableId& tableId, const TTableRange& range, ui64 lockTxId) {
    if (range.Point) { // if range is point replace it with a point lock
        SetLock(tableId, range.From, lockTxId);
        return;
    }

    Y_VERIFY(!TSysTables::IsSystemTable(tableId));
    if (!Self->IsUserTable(tableId))
        return;

    if (lockTxId) {
        Y_VERIFY(Update);
        Update->SetLock(tableId, Locker.MakeRange(tableId, range), lockTxId);
    }
}

void TSysLocks::BreakLock(const TTableId& tableId, const TArrayRef<const TCell>& key) {
    Y_VERIFY(!tableId.HasSamePath(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks)));
    if (!Self->IsUserTable(tableId))
        return;

    if (Locker.TableHasLocks(tableId))
        Update->BreakLocks(Locker.MakePoint(tableId, key));
    Update->BreakShardLock();
}

void TSysLocks::BreakAllLocks(const TTableId& tableId) {
    Y_VERIFY(!tableId.HasSamePath(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks)));
    if (!Self->IsUserTable(tableId))
        return;

    if (Locker.TableHasLocks(tableId))
        Update->BreakAllLocks(tableId);
    Update->BreakShardLock();
}

void TSysLocks::BreakSetLocks(ui64 lockTxId) {
    Y_VERIFY(Update);

    if (lockTxId)
        Update->BreakSetLocks(lockTxId);
}

bool TSysLocks::IsMyKey(const TArrayRef<const TCell>& key) const {
    ui64 tabletId;
    bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::DataShard, tabletId);
    return ok && (Self->TabletID() == tabletId);
}

TSysLocks::TLock TSysLocks::MakeLock(ui64 lockTxId, ui64 counter, const TPathId& pathId) const {
    TLock lock;
    lock.LockId = lockTxId;
    lock.DataShard = Self->TabletID();
    lock.Generation = Self->Generation();
    lock.Counter = counter;
    lock.SchemeShard = pathId.OwnerId;
    lock.PathId = pathId.LocalPathId;
    return lock;
}

TSysLocks::TLock TSysLocks::MakeAndLogLock(ui64 lockTxId, ui64 counter, const TPathId& pathId) const {
    TLock lock = MakeLock(lockTxId, counter, pathId);
    if (AccessLog)
        AccessLog->Locks[lockTxId] = lock;
    return lock;
}


}}
