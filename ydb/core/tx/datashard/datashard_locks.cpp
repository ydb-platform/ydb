#include "datashard_locks.h"
#include "datashard_impl.h"
#include "datashard_counters.h"
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NDataShard {

/**
 * Locks are tested without an actor system, where actor context is unavailable
 *
 * This class is the minimum context implementation that doesn't log anything
 * when running outside an event handler.
 */
struct TLockLoggerContext {
    TLockLoggerContext() = default;

    NLog::TSettings* LoggerSettings() const {
        return TlsActivationContext ? TlsActivationContext->LoggerSettings() : nullptr;
    }

    bool Send(TAutoPtr<IEventHandle> ev) const {
        return TlsActivationContext ? TlsActivationContext->Send(ev) : false;
    }
};

// Logger requires an l-value, so we use an empty static variable
static TLockLoggerContext LockLoggerContext;

// TLockInfo

TLockInfo::TLockInfo(TLockLocker * locker, ui64 lockId, ui32 lockNodeId)
    : Locker(locker)
    , LockId(lockId)
    , LockNodeId(lockNodeId)
    , Generation(locker->Generation())
    , Counter(locker->IncCounter())
    , CreationTime(TAppData::TimeProvider->Now())
{}

TLockInfo::TLockInfo(TLockLocker * locker, ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, TInstant createTs)
    : Locker(locker)
    , LockId(lockId)
    , LockNodeId(lockNodeId)
    , Generation(generation)
    , Counter(counter)
    , CreationTime(createTs)
    , Persistent(true)
{
    if (counter == Max<ui64>()) {
        BreakVersion.emplace(TRowVersion::Min());
    }
}

TLockInfo::~TLockInfo() {
    if (!ConflictLocks.empty()) {
        for (auto& pr : ConflictLocks) {
            // Ensure there are no dangling pointers
            pr.first->ConflictLocks.erase(this);
        }
        ConflictLocks.clear();
    }
}

void TLockInfo::MakeShardLock() {
    ShardLock = true;
    Points.clear();
    Ranges.clear();
}

bool TLockInfo::AddShardLock(const TPathId& pathId) {
    Y_VERIFY(ShardLock);
    if (ReadTables.insert(pathId).second) {
        UnpersistedRanges = true;
        return true;
    }
    return false;
}

bool TLockInfo::AddPoint(const TPointKey& point) {
    if (ReadTables.insert(point.Table->GetTableId()).second) {
        UnpersistedRanges = true;
    }
    if (!ShardLock) {
        Points.emplace_back(point);
    }
    return !ShardLock;
}

bool TLockInfo::AddRange(const TRangeKey& range) {
    if (ReadTables.insert(range.Table->GetTableId()).second) {
        UnpersistedRanges = true;
    }
    if (!ShardLock) {
        Ranges.emplace_back(range);
    }
    return !ShardLock;
}

bool TLockInfo::AddWriteLock(const TPathId& pathId) {
    if (WriteTables.insert(pathId).second) {
        UnpersistedRanges = true;
        return true;
    }
    return false;
}

void TLockInfo::SetBroken(TRowVersion at) {
    if (Persistent) {
        // Persistent locks always break completely
        at = TRowVersion::Min();
    }

    if (!IsBroken(at)) {
        LOG_TRACE_S(LockLoggerContext, NKikimrServices::TX_DATASHARD, "Lock " << LockId << " marked broken at " << at);

        BreakVersion = at;
        Locker->ScheduleRemoveBrokenRanges(LockId, at);

        if (!at) {
            // This lock is now broken in all versions, clear as soon as possible
            Counter = Max<ui64>();
            Points.clear();
            Ranges.clear();
            Locker->ScheduleBrokenLock(this);
        }
    }
}

void TLockInfo::OnRemoved() {
    if (!IsBroken()) {
        BreakVersion = TRowVersion::Min();
        Counter = Max<ui64>();
        Points.clear();
        Ranges.clear();
    }
}

void TLockInfo::PersistLock(ILocksDb* db) {
    Y_VERIFY(!IsPersistent());
    Y_VERIFY(db, "Cannot persist lock without a db");
    db->PersistAddLock(LockId, LockNodeId, Generation, Counter, CreationTime.MicroSeconds());
    Persistent = true;

    PersistRanges(db);
    PersistConflicts(db);
}

void TLockInfo::PersistBrokenLock(ILocksDb* db) {
    Y_VERIFY(IsPersistent());
    Y_VERIFY(db, "Cannot persist lock without a db");
    db->PersistLockCounter(LockId, Max<ui64>());
}

void TLockInfo::PersistRemoveLock(ILocksDb* db) {
    Y_VERIFY(IsPersistent());
    Y_VERIFY(db, "Cannot persist lock without a db");

    // Remove persistent conflicts
    for (auto& pr : ConflictLocks) {
        TLockInfo* otherLock = pr.first;
        if (otherLock->IsPersistent()) {
            if (!!(pr.second & ELockConflictFlags::BreakThemOnOurCommit)) {
                db->PersistRemoveConflict(LockId, otherLock->LockId);
            }
            if (!!(pr.second & ELockConflictFlags::BreakUsOnTheirCommit)) {
                db->PersistRemoveConflict(otherLock->LockId, LockId);
            }
        }
        otherLock->ConflictLocks.erase(this);
    }
    ConflictLocks.clear();

    // Remove persistent ranges
    for (auto& range : PersistentRanges) {
        db->PersistRemoveRange(LockId, range.Id);
    }
    PersistentRanges.clear();

    // Remove the lock itself
    db->PersistRemoveLock(LockId);
}

void TLockInfo::PersistRanges(ILocksDb* db) {
    Y_VERIFY(IsPersistent());
    if (UnpersistedRanges) {
        for (const TPathId& pathId : ReadTables) {
            PersistAddRange(pathId, ELockRangeFlags::Read, db);
        }
        for (const TPathId& pathId : WriteTables) {
            PersistAddRange(pathId, ELockRangeFlags::Write, db);
        }
        UnpersistedRanges = false;
    }
}

void TLockInfo::PersistAddRange(const TPathId& tableId, ELockRangeFlags flags, ILocksDb* db) {
    Y_VERIFY(IsPersistent());
    Y_VERIFY(db, "Cannot persist ranges without a db");
    // We usually have a single range with flags, so linear search is ok
    ui64 maxId = 0;
    for (auto& range : PersistentRanges) {
        if (range.TableId == tableId) {
            auto prevFlags = range.Flags;
            range.Flags |= flags;
            if (range.Flags != prevFlags) {
                db->PersistRangeFlags(LockId, range.Id, ui64(range.Flags));
            }
            return;
        }
        maxId = Max(maxId, range.Id);
    }
    auto& range = PersistentRanges.emplace_back();
    range.Id = maxId + 1;
    range.TableId = tableId;
    range.Flags = flags;
    db->PersistAddRange(LockId, range.Id, range.TableId, ui64(range.Flags));
}

void TLockInfo::AddConflict(TLockInfo* otherLock, ILocksDb* db) {
    Y_VERIFY(this != otherLock, "Lock cannot conflict with itself");
    Y_VERIFY(LockId != otherLock->LockId, "Unexpected conflict between a pair of locks with the same id");

    auto& flags = ConflictLocks[otherLock];
    if (!(flags & ELockConflictFlags::BreakThemOnOurCommit)) {
        flags |= ELockConflictFlags::BreakThemOnOurCommit;
        auto& otherFlags = otherLock->ConflictLocks[this];
        otherFlags |= ELockConflictFlags::BreakUsOnTheirCommit;
        if (IsPersistent() && otherLock->IsPersistent()) {
            // Any conflict between persistent locks is also persistent
            Y_VERIFY(db, "Cannot persist conflicts without a db");
            db->PersistAddConflict(LockId, otherLock->LockId);
        }
    }
}

void TLockInfo::PersistConflicts(ILocksDb* db) {
    Y_VERIFY(IsPersistent());
    Y_VERIFY(db, "Cannot persist conflicts without a db");
    for (auto& pr : ConflictLocks) {
        TLockInfo* otherLock = pr.first;
        if (!otherLock->IsPersistent()) {
            // We don't persist non-persistent conflicts
            continue;
        }
        if (!!(pr.second & ELockConflictFlags::BreakThemOnOurCommit)) {
            db->PersistAddConflict(LockId, otherLock->LockId);
        }
        if (!!(pr.second & ELockConflictFlags::BreakUsOnTheirCommit)) {
            db->PersistAddConflict(otherLock->LockId, LockId);
        }
    }
}

void TLockInfo::CleanupConflicts() {
    if (IsPersistent()) {
        for (auto it = ConflictLocks.begin(); it != ConflictLocks.end();) {
            TLockInfo* otherLock = it->first;
            if (otherLock->IsPersistent()) {
                // We keep persistent conflict in memory until lock is removed
                ++it;
            } else {
                otherLock->ConflictLocks.erase(this);
                ConflictLocks.erase(it++);
            }
        }
    } else {
        for (auto& pr : ConflictLocks) {
            TLockInfo* otherLock = pr.first;
            otherLock->ConflictLocks.erase(this);
        }
        ConflictLocks.clear();
    }
}

void TLockInfo::RestorePersistentRange(ui64 rangeId, const TPathId& tableId, ELockRangeFlags flags) {
    auto& range = PersistentRanges.emplace_back();
    range.Id = rangeId;
    range.TableId = tableId;
    range.Flags = flags;

    if (!!(range.Flags & ELockRangeFlags::Read)) {
        if (ReadTables.insert(range.TableId).second) {
            ShardLock = true;
            if (auto* table = Locker->FindTablePtr(range.TableId)) {
                table->AddShardLock(this);
            }
        }
    }
    if (!!(range.Flags & ELockRangeFlags::Write)) {
        if (WriteTables.insert(range.TableId).second) {
            if (auto* table = Locker->FindTablePtr(range.TableId)) {
                table->AddWriteLock(this);
            }
        }
    }
}

void TLockInfo::RestorePersistentConflict(TLockInfo* otherLock) {
    Y_VERIFY(IsPersistent() && otherLock->IsPersistent());

    this->ConflictLocks[otherLock] |= ELockConflictFlags::BreakThemOnOurCommit;
    otherLock->ConflictLocks[this] |= ELockConflictFlags::BreakUsOnTheirCommit;
}

// TTableLocks

void TTableLocks::AddShardLock(TLockInfo* lock) {
    ShardLocks.insert(lock);
}

void TTableLocks::AddPointLock(const TPointKey& point, TLockInfo* lock) {
    Y_VERIFY(lock->MayHavePointsAndRanges());
    Y_VERIFY(point.Table == this);
    TRangeTreeBase::TOwnedRange added(
            point.Key,
            true,
            point.Key,
            true);
    Ranges.AddRange(std::move(added), lock);
}

void TTableLocks::AddRangeLock(const TRangeKey& range, TLockInfo* lock) {
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
    Ranges.AddRange(std::move(added), lock);
}

void TTableLocks::AddWriteLock(TLockInfo* lock) {
    WriteLocks.insert(lock);
}

void TTableLocks::RemoveReadLock(TLockInfo* lock) {
    if (lock->IsShardLock()) {
        RemoveShardLock(lock);
    } else {
        RemoveRangeLock(lock);
    }
}

void TTableLocks::RemoveShardLock(TLockInfo* lock) {
    ShardLocks.erase(lock);
}

void TTableLocks::RemoveRangeLock(TLockInfo* lock) {
    Ranges.RemoveRanges(lock);
}

void TTableLocks::RemoveWriteLock(TLockInfo* lock) {
    WriteLocks.erase(lock);
}

// TLockLocker

void TLockLocker::AddPointLock(const TLockInfo::TPtr& lock, const TPointKey& key) {
    if (lock->AddPoint(key)) {
        key.Table->AddPointLock(key, lock.Get());
    } else {
        key.Table->AddShardLock(lock.Get());
    }
}

void TLockLocker::AddRangeLock(const TLockInfo::TPtr& lock, const TRangeKey& key) {
    if (lock->AddRange(key)) {
        key.Table->AddRangeLock(key, lock.Get());
    } else {
        key.Table->AddShardLock(lock.Get());
    }
}

void TLockLocker::AddShardLock(const TLockInfo::TPtr& lock, TIntrusiveList<TTableLocks, TTableLocksReadListTag>& readTables) {
    if (!lock->IsShardLock()) {
        for (const TPathId& tableId : lock->GetReadTables()) {
            Tables.at(tableId)->RemoveRangeLock(lock.Get());
        }
        lock->MakeShardLock();
        for (const TPathId& tableId : lock->GetReadTables()) {
            Tables.at(tableId)->AddShardLock(lock.Get());
        }
    }
    for (auto& table : readTables) {
        const TPathId& tableId = table.GetTableId();
        Y_VERIFY(Tables.at(tableId).Get() == &table);
        if (lock->AddShardLock(tableId)) {
            table.AddShardLock(lock.Get());
        }
    }
}

void TLockLocker::AddWriteLock(const TLockInfo::TPtr& lock, TIntrusiveList<TTableLocks, TTableLocksWriteListTag>& writeTables) {
    for (auto& table : writeTables) {
        const TPathId& tableId = table.GetTableId();
        Y_VERIFY(Tables.at(tableId).Get() == &table);
        if (lock->AddWriteLock(tableId)) {
            table.AddWriteLock(lock.Get());
        }
    }
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

void TLockLocker::BreakLocks(TIntrusiveList<TLockInfo, TLockInfoBreakListTag>& locks, const TRowVersion& at) {
    for (auto& lock : locks) {
        lock.SetBroken(at);
    }

    RemoveBrokenRanges();
}

void TLockLocker::RemoveBrokenRanges() {
    for (ui64 lockId : CleanupPending) {
        auto it = Locks.find(lockId);
        if (it != Locks.end()) {
            const TLockInfo::TPtr& lock = it->second;

            for (const TPathId& tableId : lock->GetReadTables()) {
                Tables.at(tableId)->RemoveReadLock(lock.Get());
            }
            for (const TPathId& tableId : lock->GetWriteTables()) {
                Tables.at(tableId)->RemoveWriteLock(lock.Get());
            }
            lock->CleanupConflicts();
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

            if (lock->Counter == Max<ui64>()) {
                // Skip locks that have been cleaned up already
                continue;
            }

            lock->BreakVersion = TRowVersion::Min();
            lock->Counter = Max<ui64>();
            lock->Points.clear();
            lock->Ranges.clear();
            ScheduleBrokenLock(lock.Get());

            for (const TPathId& tableId : lock->GetReadTables()) {
                Tables.at(tableId)->RemoveReadLock(lock.Get());
            }
            for (const TPathId& tableId : lock->GetWriteTables()) {
                Tables.at(tableId)->RemoveWriteLock(lock.Get());
            }
            lock->CleanupConflicts();
        }
    }
}

TLockInfo::TPtr TLockLocker::GetOrAddLock(ui64 lockId, ui32 lockNodeId) {
    auto it = Locks.find(lockId);
    if (it != Locks.end()) {
        if (it->second->IsInList<TLockInfoExpireListTag>()) {
            ExpireQueue.PushBack(it->second.Get());
        }
        if (lockNodeId && !it->second->LockNodeId) {
            // This should never happen, but better safe than sorry
            it->second->LockNodeId = lockNodeId;
            PendingSubscribeLocks.emplace_back(lockId, lockNodeId);
        }
        return it->second;
    }

    while (Locks.size() >= LockLimit()) {
        if (!BrokenLocks.Empty()) {
            // We remove broken locks first
            TLockInfo* lock = BrokenLocks.Front();
            RemoveOneLock(lock->GetLockId());
            continue;
        }
        if (!ExpireQueue.Empty()) {
            TLockInfo* lock = ExpireQueue.Front();
            if (TAppData::TimeProvider->Now() - lock->GetCreationTime() >= LockTimeLimit()) {
                RemoveOneLock(lock->GetLockId());
                continue;
            }
        }
        // We cannot add any more locks
        return nullptr;
    }

    TLockInfo::TPtr lock(new TLockInfo(this, lockId, lockNodeId));
    Y_VERIFY(!lock->IsPersistent());
    Locks[lockId] = lock;
    if (lockNodeId) {
        PendingSubscribeLocks.emplace_back(lockId, lockNodeId);
    }
    ExpireQueue.PushBack(lock.Get());
    return lock;
}

TLockInfo::TPtr TLockLocker::AddLock(ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, TInstant createTs) {
    Y_VERIFY(Locks.find(lockId) == Locks.end());

    TLockInfo::TPtr lock(new TLockInfo(this, lockId, lockNodeId, generation, counter, createTs));
    Y_VERIFY(lock->IsPersistent());
    Locks[lockId] = lock;
    if (lockNodeId) {
        PendingSubscribeLocks.emplace_back(lockId, lockNodeId);
    }
    return lock;
}

void TLockLocker::RemoveOneLock(ui64 lockTxId, ILocksDb* db) {
    auto it = Locks.find(lockTxId);
    if (it != Locks.end()) {
        TLockInfo::TPtr txLock = it->second;

        TDuration lifetime = TAppData::TimeProvider->Now() - txLock->GetCreationTime();
        if (Self->TabletCounters) {
            Self->IncCounter(COUNTER_LOCKS_LIFETIME, lifetime);
            Self->IncCounter(COUNTER_LOCKS_REMOVED);
        }

        ExpireQueue.Remove(txLock.Get());
        if (txLock->InBrokenLocks) {
            BrokenLocks.Remove(txLock.Get());
            --BrokenLocksCount_;
        }

        for (const TPathId& tableId : txLock->GetReadTables()) {
            Tables.at(tableId)->RemoveReadLock(txLock.Get());
        }
        for (const TPathId& tableId : txLock->GetWriteTables()) {
            Tables.at(tableId)->RemoveWriteLock(txLock.Get());
        }
        txLock->CleanupConflicts();
        Locks.erase(it);

        if (txLock->IsPersistent()) {
            Y_VERIFY(db, "Cannot remove persistent locks without a database");
            txLock->PersistRemoveLock(db);
        }

        txLock->OnRemoved();
    }
}

void TLockLocker::ForceBreakLock(ui64 lockId) {
    if (auto lock = GetLock(lockId, TRowVersion::Min())) {
        lock->SetBroken(TRowVersion::Min());
        RemoveBrokenRanges();
    }
}

void TLockLocker::RemoveLock(ui64 lockId, ILocksDb* db) {
    RemoveOneLock(lockId, db);
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
    BrokenLocks.Clear();
    CleanupPending.clear();
    CleanupCandidates.clear();
    PendingSubscribeLocks.clear();
}

bool TLockLocker::ForceShardLock(const TPathId& tableId) const {
    auto it = Tables.find(tableId);
    if (it != Tables.end()) {
        if (it->second->RangeCount() > LockLimit()) {
            return true;
        }
    }
    return false;
}

bool TLockLocker::ForceShardLock(const TIntrusiveList<TTableLocks, TTableLocksReadListTag>& readTables) const {
    for (auto& table : readTables) {
        if (table.RangeCount() > LockLimit())
            return true;
    }
    return false;
}

void TLockLocker::ScheduleBrokenLock(TLockInfo* lock) {
    auto it = Locks.find(lock->GetLockId());
    Y_VERIFY(it != Locks.end() && it->second.Get() == lock,
        "Sanity check: adding an unknown broken lock");
    if (lock->IsPersistent()) {
        BrokenPersistentLocks.PushBack(lock);
    } else if (!lock->InBrokenLocks) {
        BrokenLocks.PushBack(lock);
        ++BrokenLocksCount_;
        lock->InBrokenLocks = true;
    }
}

void TLockLocker::ScheduleRemoveBrokenRanges(ui64 lockId, const TRowVersion& at) {
    if (at) {
        CleanupCandidates.emplace(lockId, at);
    } else {
        CleanupPending.push_back(lockId);
    }

    if (Self->TabletCounters) {
        Self->IncCounter(COUNTER_LOCKS_BROKEN);
    }
}

void TLockLocker::RemoveSubscribedLock(ui64 lockId, ILocksDb* db) {
    auto it = Locks.find(lockId);
    if (it != Locks.end() && !it->second->IsFrozen()) {
        RemoveLock(lockId, db);
    }
}

void TLockLocker::SaveBrokenPersistentLocks(ILocksDb* db) {
    while (BrokenPersistentLocks) {
        TLockInfo* lock = BrokenPersistentLocks.PopFront();
        lock->PersistBrokenLock(db);
    }
}

// TLocksUpdate

TLocksUpdate::~TLocksUpdate() {
    auto cleanList = [](auto& list) {
        while (!list.Empty()) {
            list.PopFront();
        }
    };

    // We clean all lists to make sure items are not left linked together
    cleanList(ReadTables);
    cleanList(WriteTables);
    cleanList(AffectedTables);
    cleanList(BreakLocks);
    cleanList(BreakShardLocks);
    cleanList(BreakRangeLocks);
    cleanList(ReadConflictLocks);
    cleanList(WriteConflictLocks);
    cleanList(WriteConflictShardLocks);
    cleanList(EraseLocks);
}

// TSysLocks

TVector<TSysLocks::TLock> TSysLocks::ApplyLocks() {
    Y_VERIFY(Update);

    TMicrosecTimerCounter measureApplyLocks(*Self, COUNTER_APPLY_LOCKS_USEC);

    // Note: we don't use CheckVersion here, because ApplyLocks is all about
    //       setting locks, not validating them. If the lock is broken in any
    //       version, then extending it is pointless: validation would be at
    //       some point in the future, where it is broken already.
    TRowVersion breakVersion = Update->BreakVersion;

    // TODO: move this somewhere earlier, like the start of a new update guard
    Locker.RemoveBrokenRanges();

    Update->FlattenBreakLocks();
    if (Update->BreakLocks) {
        Locker.BreakLocks(Update->BreakLocks, breakVersion);
    }

    Locker.SaveBrokenPersistentLocks(Db);

    // Merge shard lock conflicts into write conflicts, we do this once as an optimization
    for (auto& table : Update->WriteConflictShardLocks) {
        for (auto* lock : table.ShardLocks) {
            if (lock->GetLockId() != Update->LockTxId) {
                Update->WriteConflictLocks.PushBack(lock);
            }
        }
    }

    size_t erases = 0;
    while (Update->EraseLocks) {
        Y_VERIFY(!Update->HasLocks(), "Can't erase and set locks in one Tx");
        auto* lock = Update->EraseLocks.PopFront();
        Locker.RemoveLock(lock->GetLockId(), Db);
        ++erases;
    }

    if (erases > 0 && Self->TabletCounters) {
        Self->IncCounter(COUNTER_LOCKS_ERASED, erases);
    }

    if (!Update->HasLocks()) {
        // Adding read/write conflicts implies locking
        Y_VERIFY(!Update->ReadConflictLocks);
        Y_VERIFY(!Update->WriteConflictLocks);
        return TVector<TLock>();
    }

    bool shardLock = Locker.ForceShardLock(Update->ReadTables);

    TLockInfo::TPtr lock;
    ui64 counter = TLock::ErrorNotSet;

    if (Update->BreakOwn) {
        counter = TLock::ErrorAlreadyBroken;
        Locker.ForceBreakLock(Update->LockTxId);
        Locker.SaveBrokenPersistentLocks(Db);
    } else {
        if (Update->Lock) {
            lock = std::move(Update->Lock);
        } else {
            lock = Locker.GetOrAddLock(Update->LockTxId, Update->LockNodeId);
        }
        if (!lock) {
            counter = TLock::ErrorTooMuch;
        } else if (lock->IsBroken()) {
            counter = TLock::ErrorBroken;
        } else {
            if (shardLock) {
                Locker.AddShardLock(lock, Update->ReadTables);
                if (Self->TabletCounters) {
                    Self->IncCounter(COUNTER_LOCKS_WHOLE_SHARD);
                }
            } else {
                for (const auto& key : Update->PointLocks) {
                    Locker.AddPointLock(lock, key);
                }
                for (const auto& key : Update->RangeLocks) {
                    Locker.AddRangeLock(lock, key);
                }
            }
            if (Update->WriteTables) {
                Locker.AddWriteLock(lock, Update->WriteTables);
            }
            counter = lock->GetCounter();
            Update->Lock = lock;

            if (lock->IsPersistent()) {
                lock->PersistRanges(Db);
            }
            for (auto& readConflictLock : Update->ReadConflictLocks) {
                readConflictLock.AddConflict(lock.Get(), Db);
            }
            for (auto& writeConflictLock : Update->WriteConflictLocks) {
                lock->AddConflict(&writeConflictLock, Db);
            }

            if (lock->GetWriteTables() && !lock->IsPersistent()) {
                // We need to persist a new lock
                lock->PersistLock(Db);
                // Persistent locks cannot expire
                Locker.ExpireQueue.Remove(lock.Get());
            }
        }
    }

    UpdateCounters(counter);

    // We have to tell client that there were some locks (even if we don't set them)
    TVector<TLock> out;
    for (auto& table : Update->AffectedTables) {
        out.emplace_back(MakeLock(Update->LockTxId, lock ? lock->GetGeneration() : Self->Generation(), counter,
            table.GetTableId(), Update->Lock && Update->Lock->IsWriteLock()));
    }
    return out;
}

void TSysLocks::UpdateCounters() {
    if (!Self->TabletCounters)
        return;

    Self->IncCounter(COUNTER_LOCKS_ACTIVE_PER_SHARD, LocksCount());
    Self->IncCounter(COUNTER_LOCKS_BROKEN_PER_SHARD, BrokenLocksCount());
}

void TSysLocks::UpdateCounters(ui64 counter) {
    if (!Self->TabletCounters)
        return;

    UpdateCounters();

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
        LOG_TRACE_S(LockLoggerContext, NKikimrServices::TX_DATASHARD, "TSysLocks::GetLock: lock " << lockTxId << " not found in cache");
        return TLock();
    }

    Y_VERIFY(Update);

    auto &checkVersion = Update->CheckVersion;
    TLockInfo::TPtr txLock = Locker.GetLock(lockTxId, checkVersion);
    if (txLock) {
        if (key.size() == 2) { // locks v1
            const auto& tableIds = txLock->GetReadTables();
            Y_VERIFY(tableIds.size() == 1);
            return MakeAndLogLock(lockTxId, txLock->GetGeneration(), txLock->GetCounter(checkVersion), *tableIds.begin(), txLock->IsWriteLock());
        } else { // locks v2
            Y_VERIFY(key.size() == 4);
            TPathId tableId;
            ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::SchemeShard, tableId.OwnerId);
            ok = ok && TLocksTable::ExtractKey(key, TLocksTable::EColumns::PathId, tableId.LocalPathId);
            if (ok && tableId) {
                if (txLock->GetReadTables().contains(tableId) || txLock->GetWriteTables().contains(tableId)) {
                    return MakeAndLogLock(lockTxId, txLock->GetGeneration(), txLock->GetCounter(checkVersion), tableId, txLock->IsWriteLock());
                } else {
                    LOG_TRACE_S(LockLoggerContext, NKikimrServices::TX_DATASHARD,
                            "TSysLocks::GetLock: lock " << lockTxId << " exists, but not set for table " << tableId);
                }
            } else {
                LOG_TRACE_S(LockLoggerContext, NKikimrServices::TX_DATASHARD,
                        "TSysLocks::GetLock: bad request for lock " << lockTxId);
            }
        }
    } else {
        LOG_TRACE_S(LockLoggerContext, NKikimrServices::TX_DATASHARD, "TSysLocks::GetLock: lock " << lockTxId << " not found");
    }

    Self->IncCounter(COUNTER_LOCKS_LOST);
    return TLock();
}

void TSysLocks::EraseLock(ui64 lockId) {
    Y_VERIFY(Update);
    if (auto* lock = Locker.FindLockPtr(lockId)) {
        Update->AddEraseLock(lock);
    }
}

void TSysLocks::EraseLock(const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update);
    if (auto* lock = Locker.FindLockPtr(GetLockId(key))) {
        Update->AddEraseLock(lock);
    }
}

void TSysLocks::CommitLock(const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update);
    if (auto* lock = Locker.FindLockPtr(GetLockId(key))) {
        for (auto& pr : lock->ConflictLocks) {
            if (!!(pr.second & ELockConflictFlags::BreakThemOnOurCommit)) {
                Update->AddBreakLock(pr.first);
            }
        }
        Update->AddEraseLock(lock);
    }
}

void TSysLocks::SetLock(const TTableId& tableId, const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update && Update->LockTxId);
    Y_VERIFY(!TSysTables::IsSystemTable(tableId));
    if (!Self->IsUserTable(tableId))
        return;

    Update->AddPointLock(Locker.MakePoint(tableId, key));
}

void TSysLocks::SetLock(const TTableId& tableId, const TTableRange& range) {
    if (range.Point) { // if range is point replace it with a point lock
        SetLock(tableId, range.From);
        return;
    }

    Y_VERIFY(Update && Update->LockTxId);
    Y_VERIFY(!TSysTables::IsSystemTable(tableId));
    if (!Self->IsUserTable(tableId))
        return;

    Update->AddRangeLock(Locker.MakeRange(tableId, range));
}

void TSysLocks::SetWriteLock(const TTableId& tableId, const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update && Update->LockTxId);
    Y_VERIFY(!TSysTables::IsSystemTable(tableId));
    if (!Self->IsUserTable(tableId))
        return;

    if (auto* table = Locker.FindTablePtr(tableId)) {
        Update->AddWriteLock(table);
        AddWriteConflict(tableId, key);
    }
}

void TSysLocks::BreakLock(ui64 lockId) {
    if (auto* lock = Locker.FindLockPtr(lockId)) {
        Update->AddBreakLock(lock);
    }
}

void TSysLocks::BreakLocks(const TTableId& tableId, const TArrayRef<const TCell>& key) {
    Y_VERIFY(!tableId.HasSamePath(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks)));

    if (auto* table = Locker.FindTablePtr(tableId)) {
        if (table->HasRangeLocks()) {
            // Note: avoid copying the key, find all locks here
            table->Ranges.EachIntersection(key, [update = Update](const TRangeTreeBase::TRange&, TLockInfo* lock) {
                update->AddBreakLock(lock);
            });
        }
        if (table->HasShardLocks()) {
            // We also want to break all shard locks in this table
            Update->AddBreakShardLocks(table);
        }
    }
}

void TSysLocks::AddReadConflict(ui64 conflictId) {
    Y_VERIFY(Update && Update->LockTxId);

    if (conflictId != Update->LockTxId) {
        if (auto* lock = Locker.FindLockPtr(conflictId)) {
            Update->AddReadConflictLock(lock);
        }
    }
}

void TSysLocks::AddWriteConflict(ui64 conflictId) {
    Y_VERIFY(Update && Update->LockTxId);

    if (conflictId != Update->LockTxId) {
        if (auto* lock = Locker.FindLockPtr(conflictId)) {
            Update->AddWriteConflictLock(lock);
        }
    }
}

void TSysLocks::AddWriteConflict(const TTableId& tableId, const TArrayRef<const TCell>& key) {
    Y_VERIFY(Update && Update->LockTxId);

    if (auto* table = Locker.FindTablePtr(tableId)) {
        if (table->HasRangeLocks()) {
            // Note: avoid copying the key, find all locks here
            table->Ranges.EachIntersection(key, [update = Update](const TRangeTreeBase::TRange&, TLockInfo* lock) {
                if (lock->GetLockId() != update->LockTxId) {
                    update->AddWriteConflictLock(lock);
                }
            });
        }
        if (table->HasShardLocks()) {
            // We also want to conflict with all shard locks in this table
            Update->AddWriteConflictShardLocks(table);
        }
    }
}

void TSysLocks::BreakAllLocks(const TTableId& tableId) {
    Y_VERIFY(Update);
    Y_VERIFY(!tableId.HasSamePath(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks)));
    if (!Self->IsUserTable(tableId))
        return;

    if (auto* table = Locker.FindTablePtr(tableId)) {
        if (table->HasShardLocks()) {
            Update->AddBreakShardLocks(table);
        }
        if (table->HasRangeLocks()) {
            Update->AddBreakRangeLocks(table);
        }
    }
}

void TSysLocks::BreakSetLocks() {
    Y_VERIFY(Update && Update->LockTxId);

    Update->BreakSetLocks();
}

bool TSysLocks::IsMyKey(const TArrayRef<const TCell>& key) const {
    ui64 tabletId;
    bool ok = TLocksTable::ExtractKey(key, TLocksTable::EColumns::DataShard, tabletId);
    return ok && (Self->TabletID() == tabletId);
}

bool TSysLocks::HasCurrentWriteLock(const TTableId& tableId) const {
    Y_VERIFY(Update && Update->LockTxId);

    if (Update->WriteTables) {
        if (auto* table = Locker.FindTablePtr(tableId.PathId)) {
            if (table->IsInList<TTableLocksWriteListTag>()) {
                return true;
            }
        }
    }

    if (auto* lock = Locker.FindLockPtr(Update->LockTxId)) {
        if (lock->WriteTables.contains(tableId.PathId)) {
            return true;
        }
    }

    return false;
}

bool TSysLocks::HasCurrentWriteLocks() const {
    Y_VERIFY(Update && Update->LockTxId);

    if (Update->WriteTables) {
        return true;
    }

    if (auto* lock = Locker.FindLockPtr(Update->LockTxId)) {
        if (lock->IsWriteLock()) {
            return true;
        }
    }

    return false;
}

bool TSysLocks::HasWriteLocks(const TTableId& tableId) const {
    if (Update && Update->WriteTables) {
        return true;
    }

    if (auto* table = Locker.FindTablePtr(tableId.PathId)) {
        if (!table->WriteLocks.empty()) {
            return true;
        }
    }

    return false;
}

EEnsureCurrentLock TSysLocks::EnsureCurrentLock() {
    Y_VERIFY(Update && Update->LockTxId);
    Y_VERIFY(Db, "EnsureCurrentLock needs a valid locks database");

    if (auto* lock = Locker.FindLockPtr(Update->LockTxId)) {
        // We cannot expand a broken lock
        if (lock->IsBroken()) {
            return EEnsureCurrentLock::Broken;
        }

        Update->Lock = lock;

        return EEnsureCurrentLock::Success;
    }

    if (!Db->MayAddLock(Update->LockTxId)) {
        return EEnsureCurrentLock::Abort;
    }

    Update->Lock = Locker.GetOrAddLock(Update->LockTxId, Update->LockNodeId);
    if (!Update->Lock) {
        return EEnsureCurrentLock::TooMany;
    }

    return EEnsureCurrentLock::Success;
}

TSysLocks::TLock TSysLocks::MakeLock(ui64 lockTxId, ui32 generation, ui64 counter, const TPathId& pathId, bool hasWrites) const {
    TLock lock;
    lock.LockId = lockTxId;
    lock.DataShard = Self->TabletID();
    lock.Generation = generation;
    lock.Counter = counter;
    lock.SchemeShard = pathId.OwnerId;
    lock.PathId = pathId.LocalPathId;
    lock.HasWrites = hasWrites;
    return lock;
}

TSysLocks::TLock TSysLocks::MakeAndLogLock(ui64 lockTxId, ui32 generation, ui64 counter, const TPathId& pathId, bool hasWrites) const {
    TLock lock = MakeLock(lockTxId, generation, counter, pathId, hasWrites);
    if (AccessLog)
        AccessLog->Locks[lockTxId] = lock;
    return lock;
}

bool TSysLocks::Load(ILocksDb& db) {
    TVector<ILocksDb::TLockRow> rows;
    if (!db.Load(rows)) {
        return false;
    }

    Locker.Clear();

    for (auto& lockRow : rows) {
        TLockInfo::TPtr lock = Locker.AddLock(lockRow.LockId, lockRow.LockNodeId, lockRow.Generation, lockRow.Counter, TInstant::MicroSeconds(lockRow.CreateTs));
        for (auto& rangeRow : lockRow.Ranges) {
            lock->RestorePersistentRange(rangeRow.RangeId, rangeRow.TableId, ELockRangeFlags(rangeRow.Flags));
        }
    }

    for (auto& lockRow : rows) {
        auto* lock = Locker.FindLockPtr(lockRow.LockId);
        Y_VERIFY(lock);
        for (ui64 conflictId : lockRow.Conflicts) {
            if (auto* otherLock = Locker.FindLockPtr(conflictId)) {
                lock->RestorePersistentConflict(otherLock);
            }
        }
    }

    return true;
}


}}
