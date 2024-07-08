#include "datashard_user_db.h"

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

TDataShardUserDb::TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, ui64 globalTxId, const TRowVersion& readVersion, const TRowVersion& writeVersion, NMiniKQL::TEngineHostCounters& counters, TInstant now)
    : Self(self)
    , Db(db)
    , ChangeGroupProvider(self, db)
    , GlobalTxId(globalTxId)
    , LockTxId(0)
    , LockNodeId(0)
    , ReadVersion(readVersion)
    , WriteVersion(writeVersion)
    , Now(now)
    , Counters(counters)
{
}

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        NTable::TSelectStats& stats,
        const TMaybe<TRowVersion>& readVersion)
{
    auto tid = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(tid != 0, "Unexpected SelectRow for an unknown table");

    SetPerformedUserReads(true);

    return Db.Select(tid, key, tags, row, stats, /* readFlags */ 0,
        readVersion.GetOrElse(ReadVersion),
        GetReadTxMap(tableId),
        GetReadTxObserver(tableId));
}

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        const TMaybe<TRowVersion>& readVersion)
{
    NTable::TSelectStats stats;
    return SelectRow(tableId, key, tags, row, stats, readVersion);
}

ui64 CalculateKeyBytes(const TArrayRef<const TRawTypeValue> key) {
    ui64 bytes = 0ull;
    for (const TRawTypeValue& value : key)
        bytes += value.IsEmpty() ? 1ull : value.Size();
    return bytes;
};

ui64 CalculateValueBytes(const TArrayRef<const NIceDb::TUpdateOp> ops) {
    ui64 bytes = 0ull;
    for (const NIceDb::TUpdateOp& op : ops)
        bytes += op.Value.IsEmpty() ? 1ull : op.Value.Size();
    return bytes;
};

void TDataShardUserDb::UpsertRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    // apply special columns if declared
    TUserTable::TSpecialUpdate specUpdates = Self.SpecialUpdates(Db, tableId);
    if (specUpdates.HasUpdates) {
        const NTable::TScheme& scheme = Db.GetScheme();
        const NTable::TScheme::TTableInfo* tableInfo = scheme.GetTableInfo(localTableId);

        TStackVec<NIceDb::TUpdateOp> extendedOps;
        extendedOps.reserve(ops.size() + 3);
        for (const NIceDb::TUpdateOp& op : ops) {
            if (op.Tag == specUpdates.ColIdTablet)
                specUpdates.ColIdTablet = Max<ui32>();
            else if (op.Tag == specUpdates.ColIdEpoch)
                specUpdates.ColIdEpoch = Max<ui32>();
            else if (op.Tag == specUpdates.ColIdUpdateNo)
                specUpdates.ColIdUpdateNo = Max<ui32>();

            extendedOps.push_back(op);
        }

        auto addExtendedOp = [&scheme, &tableInfo, &extendedOps](const ui64 columnTag, const ui64& columnValue) {
            const NScheme::TTypeInfo vtype = scheme.GetColumnInfo(tableInfo, columnTag)->PType;
            const char* ptr = static_cast<const char*>(static_cast<const void*>(&columnValue));
            TRawTypeValue rawTypeValue(ptr, sizeof(ui64), vtype);
            NIceDb::TUpdateOp extOp(columnTag, NTable::ECellOp::Set, rawTypeValue);
            extendedOps.emplace_back(extOp);
        };

        if (specUpdates.ColIdTablet != Max<ui32>()) {
            addExtendedOp(specUpdates.ColIdTablet, specUpdates.Tablet);
        }

        if (specUpdates.ColIdEpoch != Max<ui32>()) {
            addExtendedOp(specUpdates.ColIdEpoch, specUpdates.Epoch);
        }

        if (specUpdates.ColIdUpdateNo != Max<ui32>()) {
            addExtendedOp(specUpdates.ColIdUpdateNo, specUpdates.UpdateNo);
        }
        UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, extendedOps);

        IncreaseUpdateCounters(key, extendedOps);
    } else {
        UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);

        IncreaseUpdateCounters(key, ops);
    }
}

void TDataShardUserDb::ReplaceRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected ReplaceRow for an unknown table");

    UpsertRowInt(NTable::ERowOp::Reset, tableId, localTableId, key, ops);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::InsertRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected InsertRow for an unknown table");

    if (RowExists(tableId, key))
        throw TUniqueConstrainException();

    UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::UpdateRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    if (!RowExists(tableId, key))
        return;

    UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::EraseRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    UpsertRowInt(NTable::ERowOp::Erase, tableId, localTableId, key, {});

    ui64 keyBytes = CalculateKeyBytes(key);
    
    Counters.NEraseRow++;
    Counters.EraseRowBytes += keyBytes + 8;
}

void TDataShardUserDb::IncreaseUpdateCounters(
    const TArrayRef<const TRawTypeValue> key, 
    const TArrayRef<const NIceDb::TUpdateOp> ops) 
{
    ui64 valueBytes = CalculateValueBytes(ops);
    ui64 keyBytes = CalculateKeyBytes(key);

    Counters.NUpdateRow++;
    Counters.UpdateRowBytes += keyBytes + valueBytes;
}

void TDataShardUserDb::UpsertRowInt(
    NTable::ERowOp rowOp,
    const TTableId& tableId,
    ui64 localTableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops) 
{
    TSmallVec<TCell> keyCells = ConvertTableKeys(key);

    CheckWriteConflicts(tableId, keyCells);

    if (LockTxId) {
        Self.SysLocksTable().SetWriteLock(tableId, keyCells);
    } else {
        Self.SysLocksTable().BreakLocks(tableId, keyCells);
    }
    Self.SetTableUpdateTime(tableId, Now);

    auto* collector = GetChangeCollector(tableId);

    const ui64 writeTxId = GetWriteTxId(tableId);
    if (writeTxId == 0) {
        if (collector && !collector->OnUpdate(tableId, localTableId, rowOp, key, ops, WriteVersion))
            throw TNotReadyTabletException();

        Db.Update(localTableId, rowOp, key, ops, WriteVersion);
    } else {
        if (collector && !collector->OnUpdateTx(tableId, localTableId, rowOp, key, ops, writeTxId))
            throw TNotReadyTabletException();

        Db.UpdateTx(localTableId, rowOp, key, ops, writeTxId);
    }

    if (VolatileTxId) {
        Self.GetConflictsCache().GetTableCache(localTableId).AddUncommittedWrite(keyCells, VolatileTxId, Db);
    } else if (LockTxId) {
        Self.GetConflictsCache().GetTableCache(localTableId).AddUncommittedWrite(keyCells, LockTxId, Db);
    } else {
        Self.GetConflictsCache().GetTableCache(localTableId).RemoveUncommittedWrites(keyCells, Db);
    }

    Self.GetKeyAccessSampler()->AddSample(tableId, keyCells);
}

bool TDataShardUserDb::RowExists (
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key) 
{
    NTable::TRowState rowState;
    const auto ready = SelectRow(tableId, key, {}, rowState);
    switch (ready) {
        case NTable::EReady::Page: {
            throw TNotReadyTabletException();
        }
        case NTable::EReady::Data: {
            return true;
        }
        case NTable::EReady::Gone: {
            return false;
        }
    }
}

TSmallVec<TCell> TDataShardUserDb::ConvertTableKeys(const TArrayRef<const TRawTypeValue> key)
{
    TSmallVec<TCell> keyCells;
    keyCells.reserve(key.size());
    std::transform(key.begin(), key.end(), std::back_inserter(keyCells), [](const TRawTypeValue& x) { return TCell(&x); });
    return keyCells;
}

IDataShardChangeCollector* TDataShardUserDb::GetChangeCollector(const TTableId& tableId) {
    auto it = ChangeCollectors.find(tableId.PathId);
    if (it != ChangeCollectors.end()) {
        return it->second.Get();
    }

    it = ChangeCollectors.emplace(tableId.PathId, nullptr).first;
    if (!Self.IsUserTable(tableId)) {
        return it->second.Get();
    }

    it->second.Reset(CreateChangeCollector(
        Self,
        *const_cast<TDataShardUserDb*>(this),
        *const_cast<TDataShardUserDb*>(this),
        Db,
        tableId.PathId.LocalPathId
    ));
    return it->second.Get();
}

TVector<IDataShardChangeCollector::TChange> TDataShardUserDb::GetCollectedChanges() const {
    TVector<IDataShardChangeCollector::TChange> total;

    for (auto& [_, collector] : ChangeCollectors) {
        if (!collector) {
            continue;
        }

        auto collected = std::move(collector->GetCollected());
        std::move(collected.begin(), collected.end(), std::back_inserter(total));
    }

    return total;
}

void TDataShardUserDb::ResetCollectedChanges() {
    for (auto& pr : ChangeCollectors) {
        if (pr.second) {
            pr.second->OnRestart();
        }
    }
}

std::optional<ui64> TDataShardUserDb::GetCurrentChangeGroup() const {
    return ChangeGroupProvider.GetCurrentChangeGroup();
}

ui64 TDataShardUserDb::GetChangeGroup() {
    // Distributed transactions have their group set to zero
    if (!IsImmediateTx)
        return 0;

    return ChangeGroupProvider.GetChangeGroup();
}

void TDataShardUserDb::CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) {
    auto localTid = Self.GetLocalTableId(tableId);
    Y_VERIFY_S(localTid, "Unexpected failure to find table " << tableId << " in datashard " << Self.TabletID());

    if (!Db.HasOpenTx(localTid, lockId)) {
        return;
    }

    if (auto lock = Self.SysLocksTable().GetRawLock(lockId, TRowVersion::Min()); lock && !VolatileCommitOrdered) {
        lock->ForAllVolatileDependencies([this](ui64 txId) {
            auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId);
            if (info && info->State != EVolatileTxState::Aborting) {
                if (VolatileDependencies.insert(txId).second && !VolatileTxId) {
                    SetVolatileTxId(GlobalTxId);
                }
            }
        });
    }

    if (VolatileTxId) {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Scheduling commit of lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self.TabletID());
        if (VolatileCommitTxIds.insert(lockId).second) {
            // Update TxMap to include the new commit
            auto it = TxMaps.find(tableId.PathId);
            if (it != TxMaps.end()) {
                it->second->Add(lockId, WriteVersion);
            }
        }
        return;
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Committing changes lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self.TabletID());
    Db.CommitTx(localTid, lockId, writeVersion);
    Self.GetConflictsCache().GetTableCache(localTid).RemoveUncommittedWrites(lockId, Db);

    if (!CommittedLockChanges.contains(lockId) && Self.HasLockChangeRecords(lockId)) {
        if (auto* collector = GetChangeCollector(tableId)) {
            collector->CommitLockChanges(lockId, WriteVersion);
            CommittedLockChanges.insert(lockId);
        }
    }
}

void TDataShardUserDb::AddCommitTxId(const TTableId& tableId, ui64 txId, const TRowVersion& commitVersion) {
    auto* dynamicTxMap = static_cast<NTable::TDynamicTransactionMap*>(GetReadTxMap(tableId).Get());
    dynamicTxMap->Add(txId, commitVersion);
}

class TLockedReadTxObserver: public NTable::ITransactionObserver {
public:
    TLockedReadTxObserver(IDataShardConflictChecker& conflictChecker)
        : ConflictChecker(conflictChecker)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        ConflictChecker.AddReadConflict(txId);
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // We already use InvisibleRowSkips for these
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // We already use InvisibleRowSkips for these
    }

    void OnApplyCommitted(const TRowVersion& rowVersion) override {
        ConflictChecker.CheckReadConflict(rowVersion);
    }

    void OnApplyCommitted(const TRowVersion& rowVersion, ui64 txId) override {
        ConflictChecker.CheckReadConflict(rowVersion);
        ConflictChecker.CheckReadDependency(txId);
    }

private:
    IDataShardConflictChecker& ConflictChecker;
};

class TReadTxObserver: public NTable::ITransactionObserver {
public:
    TReadTxObserver(IDataShardConflictChecker& conflictChecker)
        : ConflictChecker(conflictChecker)
    {
    }

    void OnSkipUncommitted(ui64) override {
        // We don't care about uncommitted changes
        // Any future commit is supposed to be above our read version
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // We already use InvisibleRowSkips for these
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // We already use InvisibleRowSkips for these
    }

    void OnApplyCommitted(const TRowVersion&) override {
        // Not needed
    }

    void OnApplyCommitted(const TRowVersion&, ui64 txId) override {
        ConflictChecker.CheckReadDependency(txId);
    }

private:
    IDataShardConflictChecker& ConflictChecker;
};

void TDataShardUserDb::CheckReadDependency(ui64 txId) {
    if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        switch (info->State) {
            case EVolatileTxState::Waiting:
                // We are reading undecided changes and need to wait until they are resolved
                VolatileReadDependencies.insert(info->TxId);
                break;
            case EVolatileTxState::Committed:
                // Committed changes are immediately visible and don't need a dependency
                break;
            case EVolatileTxState::Aborting:
                // We just read something that we know is aborting, we would have to retry later
                VolatileReadDependencies.insert(info->TxId);
                break;
        }
    }
}

class TLockedWriteTxObserver: public NTable::ITransactionObserver {
public:
    TLockedWriteTxObserver(IDataShardConflictChecker& conflictChecker, NTable::TDatabase& db, ui64 txId, ui64& skipCount, ui32 localTableId)
        : ConflictChecker(conflictChecker)
        , Db(db)
        , SelfTxId(txId)
        , SkipCount(skipCount)
        , LocalTid(localTableId)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        // Note: all active volatile transactions will be uncommitted
        // without a tx map, and will be handled by AddWriteConflict.
        if (!Db.HasRemovedTx(LocalTid, txId)) {
            ++SkipCount;
            if (!SelfFound) {
                if (txId != SelfTxId) {
                    ConflictChecker.AddWriteConflict(txId);
                } else {
                    SelfFound = true;
                }
            }
        }
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

private:
    IDataShardConflictChecker& ConflictChecker;
    NTable::TDatabase& Db;
    const ui64 SelfTxId;
    ui64& SkipCount;
    const ui32 LocalTid;
    bool SelfFound = false;
};

class TWriteTxObserver: public NTable::ITransactionObserver {
public:
    TWriteTxObserver(IDataShardConflictChecker& conflictChecker)
        : ConflictChecker(conflictChecker)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        // Note: all active volatile transactions will be uncommitted
        // without a tx map, and will be handled by BreakWriteConflict.
        ConflictChecker.BreakWriteConflict(txId);
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&) override {
        // nothing
    }

    void OnApplyCommitted(const TRowVersion&, ui64) override {
        // nothing
    }

private:
    IDataShardConflictChecker& ConflictChecker;
};

void TDataShardUserDb::CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected CheckWriteConflicts for an unknown table");

    // When there are uncommitted changes (write locks) we must find which
    // locks would break upon commit.
    bool mustFindConflicts = Self.SysLocksTable().HasWriteLocks(tableId);

    // When there are volatile changes (tx map) we try to find precise
    // dependencies, but we may switch to total order on page faults.
    const bool tryFindConflicts = mustFindConflicts ||
                                  (!VolatileCommitOrdered && Self.GetVolatileTxManager().GetTxMap());

    if (!tryFindConflicts) {
        // We don't need to find conflicts
        return;
    }

    ui64 skipCount = 0;

    NTable::ITransactionObserverPtr txObserver;
    if (LockTxId) {
        // We cannot use cached conflicts since we need to find skip count
        txObserver = new TLockedWriteTxObserver(*this, Db, LockTxId, skipCount, localTableId);
        // Locked writes are immediate, increased latency is not critical
        mustFindConflicts = true;
    } else if (auto* cached = Self.GetConflictsCache().GetTableCache(localTableId).FindUncommittedWrites(keyCells)) {
        for (ui64 txId : *cached) {
            BreakWriteConflict(txId);
        }
        return;
    } else {
        txObserver = new TWriteTxObserver(*this);
        // Prefer precise conflicts for non-distributed transactions
        if (IsImmediateTx) {
            mustFindConflicts = true;
        }
    }

    // We are not actually interested in the row version, we only need to
    // detect uncommitted transaction skips on the path to that version.
    auto res = Db.SelectRowVersion(
        localTableId, keyCells, /* readFlags */ 0,
        nullptr, txObserver
    );

    if (res.Ready == NTable::EReady::Page) {
        if (mustFindConflicts || LockTxId) {
            // We must gather all conflicts
            throw TNotReadyTabletException();
        }

        // Upgrade to volatile ordered commit and ignore the page fault
        if (!VolatileCommitOrdered) {
            if (!VolatileTxId) {
                SetVolatileTxId(GlobalTxId);
            }
            VolatileCommitOrdered = true;
            VolatileDependencies.clear();
        }
        return;
    }

    if (LockTxId || VolatileTxId) {
        ui64 skipLimit = Self.GetMaxLockedWritesPerKey();
        if (skipLimit > 0 && skipCount >= skipLimit) {
            throw TLockedWriteLimitException();
        }
    }
}

void TDataShardUserDb::AddWriteConflict(ui64 txId) {
    if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        if (info->State != EVolatileTxState::Aborting) {
            Self.SysLocksTable().AddVolatileDependency(info->TxId);
        }
    } else {
        Self.SysLocksTable().AddWriteConflict(txId);
    }
}

void TDataShardUserDb::BreakWriteConflict(ui64 txId) {
    if (VolatileCommitTxIds.contains(txId)) {
        // Skip our own commits
    } else if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        // We must not overwrite uncommitted changes that may become committed
        // later, so we need to add a dependency that will force us to wait
        // until it is persistently committed. We may ignore aborting changes
        // even though they may not be persistent yet, since this tx will
        // also perform writes, and either it fails, or future generation
        // could not have possibly committed it already.
        if (info->State != EVolatileTxState::Aborting && !VolatileCommitOrdered) {
            if (!VolatileTxId) {
                // All further writes will use this VolatileTxId and will
                // add it to VolatileCommitTxIds, forcing it to be committed
                // like a volatile transaction. Note that this does not make
                // it into a real volatile transaction, it works as usual in
                // every sense, only persistent commit order is affected by
                // a dependency below.
                SetVolatileTxId(GlobalTxId);
            }
            VolatileDependencies.insert(info->TxId);
        }
    } else {
        // Break uncommitted locks
        Self.SysLocksTable().BreakLock(txId);
    }
}

absl::flat_hash_set<ui64>& TDataShardUserDb::GetVolatileReadDependencies() {
    return VolatileReadDependencies;
}

TVector<ui64> TDataShardUserDb::GetVolatileCommitTxIds() const {
    TVector<ui64> commitTxIds;

    if (!VolatileCommitTxIds.empty()) {
        commitTxIds.reserve(VolatileCommitTxIds.size());
        for (ui64 commitTxId : VolatileCommitTxIds) {
            commitTxIds.push_back(commitTxId);
        }
    }

    return commitTxIds;
}

ui64 TDataShardUserDb::GetWriteTxId(const TTableId& tableId) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected GetWriteTxId for an unknown table");

    if (VolatileTxId) {
        Y_ABORT_UNLESS(!LockTxId);
        if (VolatileCommitTxIds.insert(VolatileTxId).second) {
            // Update TxMap to include the new commit
            auto it = TxMaps.find(tableId.PathId);
            if (it != TxMaps.end()) {
                it->second->Add(VolatileTxId, WriteVersion);
            }
        }
        return VolatileTxId;
    }

    return LockTxId;
}

NTable::ITransactionMapPtr TDataShardUserDb::GetReadTxMap(const TTableId& tableId) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected GetReadTxMap for an unknown table");

    auto baseTxMap = Self.GetVolatileTxManager().GetTxMap();

    bool needTxMap = (
        // We need tx map when there are waiting volatile transactions
        baseTxMap ||
        // We need tx map to see committed volatile tx changes
        VolatileTxId && !VolatileCommitTxIds.empty() ||
        // We need tx map when current lock has uncommitted changes
        LockTxId && Self.SysLocksTable().HasCurrentWriteLock(tableId)
    );

    if (!needTxMap) {
        // We don't need tx map
        return nullptr;
    }

    auto& txMap = TxMaps[tableId.PathId];
    if (!txMap) {
        txMap = new NTable::TDynamicTransactionMap(baseTxMap);
        if (LockTxId) {
            // Uncommitted changes are visible in all possible snapshots
            txMap->Add(LockTxId, TRowVersion::Min());
        } else if (VolatileTxId) {
            // We want committed volatile changes to be visible at the write version
            for (ui64 commitTxId : VolatileCommitTxIds) {
                txMap->Add(commitTxId, WriteVersion);
            }
        }
    }

    return txMap;
}



NTable::ITransactionObserverPtr TDataShardUserDb::GetReadTxObserver(const TTableId& tableId) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected GetReadTxObserver for an unknown table");

    bool needObserver = (
        // We need observer when there are waiting changes in the tx map
        Self.GetVolatileTxManager().GetTxMap() ||
        // We need observer for locked reads when there are active write locks
        LockTxId && Self.SysLocksTable().HasWriteLocks(tableId)
    );

    if (!needObserver) {
        // We don't need tx observer
        return nullptr;
    }

    auto& ptr = TxObservers[tableId.PathId];
    if (!ptr) {
        if (LockTxId) {
            ptr = new TLockedReadTxObserver(*this);
        } else {
            ptr = new TReadTxObserver(*this);
        }
    }

    return ptr;
}

void TDataShardUserDb::AddReadConflict(ui64 txId) const {
    Y_ABORT_UNLESS(LockTxId);

    // We have detected uncommitted changes in txId that could affect
    // our read result. We arrange a conflict that breaks our lock
    // when txId commits.
    Self.SysLocksTable().AddReadConflict(txId);
}

void TDataShardUserDb::CheckReadConflict(const TRowVersion& rowVersion) const {
    Y_ABORT_UNLESS(LockTxId);

    if (rowVersion > ReadVersion) {
        // We are reading from snapshot at ReadVersion and should not normally
        // observe changes with a version above that. However, if we have an
        // uncommitted change, that we fake as committed for our own changes
        // visibility, we might shadow some change that happened after a
        // snapshot. This is a clear indication of a conflict between read
        // and that future conflict, hence we must break locks and abort.
        Self.SysLocksTable().BreakSetLocks();
    }
}



bool TDataShardUserDb::NeedToReadBeforeWrite(const TTableId& tableId) {
    if (Self.GetVolatileTxManager().GetTxMap()) {
        return true;
    }

    if (Self.SysLocksTable().HasWriteLocks(tableId)) {
        return true;
    }

    if (auto* collector = GetChangeCollector(tableId)) {
        if (collector->NeedToReadKeys()) {
            return true;
        }
    }

    return false;
}

void TDataShardUserDb::ResetCounters() {
    Counters = {};
}

NMiniKQL::TEngineHostCounters& TDataShardUserDb::GetCounters() {
    return Counters;
}

const NMiniKQL::TEngineHostCounters& TDataShardUserDb::GetCounters() const {
    return Counters;
}

} // namespace NKikimr::NDataShard
