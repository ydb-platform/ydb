#include "datashard_user_db.h"

#include "datashard_impl.h"
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/tx/data_events/payload_helper.h>

namespace NKikimr::NDataShard {

TDataShardUserDb::TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, ui64 globalTxId, const TRowVersion& mvccVersion, NMiniKQL::TEngineHostCounters& counters, TInstant now)
    : Self(self)
    , Db(db)
    , ChangeGroupProvider(self, db)
    , GlobalTxId(globalTxId)
    , LockTxId(0)
    , LockNodeId(0)
    , MvccVersion(mvccVersion)
    , Now(now)
    , Counters(counters)
{
}

void TDataShardUserDb::EnsureVolatileTxId() {
    if (!VolatileTxId) {
        if (GlobalTxId == 0) {
            throw TNeedGlobalTxId();
        }
        SetVolatileTxId(GlobalTxId);
    }
}

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        NTable::TSelectStats& stats,
        const TMaybe<TRowVersion>& snapshot)
{
    auto tid = Self.GetLocalTableId(tableId);
    Y_ENSURE(tid != 0, "Unexpected SelectRow for an unknown table");

    if (snapshot) {
        // Note: snapshot is used by change collector to check scan snapshot state
        // We don't want to apply any tx map or observer to reproduce whatever scan observes
        return Db.Select(tid, key, tags, row, stats, /* readFlags */ 0, *snapshot);
    }

    SetPerformedUserReads(true);

    auto version = MvccVersion;
    if (LockMode == ELockMode::OptimisticSnapshotIsolation && SnapshotVersion < version) {
        // We want to keep using snapshot version at commit time in SnapshotRW isolation
        version = SnapshotVersion;
    }

    NTable::EReady ready = Db.Select(tid, key, tags, row, stats, /* readFlags */ 0,
        version,
        GetReadTxMap(tableId),
        GetReadTxObserver(tableId));

    if (LockMode != ELockMode::OptimisticSnapshotIsolation && stats.InvisibleRowSkips > 0) {
        if (LockTxId) {
            Self.SysLocksTable().BreakSetLocks();
        }
        MvccReadConflict = true;
    }

    return ready;
}

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        const TMaybe<TRowVersion>& snapshot)
{
    NTable::TSelectStats stats;
    return SelectRow(tableId, key, tags, row, stats, snapshot);
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

TArrayRef<const NIceDb::TUpdateOp> TDataShardUserDb::RemoveDefaultColumnsIfNeeded(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops,
    const ui32 DefaultFilledColumnCount
)
{
    if (DefaultFilledColumnCount == 0 || !RowExists(tableId, key)) {
        // no default columns - no changes need
        // row not exist - no changes need
        return ops;
    }

    //newOps is ops without last DefaultFilledColumnCount values
    auto newOps = TArrayRef<const NIceDb::TUpdateOp> (ops.begin(), ops.end() - DefaultFilledColumnCount);

    return newOps;
}


void TDataShardUserDb::UpsertRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops,
    const ui32 DefaultFilledColumnCount
)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    auto opsWithoutNoNeedDefault = RemoveDefaultColumnsIfNeeded(tableId, key, ops, DefaultFilledColumnCount);

    UpsertRow(tableId, key, opsWithoutNoNeedDefault);
}

void TDataShardUserDb::UpsertRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops
)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected UpdateRow for an unknown table");

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
            const NScheme::TTypeId vtype = scheme.GetColumnInfo(tableInfo, columnTag)->PType.GetTypeId();
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
    Y_ENSURE(localTableId != 0, "Unexpected ReplaceRow for an unknown table");

    UpsertRowInt(NTable::ERowOp::Reset, tableId, localTableId, key, ops);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::InsertRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected InsertRow for an unknown table");

    if (RowExists(tableId, key)) {
        // Compatibility with old stats.
        // We count read only if row exists.
        IncreaseSelectCounters(key);
        throw TUniqueConstrainException();
    }

    UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::UpdateRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    if (!RowExists(tableId, key)) {
        if (LockTxId && LockMode != ELockMode::OptimisticSnapshotIsolation) {
            // We don't perform an update, but this key may be modified later
            // by a different transaction. Make sure we set the read lock to
            // guard against that.
            TSmallVec<TCell> keyCells = ConvertTableKeys(key);
            Self.SysLocksTable().SetLock(tableId, keyCells);
        }
        // Compatibility with old stats.
        // We count read only if row exists.
        return;
    }

    UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);

    IncreaseSelectCounters(key);
    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::IncrementRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TArrayRef<const NIceDb::TUpdateOp> ops,
    bool insertMissing)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected incrementRow for an unknown table");

    TStackVec<NTable::TTag> columns(ops.size());
    for (size_t i = 0; i < ops.size(); i++) {
        columns[i] = ops[i].Tag;
    }

    auto currentRow = GetRowState(tableId, key, columns);
    IncreaseSelectCounters(key);

    if (currentRow.Size() == 0) {
        if (insertMissing) {
            UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, ops);
            IncreaseUpdateCounters(key, ops);
        }
        return;
    }

    TStackVec<NIceDb::TUpdateOp> newOps(ops.size());

    Y_ENSURE(currentRow.Size() == ops.size());
    const NTable::TScheme& scheme = Db.GetScheme();
    const NTable::TScheme::TTableInfo* tableInfo = scheme.GetTableInfo(localTableId);

    TStackVec<TCell> incrementResults(ops.size());

    for (size_t i = 0; i < ops.size(); i++) {
        auto vtype = scheme.GetColumnInfo(tableInfo, ops[i].Tag)->PType.GetTypeId();

        auto current = currentRow.Get(i);
        auto delta = ops[i].AsCell();

        NFormats::AddTwoCells(incrementResults[i], current, delta, vtype);

        TRawTypeValue rawTypeValue(incrementResults[i].Data(), incrementResults[i].Size(), vtype);
        newOps[i] = NIceDb::TUpdateOp(ops[i].Tag, ops[i].Op, rawTypeValue);
    }

    UpsertRowInt(NTable::ERowOp::Upsert, tableId, localTableId, key, newOps);

    IncreaseUpdateCounters(key, ops);
}

void TDataShardUserDb::EraseRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    if (LockMode == ELockMode::OptimisticSnapshotIsolation) {
        if (!RowExists(tableId, key)) {
            // Don't perform write for keys which don't exist, SnapshotRW
            // transaction may break otherwise even when not actually
            // performing operations from the user's viewpoint
            return;
        }
    }

    UpsertRowInt(NTable::ERowOp::Erase, tableId, localTableId, key, {});

    ui64 keyBytes = CalculateKeyBytes(key);

    Counters.NEraseRow++;
    Counters.EraseRowBytes += keyBytes + 8;
}

bool TDataShardUserDb::PrechargeRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected PrechargeRow for an unknown table");

    return Db.Precharge(localTableId, key, key, {}, 0, Max<ui64>(), Max<ui64>()).Ready;
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

void TDataShardUserDb::IncreaseSelectCounters(
    const TArrayRef<const TRawTypeValue> key) 
{
    ui64 keyBytes = CalculateKeyBytes(key);

    Counters.NSelectRow++;
    Counters.SelectRowRows++;
    Counters.SelectRowBytes += keyBytes;
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
        if (collector && !collector->OnUpdate(tableId, localTableId, rowOp, key, ops, MvccVersion))
            throw TNotReadyTabletException();

        Db.Update(localTableId, rowOp, key, ops, MvccVersion);
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

bool TDataShardUserDb::RowExists(
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

NTable::TRowState TDataShardUserDb::GetRowState(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue> key,
    const TStackVec<NTable::TTag>& columns)
{
    NTable::TRowState rowState;

    const auto ready = SelectRow(tableId, key, columns, rowState);
    switch (ready) {
        case NTable::EReady::Page: {
            throw TNotReadyTabletException();
        }
        case NTable::EReady::Data: {
            return std::move(rowState);
        }
        case NTable::EReady::Gone: {
            return NTable::TRowState();
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

void TDataShardUserDb::CommitChanges(const TTableId& tableId, ui64 lockId) {
    auto localTid = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTid, "Unexpected failure to find table " << tableId << " in datashard " << Self.TabletID());

    if (!Db.HasOpenTx(localTid, lockId)) {
        if (Db.HasRemovedTx(localTid, lockId)) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Committing removed changes lockId# " << lockId << " tid# " << localTid << " shard# " << Self.TabletID());
            Self.IncCounter(COUNTER_REMOVED_COMMITTED_TXS);
        }
        return;
    }

    if (auto lock = Self.SysLocksTable().GetRawLock(lockId); lock && !VolatileCommitOrdered) {
        lock->ForAllVolatileDependencies([this](ui64 txId) {
            auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId);
            if (info && info->State != EVolatileTxState::Aborting) {
                if (VolatileDependencies.insert(txId).second) {
                    EnsureVolatileTxId();
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
                it->second->Add(lockId, MvccVersion);
            }
        }
        // Note for AI agents: we don't add lockId to CommittedTxIds because
        // it's not committed in the storage yet.
        return;
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Committing changes lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self.TabletID());
    Db.CommitTx(localTid, lockId, MvccVersion);
    Self.GetConflictsCache().GetTableCache(localTid).RemoveUncommittedWrites(lockId, Db);
    CommittedTxIds.insert(lockId);

    if (!CommittedLockChanges.contains(lockId) && Self.HasLockChangeRecords(lockId)) {
        if (auto* collector = GetChangeCollector(tableId)) {
            collector->CommitLockChanges(lockId, MvccVersion);
            CommittedLockChanges.insert(lockId);
        }
    }
}

void TDataShardUserDb::AddCommitTxId(const TTableId& tableId, ui64 txId) {
    auto* dynamicTxMap = static_cast<NTable::TDynamicTransactionMap*>(GetReadTxMap(tableId).Get());
    dynamicTxMap->Add(txId, MvccVersion);
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
        // We don't handle changes below our current (volatile) tx id, because
        // we should have checked them already. This handles cases when the
        // same key is written to multiple times.
        if (!SelfFound) {
            SelfFound = ConflictChecker.BreakWriteConflict(txId);
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
    bool SelfFound = false;
};

void TDataShardUserDb::CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected CheckWriteConflicts for an unknown table");

    // When there are uncommitted changes (write locks) we must find which
    // locks would break upon commit. For snapshot isolation transactions we
    // must also find all conflicts which could prevent us from committing.
    bool mustFindConflicts = (
        LockMode == ELockMode::OptimisticSnapshotIsolation ||
        Self.SysLocksTable().HasWriteLocks(tableId));

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
    } else {
        if (LockMode == ELockMode::OptimisticSnapshotIsolation) {
            // We switch to volatile code path to make sure all changes by the
            // current commit will not be marked as committed in the storage
            // until all operations are applied. This is more expensive, but
            // allows us to distinguish between changes written in the current
            // commit and changes committed by other transactions at the same
            // row version.
            EnsureVolatileTxId();
        }

        // TODO: conflicts cache doesn't track the last committed row version
        // for cached keys, so it cannot be used to speed up conflict checks
        // in snapshot isolation. This cache is only needed to avoid retries
        // due to page faults and does not affect correctness, but we may want
        // to track additional info in the future.
        if (LockMode != ELockMode::OptimisticSnapshotIsolation) {
            if (auto* cached = Self.GetConflictsCache().GetTableCache(localTableId).FindUncommittedWrites(keyCells)) {
                for (ui64 txId : *cached) {
                    BreakWriteConflict(txId);
                }
                return;
            }
        }

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
            EnsureVolatileTxId();
            VolatileCommitOrdered = true;
            VolatileDependencies.clear();
        }
        return;
    }

    if (LockMode == ELockMode::OptimisticSnapshotIsolation) {
        if (res.RowVersion > SnapshotVersion) {
            // Current row version is above the snapshot, which could indicate
            // a conflicting write. However, this transaction may have already
            // marked some of its TxIds as committed, and we don't want to
            // report serialization conflict in that case.
            // Note for AI agents: we don't check VolatileCommitTxIds because
            // those changes are not committed in the storage yet, and this
            // check is for changes which have already been marked as committed.
            if (!res.RowTxId || !CommittedTxIds.contains(res.RowTxId)) {
                // This change has no chance of ever committing
                throw TSerializableIsolationException();
            }
        }
    }

    if (LockTxId || VolatileTxId) {
        ui64 skipLimit = Self.GetMaxLockedWritesPerKey();
        if (skipLimit > 0 && skipCount >= skipLimit) {
            throw TLockedWriteLimitException();
        }
    }

    if (res.LockTxId != 0) {
        if (LockTxId) {
            AddWriteConflict(res.LockTxId);
        } else {
            BreakWriteConflict(res.LockTxId);
        }
    }
}

void TDataShardUserDb::AddWriteConflict(ui64 txId) {
    // Note for AI agents: this code path is only called when writing new
    // uncommitted changes, which may or may not be committed in the distant
    // future at an unknown version. We handle conflicts with changes from
    // other transactions here, which are not yet marked as committed in the
    // storage, but may or may not be logically committed.
    if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        if (info->State != EVolatileTxState::Aborting &&
            LockMode == ELockMode::OptimisticSnapshotIsolation &&
            info->Version > SnapshotVersion)
        {
            if (info->State == EVolatileTxState::Waiting) {
                // We need to resolve this dependency first (it may abort)
                VolatileReadDependencies.insert(info->TxId);
            }
            // This write will not be able to commit
            throw TSerializableIsolationException();
        }
        if (info->State != EVolatileTxState::Aborting) {
            Self.SysLocksTable().AddVolatileDependency(info->TxId);
        }
    } else {
        Self.SysLocksTable().AddWriteConflict(txId);
        if (LockMode == ELockMode::OptimisticSnapshotIsolation) {
            Self.SysLocksTable().AddReadConflict(txId);
        }
    }
}

bool TDataShardUserDb::BreakWriteConflict(ui64 txId) {
    // Note for AI agents: this code path is only called when writing new
    // changes at commit time. We handle conflicts with changes from other
    // transactions here (txId != 0), which are not yet marked as committed in
    // the storage, but may or may not be logically committed. Although we
    // perform this check at commit time, the commit may be optimistic with
    // VolatileTxId != 0 and may or may not rollback later.
    //
    // When writing uncommitted changes at commit time we add those TxIds to
    // VolatileCommitTxIds, which is a set of changes which are currently
    // uncommitted, but will be committed later when transaction eventually
    // commits in storage.
    //
    // We don't check CommittedTxIds because those changes are already marked
    // as committed in the storage, and will never end up in this callback.
    // Transaction may have some of its changes in CommittedTxIds and others
    // in VolatileCommitTxIds, because for some changes it may be safe to
    // commit immediately, but for other changes we may need to wait until
    // some earlier transaction commits or aborts.
    if (VolatileCommitTxIds.contains(txId)) {
        // Skip our own commits
        return true;
    }

    if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        if (info->State != EVolatileTxState::Aborting &&
            LockMode == ELockMode::OptimisticSnapshotIsolation &&
            info->Version > SnapshotVersion)
        {
            if (info->State == EVolatileTxState::Waiting) {
                // We need to resolve this dependency first (it may abort)
                VolatileReadDependencies.insert(info->TxId);
            }
            // This write will not be able to commit
            throw TSerializableIsolationException();
        }
        // We must not overwrite uncommitted changes that may become committed
        // later, so we need to add a dependency that will force us to wait
        // until it is persistently committed. We may ignore aborting changes
        // even though they may not be persistent yet, since this tx will
        // also perform writes, and either it fails, or future generation
        // could not have possibly committed it already.
        if (info->State != EVolatileTxState::Aborting && !VolatileCommitOrdered) {
            // All further writes will use this VolatileTxId and will
            // add it to VolatileCommitTxIds, forcing it to be committed
            // like a volatile transaction. Note that this does not make
            // it into a real volatile transaction, it works as usual in
            // every sense, only persistent commit order is affected by
            // a dependency below.
            EnsureVolatileTxId();
            VolatileDependencies.insert(info->TxId);
        }
    } else {
        // Break uncommitted locks from other transactions
        Self.SysLocksTable().BreakLock(txId);
    }

    return false;
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
    Y_ENSURE(localTableId != 0, "Unexpected GetWriteTxId for an unknown table");

    if (VolatileTxId) {
        Y_ENSURE(!LockTxId);
        if (VolatileCommitTxIds.insert(VolatileTxId).second) {
            // Update TxMap to include the new commit
            auto it = TxMaps.find(tableId.PathId);
            if (it != TxMaps.end()) {
                it->second->Add(VolatileTxId, MvccVersion);
            }
        }
        // Note for AI agents: we don't add VolatileTxId to CommittedTxIds
        // because it's not committed in the storage yet.
        return VolatileTxId;
    }

    return LockTxId;
}

NTable::ITransactionMapPtr TDataShardUserDb::GetReadTxMap(const TTableId& tableId) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected GetReadTxMap for an unknown table");

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
                txMap->Add(commitTxId, MvccVersion);
            }
        }
    }

    return txMap;
}

NTable::ITransactionObserverPtr TDataShardUserDb::GetReadTxObserver(const TTableId& tableId) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ENSURE(localTableId != 0, "Unexpected GetReadTxObserver for an unknown table");

    bool needObserver = (
        SnapshotVersion < MvccVersion ||
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

void TDataShardUserDb::AddReadConflict(ui64 txId) {
    Y_ENSURE(LockTxId);

    // We have detected uncommitted changes in txId that could affect
    // our read result. We arrange a conflict that breaks our lock
    // when txId commits.
    Self.SysLocksTable().AddReadConflict(txId);
}

void TDataShardUserDb::CheckReadConflict(const TRowVersion& rowVersion) {
    if (rowVersion > MvccVersion) {
        // We are reading from snapshot at MvccVersion and should not normally
        // observe changes with a version above that. However, if we have an
        // uncommitted change, that we fake as committed for our own changes
        // visibility, we might shadow some change that happened after a
        // snapshot. This is a clear indication of a conflict between read
        // and that future conflict, hence we must break locks and abort.
        if (LockTxId) {
            Self.SysLocksTable().BreakSetLocks();
        }
        MvccReadConflict = true;
    } else if (rowVersion > SnapshotVersion) {
        // During commit we read at the current mvcc version, however we may
        // notice there have been changes between the snapshot and current
        // commit version. This is not necessarily an error, but indicates
        // if this read was performed under a lock it would have been broken.
        SnapshotReadConflict = true;
    }
}

bool TDataShardUserDb::NeedToReadBeforeWrite(const TTableId& tableId) {
    if (LockMode == ELockMode::OptimisticSnapshotIsolation) {
        return true;
    }

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
