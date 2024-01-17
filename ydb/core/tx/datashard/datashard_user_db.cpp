#include "datashard_user_db.h"

namespace NKikimr::NDataShard {

TDataShardUserDb::TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, const TStepOrder& stepTxId, const TRowVersion& readVersion, const TRowVersion& writeVersion, TInstant now)
    : Self(self)
    , Db(db)
    , ChangeGroupProvider(self, db)
    , StepTxId(stepTxId)
    , LockTxId(0)
    , LockNodeId(0)
    , ReadVersion(readVersion)
    , WriteVersion(writeVersion)
    , Now(now)
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
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected SelectRow for an unknown table");

    return Db.Select(localTableId, key, tags, row, stats, /* readFlags */ 0,
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

void TDataShardUserDb::UpdateRow(
    const TTableId& tableId,
    const TArrayRef<const TRawTypeValue>& key,
    const TArrayRef<const NIceDb::TUpdateOp>& ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    TSmallVec<TCell> keyCells = ConvertTableKeys(key);

    CheckWriteConflicts(tableId, keyCells);

    if (LockTxId) {
        Self.SysLocksTable().SetWriteLock(tableId, keyCells);
    } else {
        Self.SysLocksTable().BreakLocks(tableId, keyCells);
    }
    Self.SetTableUpdateTime(tableId, Now);


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

        UpdateRowInt(tableId, localTableId, key, extendedOps);
    } else {
        UpdateRowInt(tableId, localTableId, key, ops);
    }

    if (VolatileTxId) {
        Self.GetConflictsCache().GetTableCache(localTableId).AddUncommittedWrite(keyCells, VolatileTxId, Db);
    } else if (LockTxId) {
        Self.GetConflictsCache().GetTableCache(localTableId).AddUncommittedWrite(keyCells, LockTxId, Db);
    } else {
        Self.GetConflictsCache().GetTableCache(localTableId).RemoveUncommittedWrites(keyCells, Db);
    }
}

void TDataShardUserDb::UpdateRowInt(
    const TTableId& tableId,
    ui64 localTableId,
    const TArrayRef<const TRawTypeValue>& key,
    const TArrayRef<const NIceDb::TUpdateOp>& ops)
{
    auto* collector = GetChangeCollector(tableId);
    
    if (LockTxId == 0) {
        if (collector && !collector->OnUpdate(tableId, localTableId, NTable::ERowOp::Upsert, key, ops, WriteVersion))
            throw TNotReadyTabletException();

        Db.Update(localTableId, NTable::ERowOp::Upsert, key, ops, WriteVersion);
    } else {
        if (collector && !collector->OnUpdateTx(tableId, localTableId, NTable::ERowOp::Upsert, key, ops, LockTxId))
            throw TNotReadyTabletException();

        Db.UpdateTx(localTableId, NTable::ERowOp::Upsert, key, ops, LockTxId);
    }
}

TSmallVec<TCell> TDataShardUserDb::ConvertTableKeys(const TArrayRef<const TRawTypeValue>& key)
{
    TSmallVec<TCell> keyCells;
    keyCells.reserve(key.size());
    std::transform(key.begin(), key.end(), std::back_inserter(keyCells), [](const TRawTypeValue& x) { return TCell(&x); });
    return keyCells;
}

void TDataShardUserDb::AddCommitTxId(const TTableId& tableId, ui64 txId, const TRowVersion& commitVersion) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected AddCommitTxId for an unknown table");

    auto baseTxMap = Self.GetVolatileTxManager().GetTxMap();
    auto& txMap = TxMaps[tableId.PathId];
    if (!txMap)
        txMap = new NTable::TDynamicTransactionMap(baseTxMap);
    txMap->Add(txId, commitVersion);
}

ui64 TDataShardUserDb::GetTableSchemaVersion(const TTableId& tableId) const {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected GetTableSchemaVersion for an unknown table");

    const auto& userTables = Self.GetUserTables();
    auto it = userTables.find(tableId.PathId.LocalPathId);
    if (it == userTables.end()) {
        Y_FAIL_S("DatshardEngineHost (tablet id: " << Self.TabletID() << " state: " << Self.GetState() << ") unables to find given table with id: " << tableId);
        return 0;
    } else {
        return it->second->GetTableSchemaVersion();
    }
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

bool TDataShardUserDb::IsValidKey(TKeyDesc& key) const {
    ui64 localTableId = Self.GetLocalTableId(key.TableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected IsValidKey for an unknown table");

    if (GetLockTxId()) {
        // Prevent updates/erases with LockTxId set, unless it's allowed for immediate mvcc txs
        if (key.RowOperation != TKeyDesc::ERowOperation::Read &&
            (!Self.GetEnableLockedWrites() || !IsImmediateTx || !IsRepeatableSnapshot || !LockNodeId))
        {
            key.Status = TKeyDesc::EStatus::OperationNotSupported;
            return false;
        }
    } else if (IsRepeatableSnapshot) {
        // Prevent updates/erases in repeatable mvcc txs
        if (key.RowOperation != TKeyDesc::ERowOperation::Read) {
            key.Status = TKeyDesc::EStatus::OperationNotSupported;
            return false;
        }
    }

    return Db.GetScheme().IsValidKey(localTableId, key);
}

// Returns whether row belong this shard.
bool TDataShardUserDb::IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const {
    ui64 localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected IsMyKey for an unknown table");

    auto iter = Self.FindUserTable(tableId.PathId);
    if (!iter) {
        // TODO: can this happen?
        Y_VERIFY_DEBUG(false);
        return false;
    }

    // Check row against range
    const TUserTable& info = *iter;
    return (ComparePointAndRange(row, info.GetTableRange(), info.KeyColumnTypes, info.KeyColumnTypes) == 0);
}
bool TDataShardUserDb::IsPathErased(const TTableId& tableId) const {
    ui64 localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected IsPathErased for an unknown table");

    return Self.GetLocalTableId(tableId) == 0;
}

bool TDataShardUserDb::HasRemovedTx(ui32 table, ui64 txId) const {
    return Db.HasRemovedTx(table, txId);
}

NMiniKQL::IEngineFlat::TValidationInfo& TDataShardUserDb::GetTxInfo() {
    return TxInfo;
}

const NMiniKQL::IEngineFlat::TValidationInfo& TDataShardUserDb::GetTxInfo() const {
    return TxInfo;
}

ui64 TDataShardUserDb::GetStep() const {
    return StepTxId.Step;
}
ui64 TDataShardUserDb::GetTxId() const {
    return StepTxId.TxId;
}

void TDataShardUserDb::SetWriteVersion(TRowVersion writeVersion) {
    WriteVersion = writeVersion;
}

TRowVersion TDataShardUserDb::GetWriteVersion() const {
    Y_ABORT_UNLESS(!WriteVersion.IsMax(), "Cannot perform writes without WriteVersion set");
    return WriteVersion;
}

void TDataShardUserDb::SetReadVersion(TRowVersion readVersion) {
    ReadVersion = readVersion;
}

TRowVersion TDataShardUserDb::GetReadVersion() const {
    Y_ABORT_UNLESS(!ReadVersion.IsMin(), "Cannot perform reads without ReadVersion set");
    return ReadVersion;
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

void TDataShardUserDb::CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) {
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected CommitChanges for an unknown table");

    if (!Db.HasOpenTx(localTableId, lockId)) {
        return;
    }

    if (auto lock = Self.SysLocksTable().GetRawLock(lockId, TRowVersion::Min()); lock && !VolatileCommitOrdered) {
        lock->ForAllVolatileDependencies([this](ui64 txId) {
            auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId);
            if (info && info->State != EVolatileTxState::Aborting) {
                if (VolatileDependencies.insert(txId).second && !VolatileTxId) {
                    VolatileTxId = GetTxId();
                }
            }
        });
    }

    if (VolatileTxId) {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Scheduling commit of lockId# " << lockId << " in localTableId# " << localTableId << " shard# " << Self.TabletID());
        if (VolatileCommitTxIds.insert(lockId).second) {
            // Update TxMap to include the new commit
            auto it = TxMaps.find(tableId.PathId);
            if (it != TxMaps.end()) {
                it->second->Add(lockId, WriteVersion);
            }
        }
        return;
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Committing changes lockId# " << lockId << " in localTableId# " << localTableId << " shard# " << Self.TabletID());
    Db.CommitTx(localTableId, lockId, writeVersion);
    Self.GetConflictsCache().GetTableCache(localTableId).RemoveUncommittedWrites(lockId, Db);

    if (!CommittedLockChanges.contains(lockId) && Self.HasLockChangeRecords(lockId)) {
        if (auto* collector = GetChangeCollector(tableId)) {
            collector->CommitLockChanges(lockId, WriteVersion);
            CommittedLockChanges.insert(lockId);
        }
    }
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

std::optional<ui64> TDataShardUserDb::GetCurrentChangeGroup() const {
    return ChangeGroupProvider.GetCurrentChangeGroup();
}

ui64 TDataShardUserDb::GetChangeGroup() {
    // Distributed transactions have their group set to zero
    if (!IsImmediateTx)
        return 0;

    return ChangeGroupProvider.GetChangeGroup();
}

class TLockedReadTxObserver: public NTable::ITransactionObserver {
public:
    TLockedReadTxObserver(TDataShardUserDb& userDb)
        : UserDb(userDb)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        UserDb.AddReadConflict(txId);
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // We already use InvisibleRowSkips for these
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // We already use InvisibleRowSkips for these
    }

    void OnApplyCommitted(const TRowVersion& rowVersion) override {
        UserDb.CheckReadConflict(rowVersion);
    }

    void OnApplyCommitted(const TRowVersion& rowVersion, ui64 txId) override {
        UserDb.CheckReadConflict(rowVersion);
        UserDb.CheckReadDependency(txId);
    }

private:
    TDataShardUserDb& UserDb;
};

class TDataShardUserDb::TReadTxObserver : public NTable::ITransactionObserver {
public:
    TReadTxObserver(TDataShardUserDb& userDb)
        : UserDb(userDb)
    { }

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
        UserDb.CheckReadDependency(txId);
    }

private:
    TDataShardUserDb& UserDb;
};

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

class TLockedWriteTxObserver: public NTable::ITransactionObserver {
public:
    TLockedWriteTxObserver(TDataShardUserDb& userDb, ui64 txId, ui64& skipCount, ui32 localTableId)
        : UserDb(userDb)
        , SelfTxId(txId)
        , SkipCount(skipCount)
        , LocalTid(localTableId)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        // Note: all active volatile transactions will be uncommitted
        // without a tx map, and will be handled by AddWriteConflict.
        if (!UserDb.HasRemovedTx(LocalTid, txId)) {
            ++SkipCount;
            if (!SelfFound) {
                if (txId != SelfTxId) {
                    UserDb.AddWriteConflict(txId);
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
    TDataShardUserDb& UserDb;
    const ui64 SelfTxId;
    ui64& SkipCount;
    const ui32 LocalTid;
    bool SelfFound = false;
};

class TWriteTxObserver: public NTable::ITransactionObserver {
public:
    TWriteTxObserver(TDataShardUserDb& userDb)
        : UserDb(userDb)
    {
    }

    void OnSkipUncommitted(ui64 txId) override {
        // Note: all active volatile transactions will be uncommitted
        // without a tx map, and will be handled by BreakWriteConflict.
        UserDb.BreakWriteConflict(txId);
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
    TDataShardUserDb& UserDb;
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
        txObserver = new TLockedWriteTxObserver(*this, LockTxId, skipCount, localTableId);
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
                VolatileTxId = GetTxId();
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

void TDataShardUserDb::AddWriteConflict(ui64 txId) const {
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
                VolatileTxId = GetTxId();
            }
            VolatileDependencies.insert(info->TxId);
        }
    } else {
        // Break uncommitted locks
        Self.SysLocksTable().BreakLock(txId);
    }
}

} // namespace NKikimr::NDataShard
