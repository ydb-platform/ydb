#include "datashard_user_db.h"

namespace NKikimr::NDataShard {

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
        GetReadTxMap(),
        GetReadTxObserver());
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
    NTable::TRawVals key,
    TArrayRef<const NIceDb::TUpdateOp> ops)
{
    auto localTableId = Self.GetLocalTableId(tableId);
    Y_ABORT_UNLESS(localTableId != 0, "Unexpected UpdateRow for an unknown table");

    auto* collector = GetChangeCollector(tableId);

    const ui64 writeTxId = GetWriteTxId(tableId);
    if (writeTxId == 0) {
        if (collector && !collector->OnUpdate(tableId, localTableId, NTable::ERowOp::Upsert, key, ops, WriteVersion))
            throw TNotReadyTabletException();

        Db.Update(localTableId, NTable::ERowOp::Upsert, key, ops, WriteVersion);
    } else {
        if (collector && !collector->OnUpdateTx(tableId, localTableId, NTable::ERowOp::Upsert, key, ops, writeTxId))
            throw TNotReadyTabletException();

        Db.UpdateTx(localTableId, NTable::ERowOp::Upsert, key, ops, writeTxId);
    }
}

void TDataShardUserDb::AddCommitTxId(ui64 txId, const TRowVersion& commitVersion) {
    if (!DynamicTxMap) {
        DynamicTxMap = new NTable::TDynamicTransactionMap(Self.GetVolatileTxManager().GetTxMap());
        TxMap = DynamicTxMap;
    }
    DynamicTxMap->Add(txId, commitVersion);
}

NTable::ITransactionMapPtr& TDataShardUserDb::GetReadTxMap() {
    if (!TxMap) {
        auto baseTxMap = Self.GetVolatileTxManager().GetTxMap();
        if (baseTxMap) {
            DynamicTxMap = new NTable::TDynamicTransactionMap(baseTxMap);
            TxMap = DynamicTxMap;
        }
    }
    return TxMap;
}

const NMiniKQL::IEngineFlat::TValidationInfo& TDataShardUserDb::GetTxInfo() const {
    return TxInfo;
}

void TDataShardUserDb::SetIsImmediateTx() {
    IsImmediateTx = true;
}

void TDataShardUserDb::SetLockTxId(ui64 lockTxId, ui32 lockNodeId) {
    LockTxId = lockTxId;
    LockNodeId = lockNodeId;
}

ui64 TDataShardUserDb::GetWriteTxId(const TTableId& tableId) {
    if (TSysTables::IsSystemTable(tableId))
        return 0;

    return LockTxId;
}

void TDataShardUserDb::SetVolatileTxId(ui64 txId) {
    VolatileTxId = txId;
}

void TDataShardUserDb::SetWriteVersion(TRowVersion writeVersion) {
    WriteVersion = writeVersion;
};

void TDataShardUserDb::SetReadVersion(TRowVersion readVersion) {
    ReadVersion = readVersion;
};

void TDataShardUserDb::MarkTxLoaded() {
    TxInfo.Loaded = true;
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
    return ChangeGroup;
}

ui64 TDataShardUserDb::GetChangeGroup() {
    if (!ChangeGroup) {
        if (IsImmediateTx) {
            NIceDb::TNiceDb db(Db);
            ChangeGroup = Self.AllocateChangeRecordGroup(db);
        } else {
            // Distributed transactions have their group set to zero
            ChangeGroup = 0;
        }
    }

    return *ChangeGroup;
}

class TDataShardUserDb::TReadTxObserver : public NTable::ITransactionObserver {
public:
    TReadTxObserver(TDataShardUserDb& userDb)
        : UserDb(userDb)
    { }

    void OnSkipUncommitted(ui64) override {
        // nothing
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

    void OnApplyCommitted(const TRowVersion&, ui64 txId) override {
        UserDb.CheckReadDependency(txId);
    }

private:
    TDataShardUserDb& UserDb;
};

NTable::ITransactionObserverPtr& TDataShardUserDb::GetReadTxObserver() {
    if (!TxObserver) {
        auto baseTxMap = Self.GetVolatileTxManager().GetTxMap();
        if (baseTxMap) {
            TxObserver = new TReadTxObserver(*this);
        }
    }
    return TxObserver;
}

void TDataShardUserDb::CheckReadDependency(ui64 txId) {
    if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
        switch (info->State) {
            case EVolatileTxState::Waiting:
                VolatileReadDependencies.insert(info->TxId);
                break;
            case EVolatileTxState::Committed:
                break;
            case EVolatileTxState::Aborting:
                VolatileReadDependencies.insert(info->TxId);
                break;
        }
    }
}

} // namespace NKikimr::NDataShard
