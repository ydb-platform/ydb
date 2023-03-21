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
    auto tid = Self.GetLocalTableId(tableId);
    Y_VERIFY(tid != 0, "Unexpected SelectRow for an unknown table");

    return Db.Select(tid, key, tags, row, stats, /* readFlags */ 0,
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
