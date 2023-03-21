#pragma once
#include "datashard_impl.h"

#include <util/generic/maybe.h>

namespace NKikimr::NDataShard {

class IDataShardUserDb {
protected:
    ~IDataShardUserDb() = default;

public:
    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            NTable::TSelectStats& stats,
            const TMaybe<TRowVersion>& readVersion = {}) = 0;

    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion = {}) = 0;
};

class TDataShardUserDb final : public IDataShardUserDb {
public:
    TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, const TRowVersion& readVersion)
        : Self(self)
        , Db(db)
        , ReadVersion(readVersion)
    { }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            NTable::TSelectStats& stats,
            const TMaybe<TRowVersion>& readVersion = {}) override;

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion = {}) override;

    void AddCommitTxId(ui64 txId, const TRowVersion& commitVersion);

    absl::flat_hash_set<ui64>& GetVolatileReadDependencies() {
        return VolatileReadDependencies;
    }

private:
    class TReadTxObserver;
    NTable::ITransactionMapPtr& GetReadTxMap();
    NTable::ITransactionObserverPtr& GetReadTxObserver();
    void CheckReadDependency(ui64 txId);

private:
    TDataShard& Self;
    NTable::TDatabase& Db;
    TRowVersion ReadVersion;
    NTable::ITransactionMapPtr TxMap;
    TIntrusivePtr<NTable::TDynamicTransactionMap> DynamicTxMap;
    NTable::ITransactionObserverPtr TxObserver;
    absl::flat_hash_set<ui64> VolatileReadDependencies;
};

} // namespace NKikimr::NDataShard
