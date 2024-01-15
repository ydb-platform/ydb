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

    virtual void UpdateRow(
            const TTableId& tableId,
            NTable::TRawVals key,
            TArrayRef<const NIceDb::TUpdateOp> ops) = 0;

};

class TDataShardUserDb final
    : public IDataShardUserDb
    , public IDataShardChangeGroupProvider
{
public:
    TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, const TRowVersion& readVersion, const TRowVersion& writeVersion)
        : Self(self)
        , Db(db)
        , LockTxId(0)
        , LockNodeId(0)
        , ReadVersion(readVersion)
        , WriteVersion(writeVersion)
    {
    }

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

    void UpdateRow(
            const TTableId& tableId,
            NTable::TRawVals key,
            TArrayRef<const NIceDb::TUpdateOp> ops) override;

    void AddCommitTxId(ui64 txId, const TRowVersion& commitVersion);

    absl::flat_hash_set<ui64>& GetVolatileReadDependencies() {
        return VolatileReadDependencies;
    }

    const NMiniKQL::IEngineFlat::TValidationInfo& GetTxInfo() const;
    void SetIsImmediateTx();
    void SetLockTxId(ui64 lockTxId, ui32 lockNodeId);
    ui64 GetWriteTxId(const TTableId& tableId);
    void SetVolatileTxId(ui64 txId);
    void SetWriteVersion(TRowVersion writeVersion);
    void SetReadVersion(TRowVersion readVersion);
    void MarkTxLoaded();
    
    std::optional<ui64> GetCurrentChangeGroup() const override;
    ui64 GetChangeGroup() override;
    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId);

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();
    

private:
    class TReadTxObserver;
    NTable::ITransactionMapPtr& GetReadTxMap();
    NTable::ITransactionObserverPtr& GetReadTxObserver();
    void CheckReadDependency(ui64 txId);

private:
    TDataShard& Self;
    NTable::TDatabase& Db;
    NMiniKQL::IEngineFlat::TValidationInfo TxInfo;
    ui64 LockTxId;
    ui32 LockNodeId;
    ui64 VolatileTxId = 0;
    bool IsImmediateTx = false;
    TRowVersion ReadVersion;
    TRowVersion WriteVersion;
    absl::flat_hash_map<TPathId, THolder<IDataShardChangeCollector>> ChangeCollectors;
    NTable::ITransactionMapPtr TxMap;
    TIntrusivePtr<NTable::TDynamicTransactionMap> DynamicTxMap;
    NTable::ITransactionObserverPtr TxObserver;
    absl::flat_hash_set<ui64> VolatileReadDependencies;
    std::optional<ui64> ChangeGroup = std::nullopt;
};

} // namespace NKikimr::NDataShard
