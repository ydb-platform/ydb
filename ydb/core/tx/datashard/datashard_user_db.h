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
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;

    virtual void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) = 0;            
};

class TDataShardUserDb final
    : public IDataShardUserDb
    , public IDataShardChangeGroupProvider {
public:
    TDataShardUserDb(
            TDataShard& self,
            NTable::TDatabase& db,
            ui64 globalTxId,
            const TRowVersion& readVersion,
            const TRowVersion& writeVersion,
            NMiniKQL::TEngineHostCounters& counters, 
            TInstant now
    );

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
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;
    
    void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) override;
public:
    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId);
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();

private:
    absl::flat_hash_map<TPathId, THolder<IDataShardChangeCollector>> ChangeCollectors;

public:
    std::optional<ui64> GetCurrentChangeGroup() const override;
    ui64 GetChangeGroup() override;

public:
    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion);

    void AddCommitTxId(const TTableId& tableId, ui64 txId, const TRowVersion& commitVersion);
    ui64 GetWriteTxId(const TTableId& tableId);

    absl::flat_hash_set<ui64>& GetVolatileReadDependencies();
    TVector<ui64> GetVolatileCommitTxIds() const;

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId);
    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId);

    void AddReadConflict(ui64 txId) const;
    void CheckReadConflict(const TRowVersion& rowVersion) const;
    void CheckReadDependency(ui64 txId);
    bool NeedToReadBeforeWrite(const TTableId& tableId);

    void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells);
    void AddWriteConflict(ui64 txId) const;
    void BreakWriteConflict(ui64 txId);

    void ResetCounters();
    NMiniKQL::TEngineHostCounters& GetCounters();
    const NMiniKQL::TEngineHostCounters& GetCounters() const;

private:
    static TSmallVec<TCell> ConvertTableKeys(const TArrayRef<const TRawTypeValue> key);

    void UpdateRowInt(NTable::ERowOp rowOp, const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);
    void UpdateRowInDb(NTable::ERowOp rowOp, const TTableId& tableId, ui64 localTableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);

private:
    TDataShard& Self;
    NTable::TDatabase& Db;

    TDataShardChangeGroupProvider ChangeGroupProvider;

    YDB_READONLY_DEF(ui64, GlobalTxId);
    YDB_ACCESSOR_DEF(ui64, LockTxId);
    YDB_ACCESSOR_DEF(ui32, LockNodeId);
    YDB_ACCESSOR_DEF(ui64, VolatileTxId);
    YDB_ACCESSOR_DEF(bool, IsImmediateTx);
    YDB_ACCESSOR_DEF(bool, IsRepeatableSnapshot);

    YDB_ACCESSOR_DEF(TRowVersion, ReadVersion);
    YDB_ACCESSOR_DEF(TRowVersion, WriteVersion);

    YDB_READONLY_DEF(TInstant, Now);

    YDB_READONLY_DEF(absl::flat_hash_set<ui64>, VolatileReadDependencies);
    absl::flat_hash_set<ui64> CommittedLockChanges;
    absl::flat_hash_map<TPathId, TIntrusivePtr<NTable::TDynamicTransactionMap>> TxMaps;
    absl::flat_hash_map<TPathId, NTable::ITransactionObserverPtr> TxObservers;

    absl::flat_hash_set<ui64> VolatileCommitTxIds;
    YDB_ACCESSOR_DEF(absl::flat_hash_set<ui64>, VolatileDependencies);
    YDB_ACCESSOR_DEF(bool, VolatileCommitOrdered);

    NMiniKQL::TEngineHostCounters& Counters;
};

} // namespace NKikimr::NDataShard
