#pragma once

#include "change_collector.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/maybe.h>

namespace NKikimr::NMiniKQL {
    struct TEngineHostCounters;
}

namespace NKikimr::NDataShard {

class TUniqueConstrainException: public yexception {};

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

    virtual void UpsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;

    virtual void ReplaceRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;
    
    virtual void InsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;

    virtual void UpdateRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;

    virtual void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) = 0;

    virtual void CommitChanges(
            const TTableId& tableId,
            ui64 lockId,
            const TRowVersion& writeVersion) = 0;
};

class IDataShardConflictChecker {
protected:
    ~IDataShardConflictChecker() = default;
public:
    virtual void AddReadConflict(ui64 txId) const = 0;
    virtual void CheckReadConflict(const TRowVersion& rowVersion) const = 0;
    virtual void CheckReadDependency(ui64 txId) = 0;

    virtual void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells) = 0;
    virtual void AddWriteConflict(ui64 txId) = 0;
    virtual void BreakWriteConflict(ui64 txId) = 0;
};

class TDataShardUserDb final
    : public IDataShardUserDb
    , public IDataShardChangeGroupProvider
    , public IDataShardConflictChecker {
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

//IDataShardUserDb
public:  
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

    void UpsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;
    
    void ReplaceRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;
            
    void InsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;
            
    void UpdateRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;

    void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) override;

    void CommitChanges(
            const TTableId& tableId, 
            ui64 lockId, 
            const TRowVersion& writeVersion) override;

//IDataShardChangeGroupProvider
public:  
    std::optional<ui64> GetCurrentChangeGroup() const override;
    ui64 GetChangeGroup() override;

//IDataShardConflictChecker
public:  
    void AddReadConflict(ui64 txId) const override;
    void CheckReadConflict(const TRowVersion& rowVersion) const override;
    void CheckReadDependency(ui64 txId) override;

    void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells) override;
    void AddWriteConflict(ui64 txId) override;
    void BreakWriteConflict(ui64 txId) override;

public:
    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId);
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();

public:
    bool NeedToReadBeforeWrite(const TTableId& tableId);
    void AddCommitTxId(const TTableId& tableId, ui64 txId, const TRowVersion& commitVersion);
    ui64 GetWriteTxId(const TTableId& tableId);

    absl::flat_hash_set<ui64>& GetVolatileReadDependencies();
    TVector<ui64> GetVolatileCommitTxIds() const;

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId);
    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId);

    void ResetCounters();
    NMiniKQL::TEngineHostCounters& GetCounters();
    const NMiniKQL::TEngineHostCounters& GetCounters() const;

private:
    static TSmallVec<TCell> ConvertTableKeys(const TArrayRef<const TRawTypeValue> key);

    void UpsertRowInt(NTable::ERowOp rowOp, const TTableId& tableId, ui64 localTableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);
    bool RowExists(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key);

    void IncreaseUpdateCounters(const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);
private:
    TDataShard& Self;
    NTable::TDatabase& Db;

    TDataShardChangeGroupProvider ChangeGroupProvider;

    absl::flat_hash_map<TPathId, THolder<IDataShardChangeCollector>> ChangeCollectors;

    YDB_READONLY_DEF(ui64, GlobalTxId);
    YDB_ACCESSOR_DEF(ui64, LockTxId);
    YDB_ACCESSOR_DEF(ui32, LockNodeId);
    YDB_ACCESSOR_DEF(ui64, VolatileTxId);
    YDB_ACCESSOR_DEF(bool, IsImmediateTx);
    YDB_ACCESSOR_DEF(bool, IsWriteTx);
    YDB_ACCESSOR_DEF(bool, UsesMvccSnapshot);

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

    YDB_ACCESSOR_DEF(bool, PerformedUserReads);

    NMiniKQL::TEngineHostCounters& Counters;
};

} // namespace NKikimr::NDataShard
