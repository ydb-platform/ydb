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
            const TMaybe<TRowVersion>& snapshot = {}) = 0;

    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& snapshot = {}) = 0;

    virtual void UpsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops,
            const ui32 defaultFilledColumnCount) = 0;

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

    virtual void IncrementRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) = 0;
    
    virtual void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) = 0;

    virtual void CommitChanges(
            const TTableId& tableId,
            ui64 lockId) = 0;
};

class IDataShardConflictChecker {
protected:
    ~IDataShardConflictChecker() = default;
public:
    virtual void AddReadConflict(ui64 txId) = 0;
    virtual void CheckReadConflict(const TRowVersion& rowVersion) = 0;
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
            const TRowVersion& mvccVersion,
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
            const TMaybe<TRowVersion>& snapshot = {}) override;

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& snapshot = {}) override;

    void UpsertRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops,
            const ui32 defaultFilledColumnCount) override;

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
        
    void IncrementRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key,
            const TArrayRef<const NIceDb::TUpdateOp> ops) override;

    void EraseRow(
            const TTableId& tableId,
            const TArrayRef<const TRawTypeValue> key) override;

    void CommitChanges(
            const TTableId& tableId, 
            ui64 lockId) override;

//IDataShardChangeGroupProvider
public:  
    std::optional<ui64> GetCurrentChangeGroup() const override;
    ui64 GetChangeGroup() override;

//IDataShardConflictChecker
public:  
    void AddReadConflict(ui64 txId) override;
    void CheckReadConflict(const TRowVersion& rowVersion) override;
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
    void AddCommitTxId(const TTableId& tableId, ui64 txId);
    ui64 GetWriteTxId(const TTableId& tableId);

    absl::flat_hash_set<ui64>& GetVolatileReadDependencies();
    TVector<ui64> GetVolatileCommitTxIds() const;

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId);
    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId);

    void ResetCounters();
    NMiniKQL::TEngineHostCounters& GetCounters();
    const NMiniKQL::TEngineHostCounters& GetCounters() const;

    bool PrechargeRow(
        const TTableId& tableId,
        const TArrayRef<const TRawTypeValue> key);    

private:
    static TSmallVec<TCell> ConvertTableKeys(const TArrayRef<const TRawTypeValue> key);

    void UpsertRowInt(NTable::ERowOp rowOp, const TTableId& tableId, ui64 localTableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);
    bool RowExists(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key);
    NTable::TRowState GetRowState(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TStackVec<NTable::TTag>& columns);

    void IncreaseUpdateCounters(const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops);

    TArrayRef<const NIceDb::TUpdateOp> RemoveDefaultColumnsIfNeeded(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops, const ui32 defaultFilledColumnCount);

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

    YDB_ACCESSOR_DEF(TRowVersion, MvccVersion);

    YDB_READONLY_DEF(TInstant, Now);

    YDB_READONLY_DEF(absl::flat_hash_set<ui64>, VolatileReadDependencies);
    absl::flat_hash_set<ui64> CommittedLockChanges;
    absl::flat_hash_map<TPathId, TIntrusivePtr<NTable::TDynamicTransactionMap>> TxMaps;
    absl::flat_hash_map<TPathId, NTable::ITransactionObserverPtr> TxObservers;

    absl::flat_hash_set<ui64> VolatileCommitTxIds;
    YDB_ACCESSOR_DEF(absl::flat_hash_set<ui64>, VolatileDependencies);
    YDB_ACCESSOR_DEF(bool, VolatileCommitOrdered);

    YDB_ACCESSOR_DEF(bool, PerformedUserReads);

    // Becomes true when user-visible reads detect changes over MvccVersion, i.e.
    // if we would have performed this read under a lock, it would have been broken.
    YDB_READONLY(bool, MvccReadConflict, false);

    // At commit time we have MvccVersion equal to the commit version, however
    // when transaction has a snapshot it should behave as if all reads are
    // performed at the snapshot version. This snapshot version is not used
    // for reads (we optimistically read from commit version at commit time to
    // minimize conflicts), however encountering errors which prevent the
    // transaction from committing having conflicts with the snapshot indicate
    // it should behave as if an imaginary lock was broken instread.
    YDB_ACCESSOR(TRowVersion, SnapshotVersion, TRowVersion::Max());
    // Becomes true when reads detect there have been committed changes between
    // the snapshot version and the commit version.
    YDB_READONLY(bool, SnapshotReadConflict, false);
    // Becomes true when writes detect there have been committed changes between
    // the snapshot version and the commit version.
    YDB_READONLY(bool, SnapshotWriteConflict, false);

    NMiniKQL::TEngineHostCounters& Counters;
};

} // namespace NKikimr::NDataShard
