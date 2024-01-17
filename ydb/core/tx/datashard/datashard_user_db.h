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
        const TArrayRef<const TRawTypeValue>& key,
        const TArrayRef<const NIceDb::TUpdateOp>& ops
    ) = 0;
};

class TDataShardUserDb final
    : public IDataShardUserDb
    , public IDataShardChangeGroupProvider
{
public:
    TDataShardUserDb(
        TDataShard& self,
        NTable::TDatabase& db,
        const TStepOrder& stepTxId,
        const TRowVersion& readVersion,
        const TRowVersion& writeVersion,
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
        const TArrayRef<const TRawTypeValue>& key,
        const TArrayRef<const NIceDb::TUpdateOp>& ops) override;

    std::optional<ui64> GetCurrentChangeGroup() const override;
    ui64 GetChangeGroup() override;

    ui64 GetTableSchemaVersion(const TTableId& tableId) const;
    void AddCommitTxId(const TTableId& tableId, ui64 txId, const TRowVersion& commitVersion);
    ui64 GetWriteTxId(const TTableId& tableId);

    bool HasRemovedTx(ui32 table, ui64 txId) const;

    bool IsValidKey(TKeyDesc& key) const;
    std::tuple<NMiniKQL::IEngineFlat::EResult, TString> ValidateKeys() const;
    bool IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const;
    bool IsPathErased(const TTableId& tableId) const;
private:
    static TSmallVec<TCell> ConvertTableKeys(const TArrayRef<const TRawTypeValue>& key);

    void UpdateRowInt(
        const TTableId& tableId,
        ui64 localTableId,
        const TArrayRef<const TRawTypeValue>& key,
        const TArrayRef<const NIceDb::TUpdateOp>& ops
    );

public:    
    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId);

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion);
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();

    TVector<ui64> GetVolatileCommitTxIds() const;
private:
    absl::flat_hash_map<TPathId, THolder<IDataShardChangeCollector>> ChangeCollectors;
    YDB_READONLY_DEF(std::optional<ui64>, ChangeGroup);

public:
    void AddReadConflict(ui64 txId) const;
    void CheckReadConflict(const TRowVersion& rowVersion) const;
    void CheckReadDependency(ui64 txId);
    bool NeedToReadBeforeWrite(const TTableId& tableId);
    void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> keyCells);
    void AddWriteConflict(ui64 txId) const;
    void BreakWriteConflict(ui64 txId);

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId);
    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId);
private:
    class TReadTxObserver;


    absl::flat_hash_set<ui64> CommittedLockChanges;
    absl::flat_hash_map<TPathId, TIntrusivePtr<NTable::TDynamicTransactionMap>> TxMaps;
    absl::flat_hash_map<TPathId, NTable::ITransactionObserverPtr> TxObservers;
    YDB_ACCESSOR_DEF(absl::flat_hash_set<ui64>, VolatileReadDependencies);
    absl::flat_hash_set<ui64> VolatileCommitTxIds;
    YDB_ACCESSOR_DEF(absl::flat_hash_set<ui64>, VolatileDependencies);
    YDB_ACCESSOR_DEF(bool, VolatileCommitOrdered);

public:
    NMiniKQL::IEngineFlat::TValidationInfo& GetTxInfo();
    const NMiniKQL::IEngineFlat::TValidationInfo& GetTxInfo() const;
    ui64 GetStep() const;
    ui64 GetTxId() const;
    void SetWriteVersion(TRowVersion writeVersion);
    TRowVersion GetWriteVersion() const;
    void SetReadVersion(TRowVersion readVersion);
    TRowVersion GetReadVersion() const;

private:
    TDataShard& Self;
    NTable::TDatabase& Db;

    TDataShardChangeGroupProvider ChangeGroupProvider;
    NMiniKQL::IEngineFlat::TValidationInfo TxInfo;
    YDB_READONLY(TStepOrder, StepTxId, TStepOrder(0, 0));
    YDB_ACCESSOR_DEF(ui64, LockTxId);
    YDB_ACCESSOR_DEF(ui32, LockNodeId);
    YDB_ACCESSOR_DEF(ui64, VolatileTxId);
    YDB_ACCESSOR_DEF(bool, IsImmediateTx);
    YDB_ACCESSOR_DEF(bool, IsRepeatableSnapshot);

    TRowVersion ReadVersion;
    TRowVersion WriteVersion;

    YDB_READONLY_DEF(TInstant, Now);
};

} // namespace NKikimr::NDataShard
