#include "change_collector.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"
#include "datashard__engine_host.h"
#include "sys_tables.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <library/cpp/actors/core/log.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTabletFlatExecutor;

namespace {

NUdf::TUnboxedValue CreateRow(const TVector<TCell>& inRow,
                              const TVector<NScheme::TTypeInfo>& inType,
                              const THolderFactory& holderFactory) {
    NUdf::TUnboxedValue* rowItems = nullptr;
    auto row = holderFactory.CreateDirectArrayHolder(inRow.size(), rowItems);

    for (ui32 i = 0; i < inRow.size(); ++i) {
        rowItems[i] = GetCellValue(inRow[i], inType[i]);
    }

    return std::move(row);
}

} // namespace

///
struct ItemInfo {
    ui32 ColumnId;
    NScheme::TTypeInfo SchemeType;
    TOptionalType* OptType;

    ItemInfo(ui32 colId, NScheme::TTypeInfo schemeType, TOptionalType* optType)
        : ColumnId(colId)
        , SchemeType(schemeType)
        , OptType(optType)
    {}

    TType* ItemType() const { return OptType->GetItemType(); }
};

///
struct TRowResultInfo {
    TOptionalType* ResultType;
    TStructType* RowType;
    TSmallVec<ItemInfo> ItemInfos;

    TRowResultInfo(const TStructLiteral* columnIds, const THashMap<ui32, TSysTables::TTableColumnInfo>& columns,
                   TOptionalType* returnType)
    {
        ResultType = AS_TYPE(TOptionalType, returnType);
        RowType = AS_TYPE(TStructType, ResultType->GetItemType());

        ItemInfos.reserve(columnIds->GetValuesCount());
        for (ui32 i = 0; i < columnIds->GetValuesCount(); ++i) {
            TOptionalType * optType = AS_TYPE(TOptionalType, RowType->GetMemberType(i));
            TDataLiteral* literal = AS_VALUE(TDataLiteral, columnIds->GetValue(i));
            ui32 colId = literal->AsValue().Get<ui32>();

            const TSysTables::TTableColumnInfo * colInfo = columns.FindPtr(colId);
            Y_VERIFY(colInfo && (colInfo->Id == colId), "No column info for column");
            ItemInfos.emplace_back(ItemInfo(colId, colInfo->PType, optType));
        }
    }

    NUdf::TUnboxedValue CreateResult(TVector<TCell>&& inRow, const THolderFactory& holderFactory) const {
        if (inRow.empty()) {
            return NUdf::TUnboxedValuePod();
        }

        Y_VERIFY(inRow.size() >= ItemInfos.size());

        // reorder columns
        TVector<TCell> outRow(Reserve(ItemInfos.size()));
        TVector<NScheme::TTypeInfo> outTypes(Reserve(ItemInfos.size()));
        for (ui32 i = 0; i < ItemInfos.size(); ++i) {
            ui32 colId = ItemInfos[i].ColumnId;
            outRow.emplace_back(std::move(inRow[colId]));
            outTypes.emplace_back(ItemInfos[i].SchemeType);
        }

        return CreateRow(outRow, outTypes, holderFactory);
    }
};

///
class TDataShardSysTable {
public:
    using TUpdateCommand = IEngineFlatHost::TUpdateCommand;

    TDataShardSysTable(const TTableId& tableId, TDataShard* self)
        : TableId(tableId)
        , Self(self)
    {
        switch (TableId.PathId.LocalPathId) {
        case TSysTables::SysTableLocks:
            TSysTables::TLocksTable::GetInfo(Columns, KeyTypes, false);
            break;
        case TSysTables::SysTableLocks2:
            TSysTables::TLocksTable::GetInfo(Columns, KeyTypes, true);
            break;
        default:
            Y_ASSERT("Unexpected system table id");
        }
    }

    NUdf::TUnboxedValue SelectRow(const TArrayRef<const TCell>& row, TStructLiteral* columnIds,
        TOptionalType* returnType, const TReadTarget& readTarget, const THolderFactory& holderFactory) const
    {
        Y_UNUSED(readTarget);

        TRowResultInfo result(columnIds, Columns, returnType);

        auto lock = Self->SysLocksTable().GetLock(row);
        Y_VERIFY(!lock.IsError());

        if (TableId.PathId.LocalPathId == TSysTables::SysTableLocks2) {
            return result.CreateResult(lock.MakeRow(true), holderFactory);
        }
        Y_VERIFY(TableId.PathId.LocalPathId == TSysTables::SysTableLocks);
        return result.CreateResult(lock.MakeRow(false), holderFactory);
    }

    void UpdateRow(const TArrayRef<const TCell>& row, const TArrayRef<const TUpdateCommand>& commands) const
    {
        Y_UNUSED(row);
        Y_UNUSED(commands);

        Y_ASSERT("Not supported");
    }

    void EraseRow(const TArrayRef<const TCell>& row) const {
        Self->SysLocksTable().EraseLock(row);
    }

    bool IsValidKey(TKeyDesc& key) const {
        Y_UNUSED(key); // TODO
        return true;
    }

    bool IsMyKey(const TArrayRef<const TCell>& row) const {
        return Self->SysLocksTable().IsMyKey(row);
    }

private:
    const TTableId TableId;
    TDataShard* Self;
    THashMap<ui32, TSysTables::TTableColumnInfo> Columns;
    TVector<NScheme::TTypeInfo> KeyTypes;
};


class TDataShardSysTables : public TThrRefBase {
    TDataShardSysTable Locks;
    TDataShardSysTable Locks2;
public:
    TDataShardSysTables(TDataShard *self)
        : Locks(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks), self)
        , Locks2(TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks2), self)
    {}

    const TDataShardSysTable& Get(const TTableId& tableId) const {
        if (tableId.PathId.LocalPathId == TSysTables::SysTableLocks2)
            return Locks2;

        if (tableId.PathId.LocalPathId == TSysTables::SysTableLocks)
            return Locks;

        Y_FAIL("unexpected sys table id");
    }
};

TIntrusivePtr<TThrRefBase> InitDataShardSysTables(TDataShard* self) {
    return new TDataShardSysTables(self);
}

///
class TDataShardEngineHost : public TEngineHost, public IDataShardUserDb {
public:
    TDataShardEngineHost(TDataShard* self, TEngineBay& engineBay, NTable::TDatabase& db, TEngineHostCounters& counters, ui64& lockTxId, ui32& lockNodeId, TInstant now)
        : TEngineHost(db, counters,
            TEngineHostSettings(self->TabletID(),
                (self->State == TShardState::Readonly || self->State == TShardState::Frozen),
                self->ByKeyFilterDisabled(),
                self->GetKeyAccessSampler()))
        , Self(self)
        , EngineBay(engineBay)
        , DB(db)
        , LockTxId(lockTxId)
        , LockNodeId(lockNodeId)
        , Now(now)
    {
    }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            NTable::TSelectStats& stats,
            const TMaybe<TRowVersion>& readVersion) override
    {
        auto tid = LocalTableId(tableId);

        return DB.Select(
            tid, key, tags, row, stats,
            /* readFlags */ 0,
            readVersion.GetOrElse(ReadVersion),
            GetReadTxMap(tableId),
            GetReadTxObserver(tableId));
    }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion) override
    {
        NTable::TSelectStats stats;
        return SelectRow(tableId, key, tags, row, stats, readVersion);
    }

    void SetWriteVersion(TRowVersion writeVersion) {
        WriteVersion = writeVersion;
    }

    TRowVersion GetWriteVersion(const TTableId& tableId) const override {
        Y_UNUSED(tableId);
        Y_VERIFY(!WriteVersion.IsMax(), "Cannot perform writes without WriteVersion set");
        return WriteVersion;
    }

    void SetReadVersion(TRowVersion readVersion) {
        ReadVersion = readVersion;
    }

    TRowVersion GetReadVersion(const TTableId& tableId) const override {
        Y_UNUSED(tableId);
        Y_VERIFY(!ReadVersion.IsMin(), "Cannot perform reads without ReadVersion set");
        return ReadVersion;
    }

    void SetVolatileTxId(ui64 txId) {
        VolatileTxId = txId;
    }

    void SetIsImmediateTx() {
        IsImmediateTx = true;
    }

    void SetIsRepeatableSnapshot() {
        IsRepeatableSnapshot = true;
    }

    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId) const override {
        auto it = ChangeCollectors.find(tableId.PathId);
        if (it != ChangeCollectors.end()) {
            return it->second.Get();
        }

        it = ChangeCollectors.emplace(tableId.PathId, nullptr).first;
        if (!Self->IsUserTable(tableId)) {
            return it->second.Get();
        }

        it->second.Reset(CreateChangeCollector(*Self, *const_cast<TDataShardEngineHost*>(this), DB, tableId.PathId.LocalPathId, IsImmediateTx));
        return it->second.Get();
    }

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) {
        auto localTid = Self->GetLocalTableId(tableId);
        Y_VERIFY_S(localTid, "Unexpected failure to find table " << tableId << " in datashard " << Self->TabletID());

        if (!DB.HasOpenTx(localTid, lockId)) {
            return;
        }

        if (VolatileTxId) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Scheduling commit of lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self->TabletID());
            if (VolatileCommitTxIds.insert(lockId).second) {
                // Update TxMap to include the new commit
                auto it = TxMaps.find(tableId.PathId);
                if (it != TxMaps.end()) {
                    it->second->Add(lockId, WriteVersion);
                }
            }
            return;
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
            "Committing changes lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self->TabletID());
        DB.CommitTx(localTid, lockId, writeVersion);

        if (!CommittedLockChanges.contains(lockId) && Self->HasLockChangeRecords(lockId)) {
            if (auto* collector = GetChangeCollector(tableId)) {
                collector->CommitLockChanges(lockId, WriteVersion);
                CommittedLockChanges.insert(lockId);
            }
        }
    }

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const {
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

    void ResetCollectedChanges() {
        for (auto& pr : ChangeCollectors) {
            if (pr.second) {
                pr.second->OnRestart();
            }
        }
    }

    TVector<ui64> GetVolatileCommitTxIds() const {
        TVector<ui64> commitTxIds;

        if (!VolatileCommitTxIds.empty()) {
            commitTxIds.reserve(VolatileCommitTxIds.size());
            for (ui64 commitTxId : VolatileCommitTxIds) {
                commitTxIds.push_back(commitTxId);
            }
        }

        return commitTxIds;
    }

    TVector<ui64> GetVolatileDependencies() const {
        TVector<ui64> dependencies;

        if (!VolatileDependencies.empty()) {
            dependencies.reserve(VolatileDependencies.size());
            for (ui64 dependency : VolatileDependencies) {
                dependencies.push_back(dependency);
            }
        }

        return dependencies;
    }

    bool IsValidKey(TKeyDesc& key, std::pair<ui64, ui64>& maxSnapshotTime) const override {
        if (TSysTables::IsSystemTable(key.TableId))
            return DataShardSysTable(key.TableId).IsValidKey(key);

        if (LockTxId) {
            // Prevent updates/erases with LockTxId set, unless it's allowed for immediate mvcc txs
            if (key.RowOperation != TKeyDesc::ERowOperation::Read &&
                (!Self->GetEnableLockedWrites() || !IsImmediateTx || !IsRepeatableSnapshot || !LockNodeId))
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

        return TEngineHost::IsValidKey(key, maxSnapshotTime);
    }

    NUdf::TUnboxedValue SelectRow(const TTableId& tableId, const TArrayRef<const TCell>& row,
        TStructLiteral* columnIds, TOptionalType* returnType, const TReadTarget& readTarget,
        const THolderFactory& holderFactory) override
    {
        if (TSysTables::IsSystemTable(tableId)) {
            return DataShardSysTable(tableId).SelectRow(row, columnIds, returnType, readTarget, holderFactory);
        }

        if (LockTxId) {
            Self->SysLocksTable().SetLock(tableId, row);
        }

        Self->SetTableAccessTime(tableId, Now);
        return TEngineHost::SelectRow(tableId, row, columnIds, returnType, readTarget, holderFactory);
    }

    NUdf::TUnboxedValue SelectRange(const TTableId& tableId, const TTableRange& range,
        TStructLiteral* columnIds,  TListLiteral* skipNullKeys, TStructType* returnType,
        const TReadTarget& readTarget, ui64 itemsLimit, ui64 bytesLimit, bool reverse,
        std::pair<const TListLiteral*, const TListLiteral*> forbidNullArgs, const THolderFactory& holderFactory) override
    {
        Y_VERIFY(!TSysTables::IsSystemTable(tableId), "SelectRange no system table is not supported");

        if (LockTxId) {
            Self->SysLocksTable().SetLock(tableId, range);
        }

        Self->SetTableAccessTime(tableId, Now);
        return TEngineHost::SelectRange(tableId, range, columnIds, skipNullKeys, returnType, readTarget,
            itemsLimit, bytesLimit, reverse, forbidNullArgs, holderFactory);
    }

    void UpdateRow(const TTableId& tableId, const TArrayRef<const TCell>& row, const TArrayRef<const TUpdateCommand>& commands) override {
        if (TSysTables::IsSystemTable(tableId)) {
            DataShardSysTable(tableId).UpdateRow(row, commands);
            return;
        }

        CheckWriteConflicts(tableId, row);

        if (LockTxId) {
            Self->SysLocksTable().SetWriteLock(tableId, row);
        } else {
            Self->SysLocksTable().BreakLocks(tableId, row);
        }
        Self->SetTableUpdateTime(tableId, Now);

        // apply special columns if declared
        TUserTable::TSpecialUpdate specUpdates = Self->SpecialUpdates(DB, tableId);
        if (specUpdates.HasUpdates) {
            TStackVec<TUpdateCommand> extendedCmds;
            extendedCmds.reserve(commands.size() + 3);
            for (const TUpdateCommand& cmd : commands) {
                if (cmd.Column == specUpdates.ColIdTablet)
                    specUpdates.ColIdTablet = Max<ui32>();
                else if (cmd.Column == specUpdates.ColIdEpoch)
                    specUpdates.ColIdEpoch = Max<ui32>();
                else if (cmd.Column == specUpdates.ColIdUpdateNo)
                    specUpdates.ColIdUpdateNo = Max<ui32>();

                extendedCmds.push_back(cmd);
            }

            if (specUpdates.ColIdTablet != Max<ui32>()) {
                extendedCmds.emplace_back(TUpdateCommand{
                    specUpdates.ColIdTablet, TKeyDesc::EColumnOperation::Set, EInplaceUpdateMode::Unknown,
                    TCell::Make<ui64>(specUpdates.Tablet)
                });
            }

            if (specUpdates.ColIdEpoch != Max<ui32>()) {
                extendedCmds.emplace_back(TUpdateCommand{
                    specUpdates.ColIdEpoch, TKeyDesc::EColumnOperation::Set, EInplaceUpdateMode::Unknown,
                    TCell::Make<ui64>(specUpdates.Epoch)
                });
            }

            if (specUpdates.ColIdUpdateNo != Max<ui32>()) {
                extendedCmds.emplace_back(TUpdateCommand{
                    specUpdates.ColIdUpdateNo, TKeyDesc::EColumnOperation::Set, EInplaceUpdateMode::Unknown,
                    TCell::Make<ui64>(specUpdates.UpdateNo)
                });
            }

            TEngineHost::UpdateRow(tableId, row, {extendedCmds.data(), extendedCmds.size()});
        } else {
            TEngineHost::UpdateRow(tableId, row, commands);
        }
    }

    void EraseRow(const TTableId& tableId, const TArrayRef<const TCell>& row) override {
        if (TSysTables::IsSystemTable(tableId)) {
            DataShardSysTable(tableId).EraseRow(row);
            return;
        }

        CheckWriteConflicts(tableId, row);

        if (LockTxId) {
            Self->SysLocksTable().SetWriteLock(tableId, row);
        } else {
            Self->SysLocksTable().BreakLocks(tableId, row);
        }

        Self->SetTableUpdateTime(tableId, Now);
        TEngineHost::EraseRow(tableId, row);
    }

    // Returns whether row belong this shard.
    bool IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const override {
        if (TSysTables::IsSystemTable(tableId))
            return DataShardSysTable(tableId).IsMyKey(row);

        auto iter = Self->TableInfos.find(tableId.PathId.LocalPathId);
        if (iter == Self->TableInfos.end()) {
            // TODO: can this happen?
            return false;
        }

        // Check row against range
        const TUserTable& info = *iter->second;
        return (ComparePointAndRange(row, info.GetTableRange(), info.KeyColumnTypes, info.KeyColumnTypes) == 0);
    }

    bool IsPathErased(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return false;
        return TDataShardEngineHost::LocalTableId(tableId) == 0;
    }

    ui64 LocalTableId(const TTableId &tableId) const override {
        return Self->GetLocalTableId(tableId);
    }

    ui64 GetTableSchemaVersion(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return 0;
        const auto& userTables = Self->GetUserTables();
        auto it = userTables.find(tableId.PathId.LocalPathId);
        if (it == userTables.end()) {
            Y_FAIL_S("DatshardEngineHost (tablet id: " << Self->TabletID()
                     << " state: " << Self->GetState()
                     << ") unables to find given table with id: " << tableId);
            return 0;
        } else {
            return it->second->GetTableSchemaVersion();
        }
    }

    ui64 GetWriteTxId(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return 0;

        if (VolatileTxId) {
            Y_VERIFY(!LockTxId);
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

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId)) {
            return nullptr;
        }

        auto baseTxMap = Self->GetVolatileTxManager().GetTxMap();

        bool needTxMap = (
            // We need tx map when there are waiting volatile transactions
            baseTxMap ||
            // We need tx map to see committed volatile tx changes
            VolatileTxId && !VolatileCommitTxIds.empty() ||
            // We need tx map when current lock has uncommitted changes
            LockTxId && Self->SysLocksTable().HasCurrentWriteLock(tableId));

        if (!needTxMap) {
            // We don't need tx map
            return nullptr;
        }

        auto& ptr = TxMaps[tableId.PathId];
        if (!ptr) {
            ptr = new NTable::TDynamicTransactionMap(baseTxMap);
            if (LockTxId) {
                // Uncommitted changes are visible in all possible snapshots
                ptr->Add(LockTxId, TRowVersion::Min());
            } else if (VolatileTxId) {
                // We want committed volatile changes to be visible at the write version
                for (ui64 commitTxId : VolatileCommitTxIds) {
                    ptr->Add(commitTxId, WriteVersion);
                }
            }
        }

        return ptr;
    }

    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return nullptr;

        bool needObserver = (
            // We need observer when there are waiting changes in the tx map
            Self->GetVolatileTxManager().GetTxMap() ||
            // We need observer for locked reads when there are active write locks
            LockTxId && Self->SysLocksTable().HasWriteLocks(tableId));

        if (!needObserver) {
            // We don't need tx observer
            return nullptr;
        }

        auto& ptr = TxObservers[tableId.PathId];
        if (!ptr) {
            if (LockTxId) {
                ptr = new TLockedReadTxObserver(this);
            } else {
                ptr = new TReadTxObserver(this);
            }
        }

        return ptr;
    }

    class TLockedReadTxObserver : public NTable::ITransactionObserver {
    public:
        TLockedReadTxObserver(const TDataShardEngineHost* host)
            : Host(host)
        { }

        void OnSkipUncommitted(ui64 txId) override {
            Host->AddReadConflict(txId);
        }

        void OnSkipCommitted(const TRowVersion&) override {
            // We already use InvisibleRowSkips for these
        }

        void OnSkipCommitted(const TRowVersion&, ui64) override {
            // We already use InvisibleRowSkips for these
        }

        void OnApplyCommitted(const TRowVersion& rowVersion) override {
            Host->CheckReadConflict(rowVersion);
        }

        void OnApplyCommitted(const TRowVersion& rowVersion, ui64 txId) override {
            Host->CheckReadConflict(rowVersion);
            Host->CheckReadDependency(txId);
        }

    private:
        const TDataShardEngineHost* const Host;
    };

    class TReadTxObserver : public NTable::ITransactionObserver {
    public:
        TReadTxObserver(const TDataShardEngineHost* host)
            : Host(host)
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
            Host->CheckReadDependency(txId);
        }

    private:
        const TDataShardEngineHost* const Host;
    };

    void AddReadConflict(ui64 txId) const {
        Y_VERIFY(LockTxId);

        // We have detected uncommitted changes in txId that could affect
        // our read result. We arrange a conflict that breaks our lock
        // when txId commits.
        Self->SysLocksTable().AddReadConflict(txId);
    }

    void CheckReadConflict(const TRowVersion& rowVersion) const {
        Y_VERIFY(LockTxId);

        if (rowVersion > ReadVersion) {
            // We are reading from snapshot at ReadVersion and should not normally
            // observe changes with a version above that. However, if we have an
            // uncommitted change, that we fake as committed for our own changes
            // visibility, we might shadow some change that happened after a
            // snapshot. This is a clear indication of a conflict between read
            // and that future conflict, hence we must break locks and abort.
            Self->SysLocksTable().BreakSetLocks();
            EngineBay.GetKqpComputeCtx().SetInconsistentReads();
        }
    }

    void CheckReadDependency(ui64 txId) const {
        if (auto* info = Self->GetVolatileTxManager().FindByCommitTxId(txId)) {
            switch (info->State) {
                case EVolatileTxState::Waiting:
                    // We are reading undecided changes and need to wait until they are resolved
                    EngineBay.GetKqpComputeCtx().AddVolatileReadDependency(info->TxId);
                    break;
                case EVolatileTxState::Committed:
                    // Committed changes are immediately visible and don't need a dependency
                    break;
                case EVolatileTxState::Aborting:
                    // We just read something that we know is aborting, we would have to retry later
                    EngineBay.GetKqpComputeCtx().AddVolatileReadDependency(info->TxId);
                    break;
            }
        }
    }

    bool NeedToReadBeforeWrite(const TTableId& tableId) const override {
        if (Self->GetVolatileTxManager().GetTxMap()) {
            return true;
        }

        if (Self->SysLocksTable().HasWriteLocks(tableId)) {
            return true;
        }

        if (auto* collector = GetChangeCollector(tableId)) {
            if (collector->NeedToReadKeys()) {
                return true;
            }
        }

        return false;
    }

    void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> row) {
        if (!Self->GetVolatileTxManager().GetTxMap() &&
            !Self->SysLocksTable().HasWriteLocks(tableId))
        {
            // We don't have any uncommitted changes, so there's nothing we
            // could possibly conflict with.
            return;
        }

        const auto localTid = LocalTableId(tableId);
        Y_VERIFY(localTid);
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
        TSmallVec<TRawTypeValue> key;
        ConvertTableKeys(Scheme, tableInfo, row, key, nullptr);

        ui64 skipCount = 0;

        NTable::ITransactionObserverPtr txObserver;
        if (LockTxId) {
            txObserver = new TLockedWriteTxObserver(this, LockTxId, skipCount, localTid);
        } else {
            txObserver = new TWriteTxObserver(this);
        }

        // We are not actually interested in the row version, we only need to
        // detect uncommitted transaction skips on the path to that version.
        auto res = Db.SelectRowVersion(
            localTid, key, /* readFlags */ 0,
            nullptr, txObserver);

        if (res.Ready == NTable::EReady::Page) {
            throw TNotReadyTabletException();
        }

        if (LockTxId || VolatileTxId) {
            ui64 skipLimit = Self->GetMaxLockedWritesPerKey();
            if (skipLimit > 0 && skipCount >= skipLimit) {
                throw TLockedWriteLimitException();
            }
        }
    }

    class TLockedWriteTxObserver : public NTable::ITransactionObserver {
    public:
        TLockedWriteTxObserver(TDataShardEngineHost* host, ui64 txId, ui64& skipCount, ui32 localTid)
            : Host(host)
            , SelfTxId(txId)
            , SkipCount(skipCount)
            , LocalTid(localTid)
        {
        }

        void OnSkipUncommitted(ui64 txId) override {
            // Note: all active volatile transactions will be uncommitted
            // without a tx map, and will be handled by AddWriteConflict.
            if (!Host->Db.HasRemovedTx(LocalTid, txId)) {
                ++SkipCount;
                if (!SelfFound) {
                    if (txId != SelfTxId) {
                        Host->AddWriteConflict(txId);
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
        TDataShardEngineHost* const Host;
        const ui64 SelfTxId;
        ui64& SkipCount;
        const ui32 LocalTid;
        bool SelfFound = false;
    };

    class TWriteTxObserver : public NTable::ITransactionObserver {
    public:
        TWriteTxObserver(TDataShardEngineHost* host)
            : Host(host)
        {
        }

        void OnSkipUncommitted(ui64 txId) override {
            // Note: all active volatile transactions will be uncommitted
            // without a tx map, and will be handled by BreakWriteConflict.
            Host->BreakWriteConflict(txId);
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
        TDataShardEngineHost* const Host;
    };

    void AddWriteConflict(ui64 txId) const {
        if (auto* info = Self->GetVolatileTxManager().FindByCommitTxId(txId)) {
            Y_FAIL("TODO: add future lock dependency from %" PRIu64 " on %" PRIu64, LockTxId, info->TxId);
        } else {
            Self->SysLocksTable().AddWriteConflict(txId);
        }
    }

    void BreakWriteConflict(ui64 txId) {
        if (VolatileCommitTxIds.contains(txId)) {
            // Skip our own commits
        } else if (auto* info = Self->GetVolatileTxManager().FindByCommitTxId(txId)) {
            // We must not overwrite uncommitted changes that may become committed
            // later, so we need to add a dependency that will force us to wait
            // until it is persistently committed. We may ignore aborting changes
            // even though they may not be persistent yet, since this tx will
            // also perform writes, and either it fails, or future generation
            // could not have possibly committed it already.
            if (info->State != EVolatileTxState::Aborting) {
                if (!VolatileTxId) {
                    // All further writes will use this VolatileTxId and will
                    // add it to VolatileCommitTxIds, forcing it to be committed
                    // like a volatile transaction. Note that this does not make
                    // it into a real volatile transaction, it works as usual in
                    // every sense, only persistent commit order is affected by
                    // a dependency below.
                    VolatileTxId = EngineBay.GetTxId();
                }
                VolatileDependencies.insert(info->TxId);
            }
        } else {
            // Break uncommitted locks
            Self->SysLocksTable().BreakLock(txId);
        }
    }

private:
    const TDataShardSysTable& DataShardSysTable(const TTableId& tableId) const {
        return static_cast<const TDataShardSysTables *>(Self->GetDataShardSysTables())->Get(tableId);
    }

    TDataShard* Self;
    TEngineBay& EngineBay;
    NTable::TDatabase& DB;
    const ui64& LockTxId;
    const ui32& LockNodeId;
    bool IsImmediateTx = false;
    bool IsRepeatableSnapshot = false;
    TInstant Now;
    TRowVersion WriteVersion = TRowVersion::Max();
    TRowVersion ReadVersion = TRowVersion::Min();
    ui64 VolatileTxId = 0;
    absl::flat_hash_set<ui64> CommittedLockChanges;
    mutable absl::flat_hash_map<TPathId, THolder<IDataShardChangeCollector>> ChangeCollectors;
    mutable absl::flat_hash_map<TPathId, TIntrusivePtr<NTable::TDynamicTransactionMap>> TxMaps;
    mutable absl::flat_hash_map<TPathId, NTable::ITransactionObserverPtr> TxObservers;
    mutable absl::flat_hash_set<ui64> VolatileCommitTxIds;
    mutable absl::flat_hash_set<ui64> VolatileDependencies;
};

//

TEngineBay::TEngineBay(TDataShard * self, TTransactionContext& txc, const TActorContext& ctx,
                       std::pair<ui64, ui64> stepTxId)
    : StepTxId(stepTxId)
    , LockTxId(0)
    , LockNodeId(0)
{
    auto now = TAppData::TimeProvider->Now();
    EngineHost = MakeHolder<TDataShardEngineHost>(self, *this, txc.DB, EngineHostCounters, LockTxId, LockNodeId, now);

    EngineSettings = MakeHolder<TEngineFlatSettings>(IEngineFlat::EProtocol::V1, AppData(ctx)->FunctionRegistry,
        *TAppData::RandomProvider, *TAppData::TimeProvider, EngineHost.Get(), self->AllocCounters);

    auto tabletId = self->TabletID();
    auto txId = stepTxId.second;
    const TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
    EngineSettings->LogErrorWriter = [actorSystem, tabletId, txId](const TString& message) {
        LOG_ERROR_S(*actorSystem, NKikimrServices::MINIKQL_ENGINE,
            "Shard %" << tabletId << ", txid %" <<txId << ", engine error: " << message);
    };

    if (ctx.LoggerSettings()->Satisfies(NLog::PRI_DEBUG, NKikimrServices::MINIKQL_ENGINE, stepTxId.second)) {
        EngineSettings->BacktraceWriter =
            [actorSystem, tabletId, txId](const char * operation, ui32 line, const TBackTrace* backtrace)
            {
                LOG_DEBUG(*actorSystem, NKikimrServices::MINIKQL_ENGINE,
                    "Shard %" PRIu64 ", txid %, %s (%" PRIu32 ")\n%s",
                    tabletId, txId, operation, line,
                    backtrace ? backtrace->PrintToString().data() : "");
            };
    }

    KqpLogFunc = [actorSystem, tabletId, txId](const TStringBuf& message) {
        LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER,
            "Shard %" << tabletId << ", txid %" << txId << ": " << message);
    };

    ComputeCtx = MakeHolder<TKqpDatashardComputeContext>(self, *EngineHost, now);
    ComputeCtx->Database = &txc.DB;

    KqpAlloc = MakeHolder<TScopedAlloc>(__LOCATION__, TAlignedPagePoolCounters(), AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
    KqpTypeEnv = MakeHolder<TTypeEnvironment>(*KqpAlloc);
    KqpAlloc->Release();

    auto kqpApplyCtx = MakeHolder<TKqpDatashardApplyContext>();
    kqpApplyCtx->Host = EngineHost.Get();
    kqpApplyCtx->ShardTableStats = &ComputeCtx->GetDatashardCounters();
    kqpApplyCtx->Env = KqpTypeEnv.Get();

    KqpApplyCtx.Reset(kqpApplyCtx.Release());

    KqpExecCtx.FuncRegistry = AppData(ctx)->FunctionRegistry;
    KqpExecCtx.ComputeCtx = ComputeCtx.Get();
    KqpExecCtx.ComputationFactory = GetKqpDatashardComputeFactory(ComputeCtx.Get());
    KqpExecCtx.RandomProvider = TAppData::RandomProvider.Get();
    KqpExecCtx.TimeProvider = TAppData::TimeProvider.Get();
    KqpExecCtx.ApplyCtx = KqpApplyCtx.Get();
    KqpExecCtx.Alloc = KqpAlloc.Get();
    KqpExecCtx.TypeEnv = KqpTypeEnv.Get();
    if (auto* rm = NKqp::TryGetKqpResourceManager()) {
        KqpExecCtx.PatternCache = rm->GetPatternCache();
    }
}

TEngineBay::~TEngineBay() {
    if (KqpTasksRunner) {
        KqpTasksRunner.Reset();
        auto guard = TGuard(*KqpAlloc);
        KqpTypeEnv.Reset();
    }
}

void TEngineBay::AddReadRange(const TTableId& tableId, const TVector<NTable::TColumn>& columns,
    const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, ui64 itemsLimit, bool reverse)
{
    TVector<TKeyDesc::TColumnOp> columnOps;
    columnOps.reserve(columns.size());
    for (auto& column : columns) {
        TKeyDesc::TColumnOp op;
        op.Column = column.Id;
        op.Operation = TKeyDesc::EColumnOperation::Read;
        op.ExpectedType = column.PType;
        columnOps.emplace_back(std::move(op));
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
        "-- AddReadRange: " << DebugPrintRange(keyTypes, range, *AppData()->TypeRegistry) << " table: " << tableId);

    auto desc = MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Read, keyTypes, columnOps, itemsLimit,
        0 /* bytesLimit */, reverse);
    Info.Keys.emplace_back(TValidatedKey(std::move(desc), /* isWrite */ false));
    // Info.Keys.back().IsResultPart = not a lock key? // TODO: KIKIMR-11134
    ++Info.ReadsCount;
    Info.Loaded = true;
}

void TEngineBay::AddWriteRange(const TTableId& tableId, const TTableRange& range,
    const TVector<NScheme::TTypeInfo>& keyTypes, const TVector<TColumnWriteMeta>& columns,
    bool isPureEraseOp)
{
    TVector<TKeyDesc::TColumnOp> columnOps;
    for (const auto& writeColumn : columns) {
        TKeyDesc::TColumnOp op;
        op.Column = writeColumn.Column.Id;
        op.Operation = TKeyDesc::EColumnOperation::Set;
        op.ExpectedType = writeColumn.Column.PType;
        op.ImmediateUpdateSize = writeColumn.MaxValueSizeBytes;
        columnOps.emplace_back(std::move(op));
    }

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
        "-- AddWriteRange: " << DebugPrintRange(keyTypes, range, *AppData()->TypeRegistry) << " table: " << tableId);

    auto rowOp = isPureEraseOp ? TKeyDesc::ERowOperation::Erase : TKeyDesc::ERowOperation::Update;
    auto desc = MakeHolder<TKeyDesc>(tableId, range, rowOp, keyTypes, columnOps);
    Info.Keys.emplace_back(TValidatedKey(std::move(desc), /* isWrite */ true));
    ++Info.WritesCount;
    if (!range.Point) {
        ++Info.DynKeysCount;
    }
    Info.Loaded = true;
}

TEngineBay::TSizes TEngineBay::CalcSizes(bool needsTotalKeysSize) const {
    Y_VERIFY(EngineHost);

    TSizes outSizes;
    TVector<const TKeyDesc*> readKeys;
    readKeys.reserve(Info.ReadsCount);
    for (const TValidatedKey& validKey : Info.Keys) {
        if (validKey.IsWrite)
            continue;

        readKeys.emplace_back(validKey.Key.get());
        if (needsTotalKeysSize || validKey.NeedSizeCalculation()) {
            ui64 size = EngineHost->CalculateResultSize(*validKey.Key);

            if (needsTotalKeysSize) {
                outSizes.TotalKeysSize += size;
            }

            if (validKey.IsResultPart) {
                outSizes.ReplySize += size;
            }

            for (ui64 shard : validKey.TargetShards) {
                outSizes.OutReadSetSize[shard] += size;
            }
        }
    }

    outSizes.ReadSize = EngineHost->CalculateReadSize(readKeys);
    return outSizes;
}

void TEngineBay::SetWriteVersion(TRowVersion writeVersion) {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetWriteVersion(writeVersion);
}

void TEngineBay::SetReadVersion(TRowVersion readVersion) {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetReadVersion(readVersion);

    Y_VERIFY(ComputeCtx);
    ComputeCtx->SetReadVersion(readVersion);
}

void TEngineBay::SetVolatileTxId(ui64 txId) {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetVolatileTxId(txId);
}

void TEngineBay::SetIsImmediateTx() {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetIsImmediateTx();
}

void TEngineBay::SetIsRepeatableSnapshot() {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetIsRepeatableSnapshot();
}

void TEngineBay::CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->CommitChanges(tableId, lockId, writeVersion);
}

TVector<IDataShardChangeCollector::TChange> TEngineBay::GetCollectedChanges() const {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetCollectedChanges();
}

void TEngineBay::ResetCollectedChanges() {
    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->ResetCollectedChanges();
}

TVector<ui64> TEngineBay::GetVolatileCommitTxIds() const {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileCommitTxIds();
}

TVector<ui64> TEngineBay::GetVolatileDependencies() const {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileDependencies();
}

IEngineFlat * TEngineBay::GetEngine() {
    if (!Engine) {
        Engine = CreateEngineFlat(*EngineSettings);
        Engine->SetStepTxId(StepTxId);
    }

    return Engine.Get();
}

void TEngineBay::SetLockTxId(ui64 lockTxId, ui32 lockNodeId) {
    LockTxId = lockTxId;
    LockNodeId = lockNodeId;
    if (ComputeCtx) {
        ComputeCtx->SetLockTxId(lockTxId, lockNodeId);
    }
}

NKqp::TKqpTasksRunner& TEngineBay::GetKqpTasksRunner(const NKikimrTxDataShard::TKqpTransaction& tx) {
    if (!KqpTasksRunner) {
        NYql::NDq::TDqTaskRunnerSettings settings;

        if (tx.HasRuntimeSettings() && tx.GetRuntimeSettings().HasStatsMode()) {
            auto statsMode = tx.GetRuntimeSettings().GetStatsMode();
            // Always collect basic stats for system views / request unit computation.
            settings.CollectBasicStats = true;
            settings.CollectProfileStats = statsMode >= NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        } else {
            settings.CollectBasicStats = false;
            settings.CollectProfileStats = false;
        }

        settings.OptLLVM = "OFF";
        settings.TerminateOnError = false;
        settings.AllowGeneratorsInUnboxedValues = false;

        KqpAlloc->SetLimit(10_MB);
        KqpTasksRunner = NKqp::CreateKqpTasksRunner(tx.GetTasks(), KqpExecCtx, settings, KqpLogFunc);
    }

    return *KqpTasksRunner;
}

TKqpDatashardComputeContext& TEngineBay::GetKqpComputeCtx() {
    Y_VERIFY(ComputeCtx);
    return *ComputeCtx;
}

} // NDataShard
} // NKikimr
