#include "change_collector.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"
#include "datashard__engine_host.h"
#include "sys_tables.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
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
            NTable::TRowState& row) override
    {
        auto tid = LocalTableId(tableId);

        return DB.Select(
            tid, key, tags, row,
            /* readFlags */ 0,
            ReadVersion,
            GetReadTxMap(tableId),
            GetReadTxObserver(tableId));
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

    void SetIsImmediateTx() {
        IsImmediateTx = true;
    }

    void SetIsRepeatableSnapshot() {
        IsRepeatableSnapshot = true;
    }

    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId) const override {
        auto it = ChangeCollectors.find(tableId);
        if (it != ChangeCollectors.end()) {
            return it->second.Get();
        }

        it = ChangeCollectors.emplace(tableId, nullptr).first;
        if (!Self->IsUserTable(tableId)) {
            return it->second.Get();
        }

        it->second.Reset(CreateChangeCollector(*Self, *const_cast<TDataShardEngineHost*>(this), DB, tableId.PathId.LocalPathId, IsImmediateTx));
        return it->second.Get();
    }

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion, TTransactionContext& txc) {
        auto localTid = Self->GetLocalTableId(tableId);
        Y_VERIFY_S(localTid, "Unexpected failure to find table " << tableId << " in datashard " << Self->TabletID());

        if (!DB.HasOpenTx(localTid, lockId)) {
            return;
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
            "Committing changes lockId# " << lockId << " in localTid# " << localTid << " shard# " << Self->TabletID());
        DB.CommitTx(localTid, lockId, writeVersion);

        if (!CommittedLockChanges.contains(lockId)) {
            if (const auto& lockChanges = Self->GetLockChangeRecords(lockId)) {
                if (auto* collector = GetChangeCollector(tableId)) {
                    collector->SetWriteVersion(WriteVersion);
                    collector->CommitLockChanges(lockId, lockChanges, txc);
                    CommittedLockChanges.insert(lockId);
                }
            }
        }
    }

    TVector<IChangeCollector::TChange> GetCollectedChanges() const {
        TVector<IChangeCollector::TChange> total;

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
                pr.second->Reset();
            }
        }
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

        return LockTxId;
    }

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId) || !LockTxId)
            return nullptr;

        // Don't use tx map when we know there's no write lock for a table
        // Note: currently write lock implies uncommitted changes
        if (!Self->SysLocksTable().HasCurrentWriteLock(tableId)) {
            return nullptr;
        }

        auto& ptr = TxMaps[tableId];
        if (!ptr) {
            // Uncommitted changes are visible in all possible snapshots
            ptr = new NTable::TSingleTransactionMap(LockTxId, TRowVersion::Min());
        }

        return ptr;
    }

    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId) || !LockTxId)
            return nullptr;

        if (!Self->SysLocksTable().HasWriteLocks(tableId)) {
            // We don't have any active write locks, so there's nothing we
            // could possibly conflict with.
            return nullptr;
        }

        auto& ptr = TxObservers[tableId];
        if (!ptr) {
            // This observer is supposed to find conflicts
            ptr = new TReadTxObserver(this, tableId);
        }

        return ptr;
    }

    class TReadTxObserver : public NTable::ITransactionObserver {
    public:
        TReadTxObserver(const TDataShardEngineHost* host, const TTableId& tableId)
            : Host(host)
            , TableId(tableId)
        {
            Y_UNUSED(Host);
            Y_UNUSED(TableId);
        }

        void OnSkipUncommitted(ui64 txId) override {
            Host->AddReadConflict(TableId, txId);
        }

        void OnSkipCommitted(const TRowVersion&) override {
            // We already use InvisibleRowSkips for these
        }

        void OnSkipCommitted(const TRowVersion&, ui64) override {
            // We already use InvisibleRowSkips for these
        }

        void OnApplyCommitted(const TRowVersion& rowVersion) override {
            Host->CheckReadConflict(TableId, rowVersion);
        }

        void OnApplyCommitted(const TRowVersion& rowVersion, ui64) override {
            Host->CheckReadConflict(TableId, rowVersion);
        }

    private:
        const TDataShardEngineHost* const Host;
        const TTableId TableId;
    };

    void AddReadConflict(const TTableId& tableId, ui64 txId) const {
        Y_UNUSED(tableId);
        Y_VERIFY(LockTxId);

        // We have detected uncommitted changes in txId that could affect
        // our read result. We arrange a conflict that breaks our lock
        // when txId commits.
        Self->SysLocksTable().AddReadConflict(txId);
    }

    void CheckReadConflict(const TTableId& tableId, const TRowVersion& rowVersion) const {
        Y_UNUSED(tableId);
        Y_VERIFY(LockTxId);

        if (rowVersion > ReadVersion) {
            // We are reading from snapshot at ReadVersion and should not normally
            // observe changes with a version above that. However, if we have an
            // uncommitted change, that we fake as committed for our own changes
            // visibility, we might shadow some change that happened after a
            // snapshot. This is a clear indication of a conflict between read
            // and that future conflict, hence we must break locks and abort.
            // TODO: add an actual abort
            Self->SysLocksTable().BreakSetLocks();
            EngineBay.GetKqpComputeCtx().SetInconsistentReads();
        }
    }

    void CheckWriteConflicts(const TTableId& tableId, TArrayRef<const TCell> row) {
        if (!Self->SysLocksTable().HasWriteLocks(tableId)) {
            // We don't have any active write locks, so there's nothing we
            // could possibly conflict with.
            return;
        }

        const auto localTid = LocalTableId(tableId);
        Y_VERIFY(localTid);
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
        TSmallVec<TRawTypeValue> key;
        ConvertTableKeys(Scheme, tableInfo, row, key, nullptr);

        // We are not actually interested in the row version, we only need to
        // detect uncommitted transaction skips on the path to that version.
        auto res = Db.SelectRowVersion(
            localTid, key, /* readFlags */ 0,
            GetReadTxMap(tableId),
            new TWriteTxObserver(this, tableId));

        if (res.Ready == NTable::EReady::Page) {
            throw TNotReadyTabletException();
        }
    }

    class TWriteTxObserver : public NTable::ITransactionObserver {
    public:
        TWriteTxObserver(const TDataShardEngineHost* host, const TTableId& tableId)
            : Host(host)
            , TableId(tableId)
        {
            Y_UNUSED(Host);
            Y_UNUSED(TableId);
        }

        void OnSkipUncommitted(ui64 txId) override {
            Host->AddWriteConflict(TableId, txId);
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
        const TDataShardEngineHost* const Host;
        const TTableId TableId;
    };

    void AddWriteConflict(const TTableId& tableId, ui64 txId) const {
        Y_UNUSED(tableId);
        if (LockTxId) {
            Self->SysLocksTable().AddWriteConflict(txId);
        } else {
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
    THashSet<ui64> CommittedLockChanges;
    mutable THashMap<TTableId, THolder<IDataShardChangeCollector>> ChangeCollectors;
    mutable THashMap<TTableId, NTable::ITransactionMapPtr> TxMaps;
    mutable THashMap<TTableId, NTable::ITransactionObserverPtr> TxObservers;
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

    auto kqpApplyCtx = MakeHolder<TKqpDatashardApplyContext>();
    kqpApplyCtx->Host = EngineHost.Get();

    KqpApplyCtx.Reset(kqpApplyCtx.Release());

    KqpAlloc = MakeHolder<TScopedAlloc>(__LOCATION__, TAlignedPagePoolCounters(), AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
    KqpTypeEnv = MakeHolder<TTypeEnvironment>(*KqpAlloc);
    KqpAlloc->Release();

    KqpExecCtx.FuncRegistry = AppData(ctx)->FunctionRegistry;
    KqpExecCtx.ComputeCtx = ComputeCtx.Get();
    KqpExecCtx.ComputationFactory = GetKqpDatashardComputeFactory(ComputeCtx.Get());
    KqpExecCtx.RandomProvider = TAppData::RandomProvider.Get();
    KqpExecCtx.TimeProvider = TAppData::TimeProvider.Get();
    KqpExecCtx.ApplyCtx = KqpApplyCtx.Get();
    KqpExecCtx.Alloc = KqpAlloc.Get();
    KqpExecCtx.TypeEnv = KqpTypeEnv.Get();
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

void TEngineBay::CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion, TTransactionContext& txc) {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->CommitChanges(tableId, lockId, writeVersion, txc);
}

TVector<IChangeCollector::TChange> TEngineBay::GetCollectedChanges() const {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetCollectedChanges();
}

void TEngineBay::ResetCollectedChanges() {
    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->ResetCollectedChanges();
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
