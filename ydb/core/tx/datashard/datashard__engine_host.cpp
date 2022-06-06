#include "change_collector.h"
#include "datashard_impl.h"
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
                              const TVector<NScheme::TTypeId>& inType,
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
    NScheme::TTypeId SchemeType;
    TOptionalType* OptType;

    ItemInfo(ui32 colId, NScheme::TTypeId schemeType, TOptionalType* optType)
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
        TVector<NScheme::TTypeId> outTypes(Reserve(ItemInfos.size()));
        for (ui32 i = 0; i < ItemInfos.size(); ++i) {
            ui32 colId = ItemInfos[i].ColumnId;
            outRow.emplace_back(std::move(inRow[colId]));
            outTypes.emplace_back(ItemInfos[i].SchemeType);
        }

        return CreateRow(outRow, outTypes, holderFactory);
    }
};

///
struct TRangeResultInfo {
    TStructType* ResultType;
    TStructType* RowType;
    TSmallVec<ItemInfo> ItemInfos;
    mutable ui64 Bytes = 0;
    TDefaultListRepresentation Rows;
    TString FirstKey;

    // optimisation: reuse vectors
    TVector<TCell> TmpRow;
    TVector<NScheme::TTypeId> TmpTypes;

    TListType* RowsListType() const { return AS_TYPE(TListType, ResultType->GetMemberType(0)); }
    TDataType* TruncType() const { return AS_TYPE(TDataType, ResultType->GetMemberType(1)); }

    TRangeResultInfo(const TStructLiteral* columnIds, const THashMap<ui32, TSysTables::TTableColumnInfo>& columns,
                     TStructType* returnType)
    {
        ResultType = AS_TYPE(TStructType, returnType);
        Y_VERIFY_DEBUG(ResultType->GetMembersCount() == 2);
        Y_VERIFY_DEBUG(ResultType->GetMemberName(0) == "List");
        Y_VERIFY_DEBUG(ResultType->GetMemberName(1) == "Truncated");

        RowType = AS_TYPE(TStructType, RowsListType()->GetItemType());

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

    static TString Serialize(const TVector<TCell>& row, const TVector<NScheme::TTypeId>& types) {
        Y_VERIFY(row.size() == types.size());

        ui32 count = row.size();
        TString str((const char*)&count, sizeof(ui32));
        str.append((const char*)&types[0], count * sizeof(NScheme::TTypeId));

        TConstArrayRef<TCell> array(&row[0], row.size());
        return str + TSerializedCellVec::Serialize(array);
    }

    void AppendRow(const TVector<TCell>& inRow, const THolderFactory& holderFactory) {
        if (inRow.empty())
            return;

        Y_VERIFY(inRow.size() >= ItemInfos.size());

        TmpRow.clear();
        TmpTypes.clear();
        TmpRow.reserve(ItemInfos.size());
        TmpTypes.reserve(ItemInfos.size());

        for (ui32 i = 0; i < ItemInfos.size(); ++i) {
            ui32 colId = ItemInfos[i].ColumnId;
            TmpRow.push_back(inRow[colId]);
            TmpTypes.push_back(ItemInfos[i].SchemeType);
        }

        Bytes += 8; // per row overhead
        Rows = Rows.Append(CreateRow(TmpRow, TmpTypes, holderFactory));

        if (!FirstKey) {
            FirstKey = Serialize(TmpRow, TmpTypes);
        }
    }

    NUdf::TUnboxedValue CreateResult(const THolderFactory& holderFactory) {
        NUdf::TUnboxedValue* resultItems = nullptr;
        auto result = holderFactory.CreateDirectArrayHolder(4, resultItems);

        resultItems[0] = holderFactory.CreateDirectListHolder(std::move(Rows));
        resultItems[1] = NUdf::TUnboxedValuePod(false);
        resultItems[2] = MakeString(FirstKey);
        resultItems[3] = NUdf::TUnboxedValuePod(Bytes);

        return std::move(result);
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
    TVector<ui32> KeyTypes;
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
class TDataShardEngineHost : public TEngineHost {
public:
    TDataShardEngineHost(TDataShard* self, NTable::TDatabase& db, TEngineHostCounters& counters, ui64& lockTxId, TInstant now)
        : TEngineHost(db, counters,
            TEngineHostSettings(self->TabletID(),
                (self->State == TShardState::Readonly || self->State == TShardState::Frozen),
                self->ByKeyFilterDisabled(),
                self->GetKeyAccessSampler()))
        , Self(self)
        , DB(db)
        , LockTxId(lockTxId)
        , Now(now)
    {}

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

    IChangeCollector* GetChangeCollector(const TTableId& tableId) const override {
        auto it = ChangeCollectors.find(tableId);
        if (it != ChangeCollectors.end()) {
            return it->second.Get();
        }

        it = ChangeCollectors.emplace(tableId, nullptr).first;
        if (!Self->IsUserTable(tableId)) {
            return it->second.Get();
        }

        it->second.Reset(CreateChangeCollector(*Self, DB, tableId.PathId.LocalPathId, IsImmediateTx));
        return it->second.Get();
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

    bool IsValidKey(TKeyDesc& key, std::pair<ui64, ui64>& maxSnapshotTime) const override {
        if (TSysTables::IsSystemTable(key.TableId))
            return DataShardSysTable(key.TableId).IsValidKey(key);

        // prevent updates/erases with LockTxId set
        if (LockTxId && key.RowOperation != TKeyDesc::ERowOperation::Read) {
            key.Status = TKeyDesc::EStatus::OperationNotSupported;
            return false;
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

        Self->SysLocksTable().SetLock(tableId, row, LockTxId);

        Self->SetTableAccessTime(tableId, Now);
        return TEngineHost::SelectRow(tableId, row, columnIds, returnType, readTarget, holderFactory);
    }

    NUdf::TUnboxedValue SelectRange(const TTableId& tableId, const TTableRange& range,
        TStructLiteral* columnIds,  TListLiteral* skipNullKeys, TStructType* returnType,
        const TReadTarget& readTarget, ui64 itemsLimit, ui64 bytesLimit, bool reverse,
        std::pair<const TListLiteral*, const TListLiteral*> forbidNullArgs, const THolderFactory& holderFactory) override
    {
        Y_VERIFY(!TSysTables::IsSystemTable(tableId), "SelectRange no system table is not supported");

        Self->SysLocksTable().SetLock(tableId, range, LockTxId);

        Self->SetTableAccessTime(tableId, Now);
        return TEngineHost::SelectRange(tableId, range, columnIds, skipNullKeys, returnType, readTarget,
            itemsLimit, bytesLimit, reverse, forbidNullArgs, holderFactory);
    }

    void UpdateRow(const TTableId& tableId, const TArrayRef<const TCell>& row, const TArrayRef<const TUpdateCommand>& commands) override {
        if (TSysTables::IsSystemTable(tableId)) {
            DataShardSysTable(tableId).UpdateRow(row, commands);
            return;
        }

        Self->SysLocksTable().BreakLock(tableId, row);
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

        Self->SysLocksTable().BreakLock(tableId, row);

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

private:
    const TDataShardSysTable& DataShardSysTable(const TTableId& tableId) const {
        return static_cast<const TDataShardSysTables *>(Self->GetDataShardSysTables())->Get(tableId);
    }

    TDataShard* Self;
    NTable::TDatabase& DB;
    const ui64& LockTxId;
    bool IsImmediateTx = false;
    TInstant Now;
    TRowVersion WriteVersion = TRowVersion::Max();
    TRowVersion ReadVersion = TRowVersion::Min();
    mutable THashMap<TTableId, THolder<IChangeCollector>> ChangeCollectors;
};

//

TEngineBay::TEngineBay(TDataShard * self, TTransactionContext& txc, const TActorContext& ctx,
                       std::pair<ui64, ui64> stepTxId)
    : StepTxId(stepTxId)
    , LockTxId(0)
{
    auto now = TAppData::TimeProvider->Now();
    EngineHost = MakeHolder<TDataShardEngineHost>(self, txc.DB, EngineHostCounters, LockTxId, now);

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

    KqpAlloc = MakeHolder<TScopedAlloc>(TAlignedPagePoolCounters(), AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
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

void TEngineBay::AddReadRange(const TTableId& tableId, const TVector<NTable::TColumn>& columns, const TTableRange& range,
                              const TVector<NScheme::TTypeId>& keyTypes, ui64 itemsLimit, bool reverse)
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
        "-- AddReadRange: " << DebugPrintRange(keyTypes, range, *AppData()->TypeRegistry));

    auto desc = MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Read, keyTypes, columnOps, itemsLimit,
                                     0 /* bytesLimit */, reverse);
    Info.Keys.emplace_back(TValidatedKey(std::move(desc), /* isWrite */ false));
    // Info.Keys.back().IsResultPart = not a lock key? // TODO: KIKIMR-11134
    ++Info.ReadsCount;
    Info.Loaded = true;
}

void TEngineBay::AddWriteRange(const TTableId& tableId, const TTableRange& range,
                               const TVector<NScheme::TTypeId>& keyTypes)
{
    auto desc = MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Update,
                                     keyTypes, TVector<TKeyDesc::TColumnOp>());
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

TVector<IChangeCollector::TChange> TEngineBay::GetCollectedChanges() const {
    Y_VERIFY(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetCollectedChanges();
}

IEngineFlat * TEngineBay::GetEngine() {
    if (!Engine) {
        Engine = CreateEngineFlat(*EngineSettings);
        Engine->SetStepTxId(StepTxId);
    }

    return Engine.Get();
}

void TEngineBay::SetLockTxId(ui64 lockTxId) {
    LockTxId = lockTxId;
    if (ComputeCtx) {
        ComputeCtx->SetLockTxId(lockTxId);
    }
}

NKqp::TKqpTasksRunner& TEngineBay::GetKqpTasksRunner(const NKikimrTxDataShard::TKqpTransaction& tx) {
    if (!KqpTasksRunner) {
        NYql::NDq::TDqTaskRunnerSettings settings;

        if (tx.HasRuntimeSettings() && tx.GetRuntimeSettings().HasStatsMode()) {
            auto statsMode = tx.GetRuntimeSettings().GetStatsMode();
            settings.CollectBasicStats = statsMode >= NYql::NDqProto::DQ_STATS_MODE_BASIC;
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
