#include "change_collector.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"
#include "datashard__engine_host.h"
#include <ydb/core/tx/locks/sys_tables.h>

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/actors/core/log.h>

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
            Y_ABORT_UNLESS(colInfo && (colInfo->Id == colId), "No column info for column");
            ItemInfos.emplace_back(ItemInfo(colId, colInfo->PType, optType));
        }
    }

    NUdf::TUnboxedValue CreateResult(TVector<TCell>&& inRow, const THolderFactory& holderFactory) const {
        if (inRow.empty()) {
            return NUdf::TUnboxedValuePod();
        }

        Y_ABORT_UNLESS(inRow.size() >= ItemInfos.size());

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
        Y_ABORT_UNLESS(!lock.IsError());

        if (TableId.PathId.LocalPathId == TSysTables::SysTableLocks2) {
            return result.CreateResult(lock.MakeRow(true), holderFactory);
        }
        Y_ABORT_UNLESS(TableId.PathId.LocalPathId == TSysTables::SysTableLocks);
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

        Y_ABORT("unexpected sys table id");
    }
};

TIntrusivePtr<TThrRefBase> InitDataShardSysTables(TDataShard* self) {
    return new TDataShardSysTables(self);
}

///
class TDataShardEngineHost final
    : public TEngineHost
    , public IDataShardUserDb
    , public IDataShardChangeGroupProvider
{
public:
    TDataShardEngineHost(TDataShard* self, TEngineBay& engineBay, NTable::TDatabase& db, ui64 globalTxId, TEngineHostCounters& counters, TInstant now)
        : TEngineHost(db, counters,
            TEngineHostSettings(self->TabletID(),
                (self->State == TShardState::Readonly || self->State == TShardState::Frozen || self->IsReplicated()),
                self->ByKeyFilterDisabled(),
                self->GetKeyAccessSampler()))
        , Self(self)
        , EngineBay(engineBay)
        , UserDb(*self, db, globalTxId, TRowVersion::Min(), TRowVersion::Max(), counters, now)        
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
        return UserDb.SelectRow(tableId, key, tags, row, stats, readVersion);
    }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion) override
    {
        return UserDb.SelectRow(tableId, key, tags, row, readVersion);
    }

    void SetWriteVersion(TRowVersion writeVersion) {
        UserDb.SetWriteVersion(writeVersion);
    }

    TRowVersion GetWriteVersion(const TTableId& tableId) const override {
        Y_UNUSED(tableId);
        Y_ABORT_UNLESS(!UserDb.GetWriteVersion().IsMax(), "Cannot perform writes without WriteVersion set");
        return UserDb.GetWriteVersion();
    }

    void SetReadVersion(TRowVersion readVersion) {
        UserDb.SetReadVersion(readVersion);
    }

    TRowVersion GetReadVersion(const TTableId& tableId) const override {
        Y_UNUSED(tableId);
        Y_ABORT_UNLESS(!UserDb.GetReadVersion().IsMin(), "Cannot perform reads without ReadVersion set");
        return UserDb.GetReadVersion();
    }

    void SetVolatileTxId(ui64 txId) {
        UserDb.SetVolatileTxId(txId);
    }

    void SetIsImmediateTx() {
        UserDb.SetIsImmediateTx(true);
    }

    void SetUsesMvccSnapshot() {
        UserDb.SetUsesMvccSnapshot(true);
    }

    std::optional<ui64> GetCurrentChangeGroup() const override {
        return UserDb.GetCurrentChangeGroup();
    }

    ui64 GetChangeGroup() override {
        return UserDb.GetChangeGroup();
    }

    IDataShardChangeCollector* GetChangeCollector(const TTableId& tableId) const override {
        return UserDb.GetChangeCollector(tableId);
    }

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) override {
        UserDb.CommitChanges(tableId, lockId, writeVersion);
    }

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const {
        return UserDb.GetCollectedChanges();
    }

    void ResetCollectedChanges() {
        UserDb.ResetCollectedChanges();
    }

    TVector<ui64> GetVolatileCommitTxIds() const {
        return UserDb.GetVolatileCommitTxIds();
    }

    const absl::flat_hash_set<ui64>& GetVolatileDependencies() const {
        return UserDb.GetVolatileDependencies();
    }

    std::optional<ui64> GetVolatileChangeGroup() const {
        return UserDb.GetChangeGroup();
    }

    bool GetVolatileCommitOrdered() const {
        return UserDb.GetVolatileCommitOrdered();
    }

    bool GetPerformedUserReads() const {
        return UserDb.GetPerformedUserReads();
    }

    bool IsValidKey(TKeyDesc& key) const override {
        TKeyValidator::TValidateOptions options(
            UserDb.GetLockTxId(),
            UserDb.GetLockNodeId(),
            UserDb.GetUsesMvccSnapshot(),
            UserDb.GetIsImmediateTx(),
            UserDb.GetIsWriteTx(), 
            Scheme
        );
        return GetKeyValidator().IsValidKey(key, options);
    }

    NUdf::TUnboxedValue SelectRow(const TTableId& tableId, const TArrayRef<const TCell>& row,
        TStructLiteral* columnIds, TOptionalType* returnType, const TReadTarget& readTarget,
        const THolderFactory& holderFactory) override
    {
        if (TSysTables::IsSystemTable(tableId)) {
            return DataShardSysTable(tableId).SelectRow(row, columnIds, returnType, readTarget, holderFactory);
        }

        if (UserDb.GetLockTxId()) {
            Self->SysLocksTable().SetLock(tableId, row);
        }

        UserDb.SetPerformedUserReads(true);

        Self->SetTableAccessTime(tableId, UserDb.GetNow());
        return TEngineHost::SelectRow(tableId, row, columnIds, returnType, readTarget, holderFactory);
    }

    NUdf::TUnboxedValue SelectRange(const TTableId& tableId, const TTableRange& range,
        TStructLiteral* columnIds,  TListLiteral* skipNullKeys, TStructType* returnType,
        const TReadTarget& readTarget, ui64 itemsLimit, ui64 bytesLimit, bool reverse,
        std::pair<const TListLiteral*, const TListLiteral*> forbidNullArgs, const THolderFactory& holderFactory) override
    {
        Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId), "SelectRange no system table is not supported");

        if (UserDb.GetLockTxId()) {
            Self->SysLocksTable().SetLock(tableId, range);
        }

        UserDb.SetPerformedUserReads(true);

        Self->SetTableAccessTime(tableId, UserDb.GetNow());
        return TEngineHost::SelectRange(tableId, range, columnIds, skipNullKeys, returnType, readTarget,
            itemsLimit, bytesLimit, reverse, forbidNullArgs, holderFactory);
    }

    void UpdateRow(const TTableId& tableId, const TArrayRef<const TCell>& row, const TArrayRef<const TUpdateCommand>& commands) override {
        if (TSysTables::IsSystemTable(tableId)) {
            DataShardSysTable(tableId).UpdateRow(row, commands);
            return;
        }

        const NTable::TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(LocalTableId(tableId));

        TSmallVec<TRawTypeValue> key;
        ConvertTableKeys(Scheme, tableInfo, row, key, nullptr);

        TSmallVec<NTable::TUpdateOp> ops;
        ConvertTableValues(Scheme, tableInfo, commands, ops, nullptr);

        UserDb.UpsertRow(tableId, key, ops);
    }

    void UpsertRow(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops) override {
        UserDb.UpsertRow(tableId, key, ops);
    }

    void ReplaceRow(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops) override {
        UserDb.ReplaceRow(tableId, key, ops);
    }

    void InsertRow(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops) override {
        UserDb.InsertRow(tableId, key, ops);
    }

    void UpdateRow(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key, const TArrayRef<const NIceDb::TUpdateOp> ops) override {
        UserDb.UpdateRow(tableId, key, ops);
    }

    void EraseRow(const TTableId& tableId, const TArrayRef<const TCell>& row) override {
        if (TSysTables::IsSystemTable(tableId)) {
            DataShardSysTable(tableId).EraseRow(row);
            return;
        }

        const NTable::TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(LocalTableId(tableId));

        TSmallVec<TRawTypeValue> key;
        ConvertTableKeys(Scheme, tableInfo, row, key, nullptr);

        UserDb.EraseRow(tableId, key);
    }

    void EraseRow(const TTableId& tableId, const TArrayRef<const TRawTypeValue> key) override
    {
        UserDb.EraseRow(tableId, key);
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

    ui64 LocalTableId(const TTableId& tableId) const override {
        return Self->GetLocalTableId(tableId);
    }

    ui64 GetTableSchemaVersion(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return 0;
        return GetKeyValidator().GetTableSchemaVersion(tableId);
    }

    ui64 GetWriteTxId(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return 0;

        return UserDb.GetWriteTxId(tableId);
    }

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId)) {
            return nullptr;
        }

        return UserDb.GetReadTxMap(tableId);
    }

    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId& tableId) const override {
        if (TSysTables::IsSystemTable(tableId))
            return nullptr;

        return UserDb.GetReadTxObserver(tableId);
    }

    bool NeedToReadBeforeWrite(const TTableId& tableId) const override {
        return UserDb.NeedToReadBeforeWrite(tableId);
    }

private:
    const TDataShardSysTable& DataShardSysTable(const TTableId& tableId) const {
        return static_cast<const TDataShardSysTables*>(Self->GetDataShardSysTables())->Get(tableId);
    }

    TKeyValidator& GetKeyValidator() {
        return EngineBay.GetKeyValidator();
    }
    const TKeyValidator& GetKeyValidator() const {
        return EngineBay.GetKeyValidator();
    }

public:
    TDataShardUserDb& GetUserDb() {
        return UserDb;
    }
    const TDataShardUserDb& GetUserDb() const {
        return UserDb;
    }

private:
    TDataShard* Self;
    TEngineBay& EngineBay;
    mutable TDataShardUserDb UserDb;
};

//

TEngineBay::TEngineBay(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TStepOrder& stepTxId)
    : StepTxId(stepTxId)
    , KeyValidator(*self)
{
    auto now = TAppData::TimeProvider->Now();
    EngineHost = MakeHolder<TDataShardEngineHost>(self, *this, txc.DB, stepTxId.TxId, EngineHostCounters, now);

    EngineSettings = MakeHolder<TEngineFlatSettings>(IEngineFlat::EProtocol::V1, AppData(ctx)->FunctionRegistry,
        *TAppData::RandomProvider, *TAppData::TimeProvider, EngineHost.Get(), self->AllocCounters);

    auto tabletId = self->TabletID();
    auto txId = stepTxId.TxId;
    const TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
    EngineSettings->LogErrorWriter = [actorSystem, tabletId, txId](const TString& message) {
        LOG_ERROR_S(*actorSystem, NKikimrServices::MINIKQL_ENGINE,
            "Shard %" << tabletId << ", txid %" <<txId << ", engine error: " << message);
    };

    if (ctx.LoggerSettings()->Satisfies(NLog::PRI_DEBUG, NKikimrServices::MINIKQL_ENGINE, txId)) {
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

    ComputeCtx = MakeHolder<TKqpDatashardComputeContext>(self, GetUserDb(), EngineHost->GetSettings().DisableByKeyFilter);
    ComputeCtx->Database = &txc.DB;

    KqpAlloc = std::make_shared<TScopedAlloc>(__LOCATION__, TAlignedPagePoolCounters(), AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
    KqpTypeEnv = MakeHolder<TTypeEnvironment>(*KqpAlloc);
    KqpAlloc->Release();

    auto kqpApplyCtx = MakeHolder<TKqpDatashardApplyContext>();
    kqpApplyCtx->Host = EngineHost.Get();
    kqpApplyCtx->ShardTableStats = &EngineHostCounters;
    kqpApplyCtx->Env = KqpTypeEnv.Get();

    KqpApplyCtx.Reset(kqpApplyCtx.Release());

    KqpExecCtx.FuncRegistry = AppData(ctx)->FunctionRegistry;
    KqpExecCtx.ComputeCtx = ComputeCtx.Get();
    KqpExecCtx.ComputationFactory = GetKqpDatashardComputeFactory(ComputeCtx.Get());
    KqpExecCtx.RandomProvider = TAppData::RandomProvider.Get();
    KqpExecCtx.TimeProvider = TAppData::TimeProvider.Get();
    KqpExecCtx.ApplyCtx = KqpApplyCtx.Get();
    KqpExecCtx.TypeEnv = KqpTypeEnv.Get();
    if (auto rm = NKqp::TryGetKqpResourceManager()) {
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

TEngineBay::TSizes TEngineBay::CalcSizes(bool needsTotalKeysSize) const {
    Y_ABORT_UNLESS(EngineHost);

    const auto& info = KeyValidator.GetInfo();

    TSizes outSizes;
    TVector<const TKeyDesc*> readKeys;
    readKeys.reserve(info.ReadsCount);
    for (const TValidatedKey& validKey : info.Keys) {
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
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetWriteVersion(writeVersion);
}

void TEngineBay::SetReadVersion(TRowVersion readVersion) {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetReadVersion(readVersion);

    Y_ABORT_UNLESS(ComputeCtx);
    ComputeCtx->SetReadVersion(readVersion);
}

void TEngineBay::SetVolatileTxId(ui64 txId) {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetVolatileTxId(txId);
}

void TEngineBay::SetIsImmediateTx() {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetIsImmediateTx();
}

void TEngineBay::SetUsesMvccSnapshot() {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->SetUsesMvccSnapshot();
}

TVector<IDataShardChangeCollector::TChange> TEngineBay::GetCollectedChanges() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetCollectedChanges();
}

void TEngineBay::ResetCollectedChanges() {
    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->ResetCollectedChanges();
}

TVector<ui64> TEngineBay::GetVolatileCommitTxIds() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileCommitTxIds();
}

const absl::flat_hash_set<ui64>& TEngineBay::GetVolatileDependencies() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileDependencies();
}

std::optional<ui64> TEngineBay::GetVolatileChangeGroup() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileChangeGroup();
}

bool TEngineBay::GetVolatileCommitOrdered() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetVolatileCommitOrdered();
}

bool TEngineBay::GetPerformedUserReads() const {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetPerformedUserReads();
}

IEngineFlat * TEngineBay::GetEngine() {
    if (!Engine) {
        Engine = CreateEngineFlat(*EngineSettings);
        Engine->SetStepTxId(StepTxId.ToPair());
    }

    return Engine.Get();
}

TDataShardUserDb& TEngineBay::GetUserDb() {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    return host->GetUserDb();
}
const TDataShardUserDb& TEngineBay::GetUserDb() const {
    Y_ABORT_UNLESS(EngineHost);

    const auto* host = static_cast<const TDataShardEngineHost*>(EngineHost.Get());
    return host->GetUserDb();
}

void TEngineBay::SetLockTxId(ui64 lockTxId, ui32 lockNodeId) {
    Y_ABORT_UNLESS(EngineHost);

    auto* host = static_cast<TDataShardEngineHost*>(EngineHost.Get());
    host->GetUserDb().SetLockTxId(lockTxId);
    host->GetUserDb().SetLockNodeId(lockNodeId);

    if (ComputeCtx) {
        ComputeCtx->SetLockTxId(lockTxId, lockNodeId);
    }
}

NKqp::TKqpTasksRunner& TEngineBay::GetKqpTasksRunner(NKikimrTxDataShard::TKqpTransaction& tx) {
    if (!KqpTasksRunner) {
        NYql::NDq::TDqTaskRunnerSettings settings;

        if (tx.HasRuntimeSettings() && tx.GetRuntimeSettings().HasStatsMode()) {
            settings.StatsMode = tx.GetRuntimeSettings().GetStatsMode();
        } else {
            settings.StatsMode = NYql::NDqProto::DQ_STATS_MODE_NONE;
        }

        settings.OptLLVM = "OFF";
        settings.TerminateOnError = false;
        Y_ABORT_UNLESS(KqpAlloc);
        KqpAlloc->SetLimit(10_MB);
        KqpTasksRunner = NKqp::CreateKqpTasksRunner(std::move(*tx.MutableTasks()), KqpAlloc, KqpExecCtx, settings, KqpLogFunc);
    }

    return *KqpTasksRunner;
}

TKqpDatashardComputeContext& TEngineBay::GetKqpComputeCtx() {
    Y_ABORT_UNLESS(ComputeCtx);
    return *ComputeCtx;
}

} // NDataShard
} // NKikimr
