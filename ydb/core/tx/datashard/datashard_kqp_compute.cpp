#include "datashard_kqp_compute.h"
#include "range_ops.h"
#include "datashard_user_db.h"

#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

typedef IComputationNode* (*TCallableDatashardBuilderFunc)(TCallable& callable,
    const TComputationNodeFactoryContext& ctx, TKqpDatashardComputeContext& computeCtx, const TString& userSID);

struct TKqpDatashardComputationMap {
    TKqpDatashardComputationMap() {
        Map["KqpUpsertRows"] = &WrapKqpUpsertRows;
        Map["KqpDeleteRows"] = &WrapKqpDeleteRows;
        Map["KqpEffects"] = &WrapKqpEffects;
    }

    THashMap<TString, TCallableDatashardBuilderFunc> Map;
};

TComputationNodeFactory GetKqpDatashardComputeFactory(TKqpDatashardComputeContext* computeCtx, const TString& userSID) {
    MKQL_ENSURE_S(computeCtx);
    MKQL_ENSURE_S(computeCtx->Database);

    auto computeFactory = GetKqpBaseComputeFactory(computeCtx);

    return [computeFactory, computeCtx, userSID]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (auto compute = computeFactory(callable, ctx)) {
                return compute;
            }

            const auto& datashardMap = Singleton<TKqpDatashardComputationMap>()->Map;
            auto it = datashardMap.find(callable.GetType()->GetName());
            if (it != datashardMap.end()) {
                return it->second(callable, ctx, *computeCtx, userSID);
            }

            return nullptr;
        };
};

TKqpDatashardComputeContext::TKqpDatashardComputeContext(NDataShard::TDataShard* shard, NDataShard::TDataShardUserDb& userDb, bool disableByKeyFilter)
    : Shard(shard)
    , UserDb(userDb)
    , DisableByKeyFilter(disableByKeyFilter)
{
}

ui64 TKqpDatashardComputeContext::GetLocalTableId(const TTableId &tableId) const {
    MKQL_ENSURE_S(Shard);
    return Shard->GetLocalTableId(tableId);
}

const NDataShard::TUserTable::TUserColumn& TKqpDatashardComputeContext::GetKeyColumnInfo(
    const NDataShard::TUserTable& table, ui32 keyIndex) const
{
    MKQL_ENSURE_S(keyIndex <= table.KeyColumnTypes.size());
    const auto& col = table.Columns.at(table.KeyColumnIds[keyIndex]);
    MKQL_ENSURE_S(col.IsKey);

    return col;
}

THashMap<TString, NScheme::TTypeInfo> TKqpDatashardComputeContext::GetKeyColumnsMap(const TTableId &tableId) const {
    MKQL_ENSURE_S(Shard);
    const NDataShard::TUserTable::TCPtr* tablePtr = Shard->GetUserTables().FindPtr(tableId.PathId.LocalPathId);
    MKQL_ENSURE_S(tablePtr);
    const NDataShard::TUserTable::TCPtr table = *tablePtr;
    MKQL_ENSURE_S(table);

    THashMap<TString, NScheme::TTypeInfo> columnsMap;
    for (size_t i = 0 ; i < table->KeyColumnTypes.size(); i++) {
        auto col = table->Columns.at(table->KeyColumnIds[i]);
        MKQL_ENSURE_S(col.IsKey);
        columnsMap[col.Name] = col.Type;

    }

    return columnsMap;
}

TString TKqpDatashardComputeContext::GetTablePath(const TTableId &tableId) const {
    MKQL_ENSURE_S(Shard);

    auto table = Shard->GetUserTables().FindPtr(tableId.PathId.LocalPathId);
    if (!table) {
        return TStringBuilder() << tableId;
    }

    return (*table)->Path;
}

const NDataShard::TUserTable* TKqpDatashardComputeContext::GetTable(const TTableId& tableId) const {
    MKQL_ENSURE_S(Shard);
    auto ptr = Shard->GetUserTables().FindPtr(tableId.PathId.LocalPathId);
    MKQL_ENSURE_S(ptr);
    return ptr->Get();
}

void TKqpDatashardComputeContext::TouchTableRange(const TTableId& tableId, const TTableRange& range) const {
    if (UserDb.GetLockTxId()) {
        Shard->SysLocksTable().SetLock(tableId, range);
    }
    UserDb.SetPerformedUserReads(true);
    Shard->SetTableAccessTime(tableId, UserDb.GetNow());
}

void TKqpDatashardComputeContext::TouchTablePoint(const TTableId& tableId, const TArrayRef<const TCell>& key) const {
    if (UserDb.GetLockTxId()) {
        Shard->SysLocksTable().SetLock(tableId, key);
    }
    UserDb.SetPerformedUserReads(true);
    Shard->SetTableAccessTime(tableId, UserDb.GetNow());
}

void TKqpDatashardComputeContext::BreakSetLocks() const {
    if (UserDb.GetLockTxId()) {
        Shard->SysLocksTable().BreakSetLocks();
    }
}

void TKqpDatashardComputeContext::SetLockTxId(ui64 lockTxId, ui32 lockNodeId) {
    UserDb.SetLockTxId(lockTxId);
    UserDb.SetLockNodeId(lockNodeId);
}

void TKqpDatashardComputeContext::SetMvccVersion(TRowVersion mvccVersion) {
    UserDb.SetMvccVersion(mvccVersion);
}

TRowVersion TKqpDatashardComputeContext::GetMvccVersion() const {
    Y_ENSURE(!UserDb.GetMvccVersion().IsMin(), "Cannot perform reads without ReadVersion set");

    return UserDb.GetMvccVersion();
}

TEngineHostCounters& TKqpDatashardComputeContext::GetDatashardCounters() {
    return UserDb.GetCounters();
}

void TKqpDatashardComputeContext::SetTaskOutputChannel(ui64 taskId, ui64 channelId, TActorId actorId) {
    OutputChannels.emplace(std::make_pair(taskId, channelId), actorId);
}

TActorId TKqpDatashardComputeContext::GetTaskOutputChannel(ui64 taskId, ui64 channelId) const {
    auto it = OutputChannels.find(std::make_pair(taskId, channelId));
    if (it != OutputChannels.end()) {
        return it->second;
    }
    return TActorId();
}

void TKqpDatashardComputeContext::Clear() {
    Database = nullptr;
    SetLockTxId(0, 0);
}

bool TKqpDatashardComputeContext::PinPages(const TVector<IEngineFlat::TValidatedKey>& keys, ui64 pageFaultCount) {
    ui64 limitMultiplier = 1;
    if (pageFaultCount >= 2) {
        if (pageFaultCount <= 63) {
            limitMultiplier <<= pageFaultCount - 1;
        } else {
            limitMultiplier = Max<ui64>();
        }
    }

    auto adjustLimit = [limitMultiplier](ui64 limit) -> ui64 {
        if (limit >= Max<ui64>() / limitMultiplier) {
            return Max<ui64>();
        } else {
            return limit * limitMultiplier;
        }
    };

    bool ret = true;
    auto& scheme = Database->GetScheme();

    for (const auto& vKey : keys) {
        const TKeyDesc& key = *vKey.Key;

        if (TSysTables::IsSystemTable(key.TableId)) {
            continue;
        }

        TSet<TKeyDesc::EColumnOperation> columnOpFilter;
        switch (key.RowOperation) {
            case TKeyDesc::ERowOperation::Read:
                columnOpFilter.insert(TKeyDesc::EColumnOperation::Read);
                break;
            case TKeyDesc::ERowOperation::Update:
            case TKeyDesc::ERowOperation::Erase: {
                if (UserDb.NeedToReadBeforeWrite(key.TableId)) {
                    columnOpFilter.insert(TKeyDesc::EColumnOperation::Set);
                    columnOpFilter.insert(TKeyDesc::EColumnOperation::InplaceUpdate);
                }
                break;
            }
            default:
                break;
        }

        if (columnOpFilter.empty()) {
            continue;
        }

        ui64 localTid = GetLocalTableId(key.TableId);
        Y_ENSURE(localTid, "table not exist");

        auto* tableInfo = scheme.GetTableInfo(localTid);
        TSmallVec<TRawTypeValue> from;
        TSmallVec<TRawTypeValue> to;
        ConvertTableKeys(scheme, tableInfo, key.Range.From, from, nullptr);
        if (!key.Range.Point) {
            ConvertTableKeys(scheme, tableInfo, key.Range.To, to, nullptr);
        }

        TSmallVec<NTable::TTag> columnTags;
        for (const auto& column : key.Columns) {
            if (columnOpFilter.contains(column.Operation)) {
                columnTags.push_back(column.Column);
            }
        }

        bool ready = Database->Precharge(localTid,
                                         from,
                                         key.Range.Point ? from : to,
                                         columnTags,
                                         DisableByKeyFilter ? (ui64)NTable::NoByKey : 0,
                                         adjustLimit(key.RangeLimits.ItemsLimit),
                                         adjustLimit(key.RangeLimits.BytesLimit),
                                         key.Reverse ? NTable::EDirection::Reverse : NTable::EDirection::Forward,
                                         GetMvccVersion()).Ready;

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Run precharge on table " << tableInfo->Name
            << ", columns: [" << JoinSeq(", ", columnTags) << "]"
            << ", range: " << DebugPrintRange(key.KeyColumnTypes, key.Range, *AppData()->TypeRegistry)
            << ", itemsLimit: " << key.RangeLimits.ItemsLimit
            << ", bytesLimit: " << key.RangeLimits.BytesLimit
            << ", reverse: " << key.Reverse
            << ", result: " << ready);

        ret &= ready;
    }

    return ret;
}

bool TKqpDatashardComputeContext::HasVolatileReadDependencies() const {
    return !UserDb.GetVolatileReadDependencies().empty();
}
const absl::flat_hash_set<ui64>& TKqpDatashardComputeContext::GetVolatileReadDependencies() const {
    return UserDb.GetVolatileReadDependencies();
}

} // namespace NMiniKQL
} // namespace NKikimr
