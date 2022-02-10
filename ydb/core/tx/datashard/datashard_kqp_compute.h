#pragma once

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
    namespace NDataShard {
        class TExecuteKqpScanTxUnit;
        class TDataShard;
        struct TUserTable;
    }
}

namespace NKikimr {
namespace NMiniKQL {

class IEngineFlatHost;

using TKqpTableStats = TEngineHostCounters;

class TKqpDatashardComputeContext : public TKqpComputeContextBase {
public:
    TKqpDatashardComputeContext(NDataShard::TDataShard* shard, TEngineHostCounters& counters, TInstant now)
        : Shard(shard)
        , DatashardCounters(counters)
        , Now(now) {}

    ui64 GetLocalTableId(const TTableId& tableId) const;
    TString GetTablePath(const TTableId& tableId) const;
    const NDataShard::TUserTable* GetTable(const TTableId& tableId) const;
    void ReadTable(const TTableId& tableId, const TTableRange& range) const;
    void ReadTable(const TTableId& tableId, const TArrayRef<const TCell>& key) const;
    void BreakSetLocks() const;
    void SetLockTxId(ui64 lockTxId);
    ui64 GetShardId() const; 

    TVector<std::pair<NScheme::TTypeId, TString>> GetKeyColumnsInfo(const TTableId &tableId) const;
    THashMap<TString, NScheme::TTypeId> GetKeyColumnsMap(const TTableId &tableId) const;

    void SetHasPersistentChannels(bool value) { PersistentChannels = value; }
    bool HasPersistentChannels() const { return PersistentChannels; }

    void SetTaskOutputChannel(ui64 taskId, ui64 channelId, TActorId actorId);
    TActorId GetTaskOutputChannel(ui64 taskId, ui64 channelId) const;

    bool PinPages(const TVector<IEngineFlat::TValidatedKey>& keys, ui64 pageFaultCount = 0);

    void Clear();

    void SetReadVersion(TRowVersion readVersion);
    TRowVersion GetReadVersion() const;

    TEngineHostCounters& GetTaskCounters(ui64 taskId) { return TaskCounters[taskId]; }
    TEngineHostCounters& GetDatashardCounters() { return DatashardCounters; }

    void SetTabletNotReady() { Y_VERIFY_DEBUG(!TabletNotReady); TabletNotReady = true; };
    bool IsTabletNotReady() const { return TabletNotReady; }

public:
    NTable::TDatabase* Database = nullptr;

private:
    NDataShard::TDataShard* Shard;
    std::unordered_map<ui64, TEngineHostCounters> TaskCounters;
    TEngineHostCounters& DatashardCounters;
    TInstant Now;
    ui64 LockTxId = 0;
    bool PersistentChannels = false;
    bool TabletNotReady = false;
    TRowVersion ReadVersion = TRowVersion::Min();
    THashMap<std::pair<ui64, ui64>, TActorId> OutputChannels;
};

class TKqpDatashardApplyContext : public NUdf::IApplyContext {
public:
    IEngineFlatHost* Host = nullptr;
};

TSmallVec<NTable::TTag> ExtractTags(const TSmallVec<TKqpComputeContextBase::TColumn>& columns);

bool TryFetchRow(NTable::TTableIt& iterator, NYql::NUdf::TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, const TKqpDatashardComputeContext& computeCtx,
    const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys);

bool TryFetchRow(NTable::TTableReverseIt& iterator, NYql::NUdf::TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, const TKqpDatashardComputeContext& computeCtx,
    const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys);

void FetchRow(const TDbTupleRef& dbTuple, NYql::NUdf::TUnboxedValue& row, TComputationContext& ctx,
    TKqpTableStats& tableStats, const TKqpDatashardComputeContext& computeCtx,
    const TSmallVec<NTable::TTag>& systemColumnTags);

IComputationNode* WrapKqpWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);
IComputationNode* WrapKqpLookupTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);
IComputationNode* WrapKqpUpsertRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);
IComputationNode* WrapKqpDeleteRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);
IComputationNode* WrapKqpEffects(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);
IComputationNode* WrapKqpWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx);

TComputationNodeFactory GetKqpDatashardComputeFactory(TKqpDatashardComputeContext* computeCtx);
TComputationNodeFactory GetKqpScanComputeFactory(TKqpScanComputeContext* computeCtx);

} // namespace NMiniKQL
} // namespace NKikimr
