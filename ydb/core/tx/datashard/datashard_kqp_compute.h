#pragma once

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>

namespace NKikimr {
    namespace NDataShard {
        class TExecuteKqpScanTxUnit;
        class TDataShard;
        class TDataShardUserDb;
    }
}

namespace NKikimr {
namespace NMiniKQL {

class IEngineFlatHost;

using TKqpTableStats = TEngineHostCounters;

class TKqpDatashardComputeContext : public TKqpComputeContextBase {
public:
    TKqpDatashardComputeContext(NDataShard::TDataShard* shard, NDataShard::TDataShardUserDb& userDb, bool disableByKeyFilter);

    ui64 GetLocalTableId(const TTableId& tableId) const;
    TString GetTablePath(const TTableId& tableId) const;
    const NDataShard::TUserTable* GetTable(const TTableId& tableId) const;
    void BreakSetLocks() const;
    void SetLockTxId(ui64 lockTxId, ui32 lockNodeId);

    const NDataShard::TUserTable::TUserColumn& GetKeyColumnInfo(
        const NDataShard::TUserTable& table, ui32 keyIndex) const;
    THashMap<TString, NScheme::TTypeInfo> GetKeyColumnsMap(const TTableId &tableId) const;

    void SetHasPersistentChannels(bool value) { PersistentChannels = value; }
    bool HasPersistentChannels() const { return PersistentChannels; }

    void SetTaskOutputChannel(ui64 taskId, ui64 channelId, TActorId actorId);
    TActorId GetTaskOutputChannel(ui64 taskId, ui64 channelId) const;

    bool PinPages(const TVector<IEngineFlat::TValidatedKey>& keys, ui64 pageFaultCount = 0);

    void Clear();

    void SetReadVersion(TRowVersion readVersion);
    TRowVersion GetReadVersion() const;

    TEngineHostCounters& GetTaskCounters(ui64 taskId) { return TaskCounters[taskId]; }
    TEngineHostCounters& GetDatashardCounters();

    bool IsTabletNotReady() const { return TabletNotReady; }

    bool ReadRow(const TTableId& tableId, TArrayRef<const TCell> key, const TSmallVec<NTable::TTag>& columnTags,
        const TSmallVec<NTable::TTag>& systemColumnTags, const THolderFactory& holderFactory,
        NUdf::TUnboxedValue& result, TKqpTableStats& stats);

    TAutoPtr<NTable::TTableIter> CreateIterator(const TTableId& tableId, const TTableRange& range,
        const TSmallVec<NTable::TTag>& columnTags);

    TAutoPtr<NTable::TTableReverseIter> CreateReverseIterator(const TTableId& tableId, const TTableRange& range,
        const TSmallVec<NTable::TTag>& columnTags);

    bool ReadRow(const TTableId& tableId, NTable::TTableIter& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        const THolderFactory& holderFactory, NUdf::TUnboxedValue& result, TKqpTableStats& stats);

    bool ReadRow(const TTableId& tableId, NTable::TTableReverseIter& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        const THolderFactory& holderFactory, NUdf::TUnboxedValue& result, TKqpTableStats& stats);

    bool ReadRowWide(const TTableId& tableId, NTable::TTableIter& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        NUdf::TUnboxedValue* const* result, TKqpTableStats& stats);

    bool ReadRowWide(const TTableId& tableId, NTable::TTableReverseIter& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        NUdf::TUnboxedValue* const* result, TKqpTableStats& stats);

    bool HadInconsistentReads() const { return InconsistentReads; }
    void SetInconsistentReads() { InconsistentReads = true; }

    bool HasVolatileReadDependencies() const;
    const absl::flat_hash_set<ui64>& GetVolatileReadDependencies() const;
private:
    void TouchTableRange(const TTableId& tableId, const TTableRange& range) const;
    void TouchTablePoint(const TTableId& tableId, const TArrayRef<const TCell>& key) const;

    template <typename TReadTableIterator>
    bool ReadRowImpl(const TTableId& tableId, TReadTableIterator& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        const THolderFactory& holderFactory, NUdf::TUnboxedValue& result, TKqpTableStats& stats);

    template <typename TReadTableIterator>
    bool ReadRowWideImpl(const TTableId& tableId, TReadTableIterator& iterator,
        const TSmallVec<NTable::TTag>& systemColumnTags, const TSmallVec<bool>& skipNullKeys,
        NUdf::TUnboxedValue* const* result, TKqpTableStats& stats);

    void SetTabletNotReady() { TabletNotReady = true; }

public:
    NTable::TDatabase* Database = nullptr;

private:
    NDataShard::TDataShard* Shard;
    std::unordered_map<ui64, TEngineHostCounters> TaskCounters;
    NDataShard::TDataShardUserDb& UserDb;
    bool DisableByKeyFilter;
    bool PersistentChannels = false;
    bool TabletNotReady = false;
    bool InconsistentReads = false;
    THashMap<std::pair<ui64, ui64>, TActorId> OutputChannels;
};

class TKqpDatashardApplyContext : public NUdf::IApplyContext {
public:
    IEngineFlatHost* Host = nullptr;
    TKqpTableStats* ShardTableStats = nullptr;
    TKqpTableStats* TaskTableStats = nullptr;
    TTypeEnvironment* Env = nullptr;
};

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
