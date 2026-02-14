#pragma once

#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/engine/minikql/minikql_engine_host_counters.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>

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

    void SetMvccVersion(TRowVersion readVersion);
    TRowVersion GetMvccVersion() const;

    TEngineHostCounters& GetTaskCounters(ui64 taskId) { return TaskCounters[taskId]; }
    TEngineHostCounters& GetDatashardCounters();

    bool IsTabletNotReady() const { return TabletNotReady; }

    bool HadInconsistentReads() const { return InconsistentReads; }
    void SetInconsistentReads() { InconsistentReads = true; }

    bool HasVolatileReadDependencies() const;
    const absl::flat_hash_set<ui64>& GetVolatileReadDependencies() const;
private:
    void TouchTableRange(const TTableId& tableId, const TTableRange& range) const;
    void TouchTablePoint(const TTableId& tableId, const TArrayRef<const TCell>& key) const;

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

IComputationNode* WrapKqpUpsertRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx, const TString& userSID);
IComputationNode* WrapKqpDeleteRows(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx, const TString& userSID);
IComputationNode* WrapKqpEffects(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx, const TString& userSID);

TComputationNodeFactory GetKqpDatashardComputeFactory(TKqpDatashardComputeContext* computeCtx, const TString& userSID);

} // namespace NMiniKQL
} // namespace NKikimr
