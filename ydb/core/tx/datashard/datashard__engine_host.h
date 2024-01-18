#pragma once

#include "defs.h"
#include "change_collector.h"

#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/tx/datashard/datashard_kqp_compute.h>
#include "ydb/core/tx/datashard/operation.h"

namespace NKikimr {

namespace NTabletFlatExecutor {
    class TTransactionContext;
}

using NTabletFlatExecutor::TTransactionContext;

namespace NDataShard {

class TDataShard;

TIntrusivePtr<TThrRefBase> InitDataShardSysTables(TDataShard* self);

class TLockedWriteLimitException : public yexception {};

///
class TEngineBay : TNonCopyable {
public:
    using TValidatedKey = NMiniKQL::IEngineFlat::TValidatedKey;
    using EResult = NMiniKQL::IEngineFlat::EResult;
    using TEngineHostCounters = NMiniKQL::TEngineHostCounters;

    struct TSizes {
        ui64 ReadSize = 0;
        ui64 ReplySize = 0;
        THashMap<ui64, ui64> OutReadSetSize;
        ui64 TotalKeysSize = 0;
    };

    TEngineBay(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TStepOrder& stepTxId);

    virtual ~TEngineBay();

    const NMiniKQL::IEngineFlat * GetEngine() const { return Engine.Get(); }
    NMiniKQL::IEngineFlat * GetEngine();
    NMiniKQL::TEngineHost * GetEngineHost() { return EngineHost.Get(); }
    void SetLockTxId(ui64 lockTxId, ui32 lockNodeId);
    void SetUseLlvmRuntime(bool llvmRuntime) { EngineSettings->LlvmRuntime = llvmRuntime; }

    EResult ExtractKeys(NMiniKQL::IEngineFlat::TValidationInfo& validationInfo) {
        if (validationInfo.Loaded)
            return EResult::Ok;
        Y_ABORT_UNLESS(Engine);
        return Engine->ExtractKeys(validationInfo);
    }

    EResult ValidateKeys(const NMiniKQL::IEngineFlat::TValidationInfo& validationInfo) {
        Y_ABORT_UNLESS(validationInfo.Loaded);
        Y_ABORT_UNLESS(Engine);
        return Engine->ValidateKeys(validationInfo);
    }

    /// @note it expects TValidationInfo keys are materialized outsize of engine's allocs
    void DestroyEngine() {
        ComputeCtx->Clear();
        if (KqpTasksRunner) {
            KqpTasksRunner.Reset();
            {
                auto guard = TGuard(*KqpAlloc);
                KqpTypeEnv.Reset();
            }
            KqpAlloc.Reset();
        }
        KqpExecCtx = {};

        Engine.Reset();
        EngineHost.Reset();
    }

    TEngineBay::TSizes CalcSizes(const NMiniKQL::IEngineFlat::TValidationInfo& validationInfo, bool needsTotalKeysSize) const;

    void SetWriteVersion(TRowVersion writeVersion);
    void SetReadVersion(TRowVersion readVersion);
    void SetVolatileTxId(ui64 txId);
    void SetIsImmediateTx();
    void SetIsRepeatableSnapshot();

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion);

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();

    TVector<ui64> GetVolatileCommitTxIds() const;
    const absl::flat_hash_set<ui64>& GetVolatileDependencies() const;
    std::optional<ui64> GetVolatileChangeGroup() const;
    bool GetVolatileCommitOrdered() const;

    void ResetCounters() { EngineHostCounters = TEngineHostCounters(); }
    const TEngineHostCounters& GetCounters() const { return EngineHostCounters; }

    NKqp::TKqpTasksRunner& GetKqpTasksRunner(NKikimrTxDataShard::TKqpTransaction& tx);
    NMiniKQL::TKqpDatashardComputeContext& GetKqpComputeCtx();

private:
    THolder<NMiniKQL::TEngineHost> EngineHost;
    THolder<NMiniKQL::TEngineFlatSettings> EngineSettings;
    THolder<NMiniKQL::IEngineFlat> Engine;
    TEngineHostCounters EngineHostCounters;
    NYql::NDq::TLogFunc KqpLogFunc;
    THolder<NUdf::IApplyContext> KqpApplyCtx;
    THolder<NMiniKQL::TKqpDatashardComputeContext> ComputeCtx;
    THolder<NMiniKQL::TScopedAlloc> KqpAlloc;
    THolder<NMiniKQL::TTypeEnvironment> KqpTypeEnv;
    NYql::NDq::TDqTaskRunnerContext KqpExecCtx;
    TIntrusivePtr<NKqp::TKqpTasksRunner> KqpTasksRunner;
};

}}
