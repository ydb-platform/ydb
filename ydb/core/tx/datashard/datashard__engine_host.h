#pragma once

#include "defs.h"
#include "change_collector.h"
#include "key_validator.h"
#include "operation.h"

#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/tx/datashard/datashard_kqp_compute.h>

namespace NKikimr {

namespace NTabletFlatExecutor {
    class TTransactionContext;
}

using NTabletFlatExecutor::TTransactionContext;

namespace NDataShard {

class TDataShard;
class TDataShardUserDb;

TIntrusivePtr<TThrRefBase> InitDataShardSysTables(TDataShard* self);

class TLockedWriteLimitException : public yexception {};

///
class TEngineBay : TNonCopyable {
public:
    using TValidationInfo = NMiniKQL::IEngineFlat::TValidationInfo;
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

    TDataShardUserDb& GetUserDb();
    const TDataShardUserDb& GetUserDb() const;

    void SetLockTxId(ui64 lockTxId, ui32 lockNodeId);
    void SetUseLlvmRuntime(bool llvmRuntime) { EngineSettings->LlvmRuntime = llvmRuntime; }

    EResult Validate() {
        if (KeyValidator.GetInfo().Loaded)
            return EResult::Ok;
        Y_ABORT_UNLESS(Engine);
        return Engine->Validate(KeyValidator.GetInfo());
    }

    EResult ReValidateKeys() {
        Y_ABORT_UNLESS(KeyValidator.GetInfo().Loaded);
        Y_ABORT_UNLESS(Engine);
        return Engine->ValidateKeys(KeyValidator.GetInfo());
    }

    void MarkTxLoaded() {
        KeyValidator.GetInfo().SetLoaded();
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
            KqpAlloc.reset();
        }
        KqpExecCtx = {};

        Engine.Reset();
        EngineHost.Reset();
    }

    TKeyValidator& GetKeyValidator() { return KeyValidator; }
    const TKeyValidator& GetKeyValidator() const { return KeyValidator; }
    TValidationInfo& TxInfo() { return KeyValidator.GetInfo(); }
    const TValidationInfo& TxInfo() const { return KeyValidator.GetInfo(); }
    TEngineBay::TSizes CalcSizes(bool needsTotalKeysSize) const;

    void SetWriteVersion(TRowVersion writeVersion);
    void SetReadVersion(TRowVersion readVersion);
    void SetVolatileTxId(ui64 txId);
    void SetIsImmediateTx();
    void SetUsesMvccSnapshot();

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;
    void ResetCollectedChanges();

    TVector<ui64> GetVolatileCommitTxIds() const;
    const absl::flat_hash_set<ui64>& GetVolatileDependencies() const;
    std::optional<ui64> GetVolatileChangeGroup() const;
    bool GetVolatileCommitOrdered() const;
    bool GetPerformedUserReads() const;

    void ResetCounters() { EngineHostCounters = TEngineHostCounters(); }
    const TEngineHostCounters& GetCounters() const { return EngineHostCounters; }

    NKqp::TKqpTasksRunner& GetKqpTasksRunner(NKikimrTxDataShard::TKqpTransaction& tx);
    NMiniKQL::TKqpDatashardComputeContext& GetKqpComputeCtx();

private:
    TStepOrder StepTxId;
    THolder<NMiniKQL::TEngineHost> EngineHost;
    THolder<NMiniKQL::TEngineFlatSettings> EngineSettings;
    THolder<NMiniKQL::IEngineFlat> Engine;
    TKeyValidator KeyValidator;
    TEngineHostCounters EngineHostCounters;
    NYql::NDq::TLogFunc KqpLogFunc;
    THolder<NUdf::IApplyContext> KqpApplyCtx;
    THolder<NMiniKQL::TKqpDatashardComputeContext> ComputeCtx;
    std::shared_ptr<NMiniKQL::TScopedAlloc> KqpAlloc;
    THolder<NMiniKQL::TTypeEnvironment> KqpTypeEnv;
    NYql::NDq::TDqTaskRunnerContext KqpExecCtx;
    TIntrusivePtr<NKqp::TKqpTasksRunner> KqpTasksRunner;
};

}}
