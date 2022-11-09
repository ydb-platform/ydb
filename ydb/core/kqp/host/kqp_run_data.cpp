#include "kqp_host_impl.h"
#include "kqp_run_physical.h"

#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NThreading;

class TKqpExecutePhysicalDataTransformer : public TKqpExecutePhysicalTransformerBase {
public:
    TKqpExecutePhysicalDataTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : TKqpExecutePhysicalTransformerBase(gateway, cluster, txState, transformCtx) {}

protected:
    TStatus DoExecute(std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit, NYql::TExprContext& ctx) final {
        auto& txState = TxState->Tx();
        auto request = PrepareRequest();

        if (!request.Timeout) {
            ctx.AddError(YqlIssue({}, TIssuesIds::KIKIMR_TIMEOUT, "Query request timeout."));
            return TStatus::Error;
        }

        if (request.CancelAfter && !*request.CancelAfter) {
            ctx.AddError(YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_CANCELLED, "Query cancelled by timeout."));
            return TStatus::Error;
        }

        if (tx) {
            switch (tx->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected physical tx type in data query: " << (ui32)tx->GetType());
            }

            request.Transactions.emplace_back(tx, PrepareParameters(*tx));
        } else {
            YQL_ENSURE(commit);
            if (txState.DeferredEffects.Empty() && !txState.Locks.HasLocks()) {
                return TStatus::Ok;
            }
        }

        if (commit) {
            Y_VERIFY_DEBUG(txState.DeferredEffects.Empty() || !txState.Locks.Broken());

            for (const auto& effect : txState.DeferredEffects) {
                YQL_ENSURE(effect.PhysicalTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA);
                request.Transactions.emplace_back(effect.PhysicalTx, GetParamsRefMap(effect.Params));
            }

            if (!txState.DeferredEffects.Empty()) {
                request.PerShardKeysSizeLimitBytes = TransformCtx->Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
            }

            if (txState.Locks.HasLocks()) {
                request.ValidateLocks = !(txState.GetSnapshot().IsValid() && txState.DeferredEffects.Empty());
                request.EraseLocks = true;

                for (auto& [lockId, lock] : txState.Locks.LocksMap) {
                    request.Locks.emplace_back(lock.GetValueRef(txState.Locks.LockType));
                }
            }
        } else if (ShouldAcquireLocks()) {
            request.AcquireLocksTxId = txState.Locks.GetLockTxId();
        }

        ExecuteFuture = Gateway->ExecutePhysical(std::move(request), {});
        return TStatus::Async;
    }

    TStatus DoRollback() final {
        auto& txState = TxState->Tx();

        YQL_CLOG(DEBUG, ProviderKqp) << "TKqpExecutePhysicalDataTransformer::DoRollback()"
            << ", effects: " << txState.DeferredEffects.Size() << ", locks: " << txState.Locks.Size()
            << ", timeout: " << TransformCtx->QueryCtx->Deadlines.TimeoutAt;

        if (!txState.Locks.HasLocks()) {
            ClearTx();
            return TStatus::Ok;
        }

        auto request = PrepareRequest();

        request.ValidateLocks = false;
        request.EraseLocks = true;

        for (auto& [key, lock] : txState.Locks.LocksMap) {
            request.Locks.emplace_back(lock.GetValueRef(txState.Locks.LockType));
        }

        ExecuteFuture = Gateway->ExecutePhysical(std::move(request), {});
        return TStatus::Async;
    }

    bool OnExecuterResult(NKikimrKqp::TExecuterTxResult&& execResult, NLongTxService::TLockHandle&& lockHandle, TExprContext& ctx, bool commit) override {
        if (lockHandle) {
            TxState->Tx().Locks.LockHandle = std::move(lockHandle);
        }

        if (execResult.HasLocks()) {
            YQL_ENSURE(!commit);

            if (!MergeLocks(execResult.GetLocks().GetType(), execResult.GetLocks().GetValue(), TxState->Tx(), ctx)) {
                return false;
            }
        }

        if (execResult.HasStats()) {
            TransformCtx->QueryStats.AddExecutions()->Swap(execResult.MutableStats());
        }

        if (commit) {
            ClearTx();
        }

        return true;
    }

private:
    IKqpGateway::TExecPhysicalRequest PrepareRequest() {
        IKqpGateway::TExecPhysicalRequest request;
        auto now = Gateway->GetCurrentTime();

        request.Timeout = TransformCtx->QueryCtx->Deadlines.TimeoutAt - now;
        if (auto cancelAt = TransformCtx->QueryCtx->Deadlines.CancelAt) {
            request.CancelAfter = cancelAt - now;
        }
        request.StatsMode = GetStatsMode(TransformCtx->QueryCtx->StatsMode);
        request.MaxAffectedShards = TransformCtx->QueryCtx->Limits.PhaseLimits.AffectedShardsLimit;
        request.TotalReadSizeLimitBytes = TransformCtx->QueryCtx->Limits.PhaseLimits.TotalReadSizeLimitBytes;
        request.MkqlMemoryLimit = TransformCtx->QueryCtx->Limits.PhaseLimits.ComputeNodeMemoryLimitBytes;
        request.Snapshot = TxState->Tx().GetSnapshot();
        request.IsolationLevel = *TxState->Tx().EffectiveIsolationLevel;

        return request;
    }

    bool ShouldAcquireLocks() {
        if (*TxState->Tx().EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
            return false;
        }

        if (TxState->Tx().Locks.Broken()) {
            return false;  // Do not acquire locks after first lock issue
        }

        if (!TxState->Tx().DeferredEffects.Empty()) {
            return true; // Acquire locks in read write tx
        }

        for (auto& tx : TransformCtx->PhysicalQuery->GetTransactions()) {
            if (tx.GetHasEffects()) {
                return true; // Acquire locks in read write tx
            }
        }

        if (!TransformCtx->Settings.GetCommitTx()) {
            return true; // Is not a commit tx
        }

        if (TxState->Tx().GetSnapshot().IsValid()) {
            return false; // It is a read only tx with snapshot, no need to acquire locks
        }

        return true;
    }

    TKqpParamsMap GetParamsRefMap(const TParamValueMap& map) {
        TKqpParamsMap paramsMap(TransformState);
        for (auto& pair : map) {
            auto valueRef = NYql::NDq::TMkqlValueRef(pair.second);
            YQL_ENSURE(paramsMap.Values.emplace(std::make_pair(pair.first, valueRef)).second);
        }

        return paramsMap;
    }
};

TAutoPtr<IGraphTransformer> CreateKqpExecutePhysicalDataTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpExecutePhysicalDataTransformer(gateway, cluster, txState, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
