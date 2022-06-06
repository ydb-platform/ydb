#include "kqp_host_impl.h"

#include <ydb/library/yql/utils/log/log.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

class TKqpExecutePreparedTransformer : public TGraphTransformerBase {
public:
    TKqpExecutePreparedTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TxState(txState)
        , TransformCtx(transformCtx)
        , CurrentMkqlIndex(0)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        const auto& kql = TransformCtx->GetPreparedKql();

        // TODO: Enable after switch to NewEngine
        // YQL_ENSURE(!TransformCtx->Config->HasKqpForceNewEngine());

        if (CurrentMkqlIndex >= kql.MkqlsSize()) {
            return Finish(ctx);
        }

        const auto& mkql = kql.GetMkqls(CurrentMkqlIndex);

        AcquireLocks = ShouldAcquireLocks(kql);

        Promise = NewPromise();

        if (!Execute(mkql, MkqlExecuteResult.Future)) {
            return TStatus::Error;
        }

        auto promise = Promise;
        MkqlExecuteResult.Future.Apply([promise](const TFuture<IKqpGateway::TMkqlResult> future) mutable {
            YQL_ENSURE(future.HasValue());
            promise.SetValue();
        });

        return TStatus::Async;
    }

    TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        NKikimr::NKqp::IKqpGateway::TMkqlResult result(MkqlExecuteResult.Future.ExtractValue());
        result.ReportIssues(ctx.IssueManager);
        if (!result.Success()) {
            return TStatus::Error;
        }

        const auto& mkql = TransformCtx->GetPreparedKql().GetMkqls(CurrentMkqlIndex);
        if (mkql.GetIsPure()) {
            Y_VERIFY_DEBUG(result.TxStats.TableAccessStatsSize() == 0);
        }

        auto mkqlResult = MakeSimpleShared<NKikimrMiniKQL::TResult>();
        mkqlResult->Swap(&result.Result);

        if (AcquireLocks) {
            if (!UnpackMergeLocks(*mkqlResult, TxState->Tx(), ctx)) {
                return TStatus::Error;
            }
        }

        TransformCtx->MkqlResults.push_back(mkqlResult);
        TransformCtx->AddMkqlStats(MkqlExecuteResult.Program, std::move(result.TxStats));

        ++CurrentMkqlIndex;
        return TStatus::Repeat;
    }

    void Rewind() override {
        CurrentMkqlIndex = 0;
    }

private:
    TStatus Finish(TExprContext& ctx) {
        const auto& kql = TransformCtx->GetPreparedKql();
        if (!kql.GetEffects().empty()) {
            YQL_ENSURE(kql.GetEffects().size() == 1);

            auto& effect = kql.GetEffects(0);

            TExprNode::TPtr expr;
            if (!GetExpr(effect.GetNodeAst(), expr, ctx)) {
                return TStatus::Error;
            }

            TVector<NKikimrKqp::TParameterBinding> bindings(effect.GetBindings().begin(),
                effect.GetBindings().end());

            bool preserveParams = !TransformCtx->Settings.GetCommitTx();
            if (!AddDeferredEffect(TExprBase(expr), bindings, ctx, *TxState, *TransformCtx, preserveParams)) {
                return TStatus::Error;
            }
        }
        return TStatus::Ok;
    }

    bool Execute(const NKikimrKqp::TPreparedMkql& mkql, TFuture<IKqpGateway::TMkqlResult>& future) {
        if (YQL_CLOG_ACTIVE(DEBUG, ProviderKqp)) {
            TString mkqlText;
            NProtoBuf::TextFormat::PrintToString(mkql, &mkqlText);
            YQL_CLOG(DEBUG, ProviderKqp) << "Mkql:" << Endl << mkqlText;
        }

        TVector<NKikimrKqp::TParameterBinding> bindings(mkql.GetBindings().begin(), mkql.GetBindings().end());
        auto execParams = BuildParamsMap(bindings, TxState, TransformCtx, AcquireLocks);

        if (YQL_CLOG_ACTIVE(TRACE, ProviderKqp)) {
            TStringBuilder paramsTextBuilder;
            for (auto& pair : execParams.Values) {
                TString paramText;
                NProtoBuf::TextFormat::PrintToString(pair.second.GetValue(), &paramText);
                paramsTextBuilder << pair.first << ": " << paramText << Endl;
            }

            YQL_CLOG(TRACE, ProviderKqp) << "MiniKQL parameters:" << Endl << paramsTextBuilder;
        }

        if (TransformCtx->QueryCtx->StatsMode == EKikimrStatsMode::Profile) {
            MkqlExecuteResult.Program = mkql.GetProgramText();
        }

        future = Gateway->ExecuteMkqlPrepared(Cluster, mkql.GetProgram(), std::move(execParams),
            TransformCtx->GetMkqlSettings(false, Gateway->GetCurrentTime()),
            TxState->Tx().GetSnapshot());
        return true;
    }

    bool GetExpr(const TString& astStr, TExprNode::TPtr& expr, TExprContext& ctx) {
        auto astRes = ParseAst(astStr);
        ctx.IssueManager.AddIssues(astRes.Issues);
        if (!astRes.IsOk()) {
            return false;
        }

        return CompileExpr(*astRes.Root, expr, ctx, nullptr);
    }

    bool ShouldAcquireLocks(const NKikimrKqp::TPreparedKql& kql) {
        if (*TxState->Tx().EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
            return false;
        }

        if (TxState->Tx().Locks.Broken()) {
            return false; // Do not acquire locks after first lock issue
        }

        if (!TxState->Tx().DeferredEffects.Empty() || !kql.GetEffects().empty()) {
            return true; // Acquire locks in read write tx
        }

        if (!TransformCtx->Settings.GetCommitTx()) {
            return true; // It is not a commit tx
        }

        if (TxState->Tx().GetSnapshot().IsValid()) {
            return false; // Read only tx with snapshot, no need to acquire locks
        }

        if (CurrentMkqlIndex == kql.MkqlsSize() - 1 && !TxState->Tx().Locks.HasLocks()) {
            return false; // Final phase of read only tx, no need to acquire locks
        }

        return true;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;

    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;

    ui32 CurrentMkqlIndex;
    bool AcquireLocks;
    TMkqlExecuteResult MkqlExecuteResult;
    TPromise<void> Promise;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpExecutePreparedTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpExecutePreparedTransformer(gateway, cluster, txState, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
