#include "kqp_host_impl.h"
#include "kqp_run_physical.h"

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NThreading;

class TKqpExecuteScanTransformer : public TKqpExecutePhysicalTransformerBase {
public:
    TKqpExecuteScanTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : TKqpExecutePhysicalTransformerBase(gateway, cluster, txState, transformCtx) {}

protected:
    TStatus DoExecute(std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit, NYql::TExprContext&) final {
        YQL_ENSURE(tx);
        YQL_ENSURE(!commit);

        IKqpGateway::TExecPhysicalRequest request;
        request.Transactions.emplace_back(tx, PrepareParameters(*tx));

        request.RlPath = TransformCtx->QueryCtx->RlPath;
        request.Timeout = TransformCtx->QueryCtx->Deadlines.TimeoutAt - Gateway->GetCurrentTime();
        if (!request.Timeout) {
            // TODO: Just cancel request.
            request.Timeout = TDuration::MilliSeconds(1);
        }
        request.MaxComputeActors = TransformCtx->Config->_KqpMaxComputeActors.Get().GetRef();
        request.StatsMode = GetStatsMode(TransformCtx->QueryCtx->StatsMode);
        request.DisableLlvmForUdfStages = TransformCtx->Config->DisableLlvmForUdfStages();
        request.LlvmEnabled = TransformCtx->Config->GetEnableLlvm() != EOptionalFlag::Disabled;
        request.Snapshot = TxState->Tx().GetSnapshot();

        switch (tx->GetType()) {
            case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                ExecuteFuture = Gateway->ExecutePhysical(std::move(request), TransformCtx->ReplyTarget);
                break;
            case NKqpProto::TKqpPhyTx::TYPE_SCAN:
                ExecuteFuture = Gateway->ExecuteScanQuery(std::move(request), TransformCtx->ReplyTarget);
                break;
            default:
                YQL_ENSURE(false, "Unexpected physical tx type in scan query: "
                    << NKqpProto::TKqpPhyTx_EType_Name(tx->GetType()));
        }

        return TStatus::Async;
    }

    TStatus DoRollback() final {
        YQL_ENSURE(false, "Rollback in ScanQuery tx");
    }

    bool OnExecuterResult(NKikimrKqp::TExecuterTxResult&& execResult, TExprContext& ctx, bool commit) override {
        Y_UNUSED(ctx);
        Y_UNUSED(commit);

        if (execResult.HasStats()) {
            TransformCtx->QueryStats.AddExecutions()->Swap(execResult.MutableStats());
        }

        return true;
    }
};

class TKqpCreateSnapshotTransformer : public TGraphTransformerBase {
public:
    TKqpCreateSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKqlTransformContext> transformCtx,
        TIntrusivePtr<TKqpTransactionState> txState)
        : Gateway(gateway)
        , TransformCtx(transformCtx)
        , TxState(txState)
    {}

    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext&) {
        output = input;

        THashSet<TString> tablesSet;
        for (const auto& phyTx: TransformCtx->PhysicalQuery->GetTransactions()) {
            for (const auto& stage: phyTx.GetStages()) {
                for (const auto& tableOp: stage.GetTableOps()) {
                    tablesSet.insert(tableOp.GetTable().GetPath());
                }
            }
        }
        TVector<TString> tables(tablesSet.begin(), tablesSet.end());

        auto timeout = TransformCtx->QueryCtx->Deadlines.TimeoutAt - Gateway->GetCurrentTime();
        if (!timeout) {
            // TODO: Just cancel request.
            timeout = TDuration::MilliSeconds(1);
        }
        SnapshotFuture = Gateway->CreatePersistentSnapshot(tables, timeout);

        Promise = NewPromise();
        auto promise = Promise;
        SnapshotFuture.Apply([promise](const TFuture<IKqpGateway::TKqpSnapshotHandle> future) mutable {
            YQL_ENSURE(future.HasValue());
            promise.SetValue();
        });

        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode& ) {
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) {
        output = input;

        auto handle = SnapshotFuture.ExtractValue();

        if (!handle.Snapshot.IsValid()) {
            YQL_CLOG(NOTICE, ProviderKqp) << "Failed to create persistent snapshot. "
                << "Status: " << NKikimrIssues::TStatusIds_EStatusCode_Name(handle.Status)
                << ", issues: " << handle.Issues().ToString();

            TIssue issue("Failed to create persistent snapshot");
            switch (handle.Status) {
                case NKikimrIssues::TStatusIds::SCHEME_ERROR:
                    issue.SetCode(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR, NYql::TSeverityIds::S_ERROR);
                    break;
                case NKikimrIssues::TStatusIds::TIMEOUT:
                    issue.SetCode(NYql::TIssuesIds::KIKIMR_TIMEOUT, NYql::TSeverityIds::S_ERROR);
                    break;
                case NKikimrIssues::TStatusIds::OVERLOADED:
                    issue.SetCode(NYql::TIssuesIds::KIKIMR_OVERLOADED, NYql::TSeverityIds::S_ERROR);
                    break;
                default:
                    // ScanQuery is always RO, so we can return UNAVAILABLE here
                    issue.SetCode(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, NYql::TSeverityIds::S_ERROR);
                    break;
            }

            for (const auto& subIssue: handle.Issues()) {
                issue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));
            }
            ctx.AddError(issue);
            return TStatus::Error;
        }

        TxState->Tx().SnapshotHandle = handle;
        return TStatus::Ok;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;

    NThreading::TFuture<IKqpGateway::TKqpSnapshotHandle> SnapshotFuture;
    NThreading::TPromise<void> Promise;
    TIntrusivePtr<TKqpTransactionState> TxState;
};

class TKqpReleaseSnapshotTransformer : public TSyncTransformerBase {
public:
    TKqpReleaseSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKqpTransactionState> txState)
        : Gateway(gateway)
        , TxState(txState)
    {}

    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext&) {
        output = input;

        if (TxState->Tx().GetSnapshot().IsValid()) {
            Gateway->DiscardPersistentSnapshot(TxState->Tx().SnapshotHandle);
        }

        return TStatus::Ok;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKqpTransactionState> TxState;
};


TAutoPtr<IGraphTransformer> CreateKqpExecuteScanTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpExecuteScanTransformer(gateway, cluster, txState, transformCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpCreateSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqlTransformContext> transformCtx, TIntrusivePtr<TKqpTransactionState> txState)
{
    return new TKqpCreateSnapshotTransformer(gateway, transformCtx, txState);
}

TAutoPtr<IGraphTransformer> CreateKqpReleaseSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqpTransactionState> txState)
{
    return new TKqpReleaseSnapshotTransformer(gateway, txState);
}

} // namespace NKqp
} // namespace NKikimr
