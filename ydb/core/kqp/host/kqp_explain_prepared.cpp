#include "kqp_host_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/host/kqp_transform.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NThreading;

class TKqpExplainPreparedTransformer : public NYql::TGraphTransformerBase {
public:
    TKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqlTransformContext> transformCtx, const NMiniKQL::IFunctionRegistry* funcRegistry,
        TTypeAnnotationContext& typeCtx, TIntrusivePtr<NOpt::TKqpOptimizeContext> optCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TransformCtx(transformCtx)
        , FuncRegistry(funcRegistry)
        , CurrentTxIndex(0)
        , TypeCtx(typeCtx)
        , OptimizeCtx(optCtx)
    {
        TxAlloc = TransformCtx->QueryCtx->QueryData->GetAllocState();
    }

    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override {
        if (!TransformCtx->ExplainTransformerInput) {
            ctx.IssueManager.RaiseIssue(TIssue("No input nodes for explain transformer"));
            return TStatus::Error;
        }

        output = input;
        auto preparedQueryCopy = std::make_unique<NKikimrKqp::TPreparedQuery>(*TransformCtx->QueryCtx->PreparingQuery);
        TPreparedQueryHolder::TConstPtr queryConstPtr = std::make_shared<TPreparedQueryHolder>(
            preparedQueryCopy.release(), FuncRegistry);

        auto& query = *TransformCtx->QueryCtx->PreparingQuery->MutablePhysicalQuery();
        TxResults.resize(query.TransactionsSize());
        while (CurrentTxIndex < query.TransactionsSize()) {
            const auto& tx = queryConstPtr->GetPhyTx(CurrentTxIndex);
            bool prepared = PrepareParameters(tx);

            if (tx->IsLiteralTx() && prepared) {
                IKqpGateway::TExecPhysicalRequest request(TxAlloc);
                request.Transactions.emplace_back(tx, TransformCtx->QueryCtx->QueryData);
                request.NeedTxId = false;

                ExecuteFuture = Gateway->ExecuteLiteral(std::move(request), TransformCtx->QueryCtx->QueryData, CurrentTxIndex);

                Promise = NewPromise();
                ExecuteFuture.Apply([promise = Promise](const TFuture<IKqpGateway::TExecPhysicalResult> future) mutable {
                    YQL_ENSURE(future.HasValue());
                    promise.SetValue();
                });

                return TStatus::Async;
            }

            ++CurrentTxIndex;
        }

        PhyQuerySetTxPlans(query, TKqpPhysicalQuery(TransformCtx->ExplainTransformerInput), std::move(TxResults),
            ctx, Cluster, TransformCtx->Tables, TransformCtx->Config, TypeCtx, OptimizeCtx);
        query.SetQueryAst(KqpExprToPrettyString(*input, ctx));

        TransformCtx->ExplainTransformerInput = nullptr;
        return TStatus::Ok;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode& /*input*/) override {
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output,
        NYql::TExprContext& ctx) override
    {
        output = input;

        auto result = ExecuteFuture.ExtractValue();
        result.ReportIssues(ctx.IssueManager);
        if (!result.Success()) {
            return TStatus::Error;
        }

        auto& txResults = result.Results;
        TxResults[CurrentTxIndex] = {txResults.begin(), txResults.end()};

        ++CurrentTxIndex;
        return TStatus::Repeat;
    }

    void Rewind() override {
        CurrentTxIndex = 0;
        TxResults.clear();
    }

private:
    bool PrepareParameters(const TKqpPhyTxHolder::TConstPtr& tx) {
        for (const auto& paramBinding : tx->GetParamBindings()) {
            bool res = TransformCtx->QueryCtx->QueryData->MaterializeParamValue(/*ensure*/ false,
                paramBinding);
            if (!res) {
                return false;
            }
        }

        return true;
    }

    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TVector<TVector<NKikimrMiniKQL::TResult>> TxResults;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    const NMiniKQL::IFunctionRegistry* FuncRegistry;
    ui32 CurrentTxIndex;
    NThreading::TFuture<IKqpGateway::TExecPhysicalResult> ExecuteFuture;
    NThreading::TPromise<void> Promise;
    TTxAllocatorState::TPtr TxAlloc;
    TTypeAnnotationContext& TypeCtx;
    TIntrusivePtr<NOpt::TKqpOptimizeContext> OptimizeCtx;
};


TAutoPtr<IGraphTransformer> CreateKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqlTransformContext> transformCtx, const NMiniKQL::IFunctionRegistry* funcRegistry,
    TTypeAnnotationContext& typeCtx, TIntrusivePtr<NOpt::TKqpOptimizeContext> optimizeCtx)
{
    return new TKqpExplainPreparedTransformer(gateway, cluster, transformCtx, funcRegistry, typeCtx, optimizeCtx);
}

} // namespace NKqp
} // namespace NKikimr
