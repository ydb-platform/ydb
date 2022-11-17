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
        TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TransformCtx(transformCtx)
        , CurrentTxIndex(0)
    {
    }

    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override {
        output = input;

        auto pure = [](const auto& tx) {
            if (tx.GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) {
                return false;
            }

            for (const auto& stage : tx.GetStages()) {
                if (stage.InputsSize() != 0) {
                    return false;
                }
            }

            return true;
        };

        auto& query = *TransformCtx->QueryCtx->PreparingQuery->MutablePhysicalQuery();
        TxResults.resize(query.TransactionsSize());
        while (CurrentTxIndex < query.TransactionsSize()) {
            auto tx = std::make_shared<NKqpProto::TKqpPhyTx>(query.GetTransactions(CurrentTxIndex));
            auto params = PrepareParameters(*tx);

            if (pure(*tx) && params) {
                IKqpGateway::TExecPhysicalRequest request;
                request.Transactions.emplace_back(std::move(tx), std::move(*params));
                request.NeedTxId = false;

                ExecuteFuture = Gateway->ExecutePure(std::move(request));

                Promise = NewPromise();
                ExecuteFuture.Apply([promise = Promise](const TFuture<IKqpGateway::TExecPhysicalResult> future) mutable {
                    YQL_ENSURE(future.HasValue());
                    promise.SetValue();
                });

                return TStatus::Async;
            }

            ++CurrentTxIndex;
        }

        PhyQuerySetTxPlans(query, TKqpPhysicalQuery(input), std::move(TxResults),
            ctx, Cluster, TransformCtx->Tables, TransformCtx->Config);
        query.SetQueryAst(KqpExprToPrettyString(*input, ctx));

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

        auto& txResults = result.ExecuterResult.GetResults();
        TxResults[CurrentTxIndex] = {txResults.begin(), txResults.end()};

        ++CurrentTxIndex;
        return TStatus::Repeat;
    }

    void Rewind() override {
        CurrentTxIndex = 0;
        TxResults.clear();
    }

private:
    TMaybe<TKqpParamsMap> PrepareParameters(const NKqpProto::TKqpPhyTx& tx) {
        TKqpParamsMap params;
        for (const auto& paramBinding : tx.GetParamBindings()) {
            if (auto paramValue = GetParamValue(/*ensure*/ false, *TransformCtx->QueryCtx,
                    TransformCtx->QueryCtx->Parameters, TxResults, paramBinding))
            {
                params.Values.emplace(std::make_pair(paramBinding.GetName(), *paramValue));
            } else {
                return {};
            }
        }

        return TMaybe<TKqpParamsMap>(params);
    }

    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TVector<TVector<NKikimrMiniKQL::TResult>> TxResults;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    ui32 CurrentTxIndex;
    NThreading::TFuture<IKqpGateway::TExecPhysicalResult> ExecuteFuture;
    NThreading::TPromise<void> Promise;
};


TAutoPtr<IGraphTransformer> CreateKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpExplainPreparedTransformer(gateway, cluster, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
