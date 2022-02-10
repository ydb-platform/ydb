#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDataSinkExecTransformer : public TExecTransformerBase {
public:
    explicit TPqDataSinkExecTransformer(TPqState::TPtr state)
        : State_(state)
    {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Hndl(&TPqDataSinkExecTransformer::HandleCommit));
    }

    TStatusCallbackPair HandleCommit(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (TDqQuery::Match(input->Child(TCoCommit::idx_World))) {
            return DelegateExecutionToDqProvider(input->ChildPtr(TCoCommit::idx_World), input, ctx);
        } else { // Pass
            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }
    }

    TStatusCallbackPair DelegateExecutionToDqProvider(const TExprNode::TPtr& input, const TExprNode::TPtr& originInput, TExprContext& ctx) {
        YQL_CLOG(INFO, ProviderPq) << "Delegate execution of " << input->Content() << " to dq provider";
        auto delegatedNode = Build<TPull>(ctx, input->Pos())
            .Input(input)
            .BytesLimit()
                .Value(TString())
                .Build()
            .RowsLimit()
                .Value(TString("0"))
                .Build()
            .FormatDetails()
                .Value(ToString((ui32)NYson::EYsonFormat::Binary))
                .Build()
            .Settings()
                .Build()
            .Format()
                .Value(ToString("0"))
                .Build()
            .PublicId()
                .Value("id")
                .Build()
            .Discard()
                .Value(ToString(true))
                .Build()
            .Origin(originInput)
            .Done()
            .Ptr();

        auto atomType = ctx.MakeType<TUnitExprType>();

        for (auto idx: {TResOrPullBase::idx_BytesLimit, TResOrPullBase::idx_RowsLimit, TResOrPullBase::idx_FormatDetails,
            TResOrPullBase::idx_Format, TResOrPullBase::idx_PublicId, TResOrPullBase::idx_Discard }) {
            delegatedNode->Child(idx)->SetTypeAnn(atomType);
            delegatedNode->Child(idx)->SetState(TExprNode::EState::ConstrComplete);
        }

        delegatedNode->SetTypeAnn(originInput->GetTypeAnn());
        delegatedNode->SetState(TExprNode::EState::ConstrComplete);
        originInput->SetState(TExprNode::EState::ExecutionInProgress);

        auto dqProvider =  State_->Types->DataSourceMap.FindPtr(DqProviderName);
        YQL_ENSURE(dqProvider);

        TExprNode::TPtr delegatedNodeOutput;
        auto status = dqProvider->Get()->GetCallableExecutionTransformer().Transform(delegatedNode, delegatedNodeOutput, ctx);

        if (status.Level != TStatus::Async) {
            YQL_ENSURE(status.Level != TStatus::Ok, "Asynchronous execution is expected in a happy path.");
            return SyncStatus(status);
        }

        auto dqFuture = dqProvider->Get()->GetCallableExecutionTransformer().GetAsyncFuture(*delegatedNode);

        TAsyncTransformCallbackFuture callbackFuture = dqFuture.Apply(
            [dqProvider, delegatedNode](const NThreading::TFuture<void>& completedFuture) {
                return TAsyncTransformCallback(
                    [completedFuture, dqProvider, delegatedNode](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                        completedFuture.GetValue();
                        TExprNode::TPtr delegatedNodeOutput;
                        auto dqWriteStatus = dqProvider->Get()->GetCallableExecutionTransformer()
                            .ApplyAsyncChanges(delegatedNode, delegatedNodeOutput, ctx);

                        YQL_ENSURE(dqWriteStatus != TStatus::Async, "ApplyAsyncChanges should not return Async.");

                        if (dqWriteStatus != TStatus::Ok) {
                            output = input;
                            return dqWriteStatus;
                        }

                        input->SetState(TExprNode::EState::ExecutionComplete);
                        output = ctx.ShallowCopy(*input);
                        output->SetResult(ctx.NewAtom(input->Pos(), "DQ_completed"));

                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                    });
            });

        return std::make_pair(IGraphTransformer::TStatus::Async, callbackFuture);
    }

private:
    TPqState::TPtr State_;
};

}

THolder<TExecTransformerBase> CreatePqDataSinkExecTransformer(TPqState::TPtr state) {
    return THolder(new TPqDataSinkExecTransformer(state));
}

} // namespace NYql
