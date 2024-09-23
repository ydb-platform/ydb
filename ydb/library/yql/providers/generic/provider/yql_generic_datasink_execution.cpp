#include "yql_generic_state.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        class TGenericDataSinkExecTransformer: public TExecTransformerBase {
        public:
            TGenericDataSinkExecTransformer(TGenericState::TPtr state)
                : State_(state)
            {
                AddHandler({TCoCommit::CallableName()}, RequireFirst(), Hndl(&TGenericDataSinkExecTransformer::HandleCommit));
            }

        private:
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
                YQL_CLOG(INFO, ProviderGeneric) << "Delegate execution of " << input->Content() << " to DQ provider.";
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

                for (auto idx : {TResOrPullBase::idx_BytesLimit, TResOrPullBase::idx_RowsLimit, TResOrPullBase::idx_FormatDetails,
                                 TResOrPullBase::idx_Format, TResOrPullBase::idx_PublicId, TResOrPullBase::idx_Discard}) {
                    delegatedNode->Child(idx)->SetTypeAnn(ctx.MakeType<TUnitExprType>());
                    delegatedNode->Child(idx)->SetState(TExprNode::EState::ConstrComplete);
                }

                delegatedNode->SetTypeAnn(originInput->GetTypeAnn());
                delegatedNode->SetState(TExprNode::EState::ConstrComplete);
                originInput->SetState(TExprNode::EState::ExecutionInProgress);

                const auto dqProvider = State_->Types->DataSourceMap.FindPtr(DqProviderName);

                TExprNode::TPtr delegatedNodeOutput;
                if (const auto status = dqProvider->Get()->GetCallableExecutionTransformer().Transform(delegatedNode, delegatedNodeOutput, ctx); status.Level != TStatus::Async) {
                    YQL_ENSURE(status.Level != TStatus::Ok, "Asynchronous execution is expected in a happy path.");
                    return SyncStatus(status);
                }

                auto dqFuture = dqProvider->Get()->GetCallableExecutionTransformer().GetAsyncFuture(*delegatedNode);

                TAsyncTransformCallbackFuture callbackFuture = dqFuture.Apply(
                    [dqProvider, delegatedNode](const NThreading::TFuture<void>& completedFuture) {
                        return TAsyncTransformCallback(
                            [completedFuture, dqProvider, delegatedNode](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                                completedFuture.GetValue();
                                output = input;
                                TExprNode::TPtr delegatedNodeOutput;
                                auto dqWriteStatus = dqProvider->Get()->GetCallableExecutionTransformer().ApplyAsyncChanges(delegatedNode, delegatedNodeOutput, ctx);

                                YQL_ENSURE(dqWriteStatus != TStatus::Async, "ApplyAsyncChanges should not return Async.");

                                if (dqWriteStatus == TStatus::Repeat)
                                    output->SetState(TExprNode::EState::ExecutionRequired);

                                if (dqWriteStatus != TStatus::Ok)
                                    return dqWriteStatus;

                                output->SetState(TExprNode::EState::ExecutionComplete);
                                output->SetResult(ctx.NewAtom(input->Pos(), "DQ_completed"));
                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
                            });
                    });

                return std::make_pair(IGraphTransformer::TStatus::Async, callbackFuture);
            }

            const TGenericState::TPtr State_;
        };

    }

    THolder<TExecTransformerBase> CreateGenericDataSinkExecTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericDataSinkExecTransformer(state));
    }

} // namespace NYql
