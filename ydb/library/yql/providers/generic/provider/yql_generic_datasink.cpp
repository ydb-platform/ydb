#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        class TGenericDataSink: public TDataProviderBase {
        public:
            TGenericDataSink(TGenericState::TPtr state)
                : State_(state)
                , TypeAnnotationTransformer_(CreateGenericDataSinkTypeAnnotationTransformer(State_))
                , ExecutionTransformer_(CreateGenericDataSinkExecTransformer(State_))
                , LogicalOptProposalTransformer_(CreateGenericLogicalOptProposalTransformer(State_))
                , PhysicalOptProposalTransformer_(CreateGenericPhysicalOptProposalTransformer(State_))
            {
            }

            TStringBuf GetName() const override {
                return GenericProviderName;
            }

            bool CanParse(const TExprNode& node) override {
                return TypeAnnotationTransformer_->CanParse(node);
            }

            IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
                Y_UNUSED(instantOnly);
                return *TypeAnnotationTransformer_;
            }

            IGraphTransformer& GetCallableExecutionTransformer() override {
                return *ExecutionTransformer_;
            }

            bool CanExecute(const TExprNode& node) override {
                return ExecutionTransformer_->CanExec(node);
            }

            bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
                if (node.IsCallable(TCoDataSink::CallableName())) {
                    if (node.Child(0)->Content() == GenericProviderName) {
                        auto clusterName = node.Child(1)->Content();
                        if (!State_->Configuration->HasCluster(clusterName)) {
                            ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()),
                                                TStringBuilder() << "Unknown cluster name: " << clusterName));
                            return false;
                        }
                        cluster = clusterName;
                        return true;
                    }
                }
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Generic DataSink parameters"));
                return false;
            }

            IGraphTransformer& GetLogicalOptProposalTransformer() override {
                return *LogicalOptProposalTransformer_;
            }

            IGraphTransformer& GetPhysicalOptProposalTransformer() override {
                return *PhysicalOptProposalTransformer_;
            }

        private:
            const TGenericState::TPtr State_;
            const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
            const THolder<TExecTransformerBase> ExecutionTransformer_;
            const THolder<IGraphTransformer> LogicalOptProposalTransformer_;
            const THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
        };

    }

    TIntrusivePtr<IDataProvider> CreateGenericDataSink(TGenericState::TPtr state) {
        return new TGenericDataSink(state);
    }

} // namespace NYql
