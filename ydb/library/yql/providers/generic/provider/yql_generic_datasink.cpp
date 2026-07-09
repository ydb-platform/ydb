#include "yql_generic_provider_impl.h"
#include "yql_generic_dq_integration.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>

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
                , DqIntegration_(CreateGenericDqIntegration(State_))
            {
            }

            TStringBuf GetName() const override {
                return GenericProviderName;
            }

            bool CanParse(const TExprNode& node) override {
                if (node.IsCallable(TCoWrite::CallableName())) {
                    return TGenDataSink::Match(node.Child(1));
                }
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

            TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
                auto maybeWrite = TMaybeNode<TGenWrite>(node);
                YQL_ENSURE(maybeWrite, "Expected Write!");

                YQL_CLOG(INFO, ProviderGeneric) << "RewriteIO";

                auto write = maybeWrite.Cast();

                // The Write! callable carries the target key as its 3rd argument (a `Key`
                // callable of the form (Key '('table (String '<path>)))), the data being
                // written as its 4th argument, and the write settings as the tail.
                const auto& keyNode = *write.Ref().Child(2);
                if (!keyNode.IsCallable("Key")) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyNode.Pos()), "Expected key"));
                    return {};
                }
                if (keyNode.ChildrenSize() < 1) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyNode.Pos()), "Key must have at least one component"));
                    return {};
                }

                const auto tagName = keyNode.Child(0)->Child(0)->Content();
                if (tagName != TStringBuf("table")) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyNode.Child(0)->Pos()),
                                        TStringBuilder() << "Unexpected tag: " << tagName));
                    return {};
                }

                const TExprNode* nameNode = keyNode.Child(0)->Child(1);
                if (!nameNode->IsCallable("String")) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyNode.Pos()), "Expected String as table key"));
                    return {};
                }
                if (!EnsureArgsCount(*nameNode, 1, ctx)) {
                    return {};
                }
                const TExprNode* tablePath = nameNode->Child(0);
                if (!EnsureAtom(*tablePath, ctx)) {
                    return {};
                }
                if (tablePath->Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(tablePath->Pos()), "Table name must not be empty"));
                    return {};
                }

                // Parse write settings (the tail of the Write! callable) to obtain the mode.
                const auto settings = NCommon::ParseWriteTableSettings(TExprList(write.Ref().TailPtr()), ctx);
                TCoAtom mode = settings.Mode
                                   ? settings.Mode.Cast()
                                   : Build<TCoAtom>(ctx, write.Pos()).Value("append").Done();

                return Build<TGenWriteTable>(ctx, write.Pos())
                    .World(write.World())
                    .DataSink(write.DataSink())
                    .Table<TGenTable>()
                        .Name().Value(tablePath->Content()).Build()
                        .Build()
                    .Input<TCoRemoveSystemMembers>()
                        .Input(write.Ref().ChildPtr(3))
                        .Build()
                    .Mode(mode)
                    .Done().Ptr();
            }

            bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
                Y_UNUSED(compact);
                if (CanExecute(node)) {
                    children.push_back(node.ChildPtr(0));
                    return true;
                }
                return false;
            }

            void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
                if (CanExecute(node)) {
                    children.push_back(node.ChildPtr(0));
                }
            }

            IGraphTransformer& GetLogicalOptProposalTransformer() override {
                return *LogicalOptProposalTransformer_;
            }

            IGraphTransformer& GetPhysicalOptProposalTransformer() override {
                return *PhysicalOptProposalTransformer_;
            }

            IDqIntegration* GetDqIntegration() override {
                return DqIntegration_.Get();
            }

        private:
            const TGenericState::TPtr State_;
            const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
            const THolder<TExecTransformerBase> ExecutionTransformer_;
            const THolder<IGraphTransformer> LogicalOptProposalTransformer_;
            const THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
            const THolder<IDqIntegration> DqIntegration_;
        };

    } // namespace

    TIntrusivePtr<IDataProvider> CreateGenericDataSink(TGenericState::TPtr state) {
        return new TGenericDataSink(state);
    }

} // namespace NYql
