#include "yql_dq_datasource.h"
#include "yql_dq_datasource_constraints.h"
#include "yql_dq_datasource_type_ann.h"
#include "yql_dq_state.h"
#include "yql_dq_validate.h"
#include "yql_dq_statistics.h"

#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/providers/dq/opt/dqs_opt.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/planner/execution_planner.h>

#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

using namespace NCommon;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using namespace NDq;

namespace {

class TDqDataProviderSource: public TDataProviderBase {
public:
    TDqDataProviderSource(const TDqState::TPtr& state, TExecTransformerFactory execTransformerFactory)
        : State_(state)
        , ConfigurationTransformer_([this]() {
            return MakeHolder<NCommon::TProviderConfigurationTransformer>(State_->Settings, *State_->TypeCtx, TString{DqProviderName});
        })
        , ExecTransformer_([this, execTransformerFactory] () { return THolder<IGraphTransformer>(execTransformerFactory(State_)); })
        , TypeAnnotationTransformer_([] () { return CreateDqsDataSourceTypeAnnotationTransformer(); })
        , ConstraintsTransformer_([] () { return CreateDqDataSourceConstraintTransformer(); })
        , StatisticsTransformer_([this]() { return CreateDqsStatisticsTransformer(State_, TBaseProviderContext::Instance()); })
    { }

    TStringBuf GetName() const override {
        return DqProviderName;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        Y_UNUSED(instantOnly && subGraph);
        return *ConstraintsTransformer_;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer_;
    }

    IGraphTransformer& GetStatisticsProposalTransformer() override {
        return *StatisticsTransformer_;
    }

    bool CanBuildResultImpl(const TExprNode& node, TNodeSet& visited) {
        if (!visited.emplace(&node).second) {
            return true;
        }

        if (TDqConnection::Match(&node) || TDqPhyPrecompute::Match(&node)) {
            // Don't go deeper
            return true;
        }

        if (auto right = TMaybeNode<TCoRight>(&node)) {
            auto cons = right.Cast().Input().Maybe<TCoCons>();
            if (!cons) {
                return false;
            }

            return CanBuildResultImpl(cons.Cast().Input().Ref(), visited);
        }

        if (!node.IsComposable()) {
            return false;
        }

        for (const auto& child : node.Children()) {
            if (!CanBuildResultImpl(*child, visited)) {
                return false;
            }
        }

        return true;
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        if (!node.IsComplete()) {
            return false;
        }

        TNodeSet visited;
        bool canBuild = CanBuildResultImpl(node, visited);
        if (canBuild) {
            for (const auto& child : node.ChildrenList()) {
                VisitExpr(child, [&syncList] (const TExprNode::TPtr& item) {
                    if (ETypeAnnotationKind::World == item->GetTypeAnn()->GetKind()) {
                        syncList.emplace(item, syncList.size());
                        return false;
                    }
                    return true;
                });
            }
        }

        return canBuild;
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        if (ETypeAnnotationKind::World == node->GetTypeAnn()->GetKind() && !root) {
            root = node;
            return true;
        }
        root = nullptr;
        return false;
    }

    bool CanEvaluate(const TExprNode& node) override {
        return TDqConnection::Match(&node) || TDqPhyPrecompute::Match(&node);
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& node, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) override
    {
        Y_UNUSED(optCtx);
        Y_UNUSED(fillSettings);

        if (TDqCnResult::Match(node.Get())) {
            return node;
        }

        if (!(TDqCnUnionAll::Match(node.Get()) || TDqCnMerge::Match(node.Get()))) {
            ctx.AddError(TIssue(node->Pos(ctx), "Last connection must be union all or merge"));
            return {};
        }

        const auto newWorld = ctx.NewWorld(node->Pos());
        TNodeOnNodeOwnedMap replaces;
        VisitExpr(node, [&replaces, &newWorld, &ctx] (const TExprNode::TPtr& item) {
            if (ETypeAnnotationKind::World == item->GetTypeAnn()->GetKind()) {
                replaces[item.Get()] = newWorld;
                return false;
            }

            if (auto right = TMaybeNode<TCoRight>(item)) {
                if (right.Cast().Input().Ref().IsCallable("PgReadTable!")) {
                    const auto& read = right.Cast().Input().Ref();
                    replaces[item.Get()] = ctx.Builder(item->Pos())
                        .Callable("PgTableContent")
                            .Add(0, read.Child(1)->TailPtr())
                            .Add(1, read.ChildPtr(2))
                            .Add(2, read.ChildPtr(3))
                            .Add(3, read.ChildPtr(4))
                        .Seal()
                        .Build();
                    return false;
                }
            }

            return true;
        });

        return Build<TDqCnResult>(ctx, node->Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(ctx.ReplaceNodes(TExprNode::TPtr(node), std::move(replaces)))
                    .Build()
                    .Program()
                        .Args({"row"})
                        .Body("row")
                    .Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, node->Pos()))
                .Build()
                .Index().Build("0")
            .Build()
            .ColumnHints() // TODO: set column hints
            .Build()
            .Done().Ptr();
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        if (!(TDqCnUnionAll::Match(&node) || TDqCnMerge::Match(&node))) {
            return false;
        }

        if (auto type = GetSeqItemType(node.GetTypeAnn()); !type || type->GetKind() != ETypeAnnotationKind::Struct) {
            return false;
        }

        canRef = State_->Settings->EnableFullResultWrite.Get().GetOrElse(false);
        if (canRef) {
            if (auto fullResultTableProvider = State_->TypeCtx->DataSinkMap.Value(State_->TypeCtx->FullResultDataSink, nullptr)) {
                canRef = !!fullResultTableProvider->GetDqIntegration();
            } else {
                canRef = false;
            }
        }

        for (const auto& child : node.ChildrenList())
            VisitExpr(child, [&syncList] (const TExprNode::TPtr& item) {
                if (ETypeAnnotationKind::World == item->GetTypeAnn()->GetKind()) {
                    syncList.emplace(item, syncList.size());
                    return false;
                }
                return true;
            });
        return true;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (!EnsureMinMaxArgsCount(node, 1, 2, ctx)) {
                return false;
            }

            if (node.Child(0)->Content() == DqProviderName) {
                if (node.ChildrenSize() == 2) {
                    if (!EnsureAtom(*node.Child(1), ctx)) {
                        return false;
                    }

                    if (node.Child(1)->Content() != "$all") {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() << "Unexpected cluster name: " << node.Child(1)->Content()));
                        return false;
                    }
                }
                cluster = Nothing();
                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid DQ DataSource parameters"));
        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        return TDqCnResult::Match(&node) || TDqQuery::Match(&node);
    }

    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
        return ValidateDqExecution(node, *State_->TypeCtx, ctx, State_);
    }

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

    void Reset() final {
        if (ExecTransformer_) {
            ExecTransformer_->Rewind();
            TypeAnnotationTransformer_->Rewind();
            ConstraintsTransformer_->Rewind();
        }
    }

private:
    const TDqState::TPtr State_;
    TLazyInitHolder<IGraphTransformer> ConfigurationTransformer_;
    TLazyInitHolder<IGraphTransformer> ExecTransformer_;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    TLazyInitHolder<IGraphTransformer> ConstraintsTransformer_;
    TLazyInitHolder<IGraphTransformer> StatisticsTransformer_;
};

}

TIntrusivePtr<IDataProvider> CreateDqDataSource(const TDqState::TPtr& state, TExecTransformerFactory execTransformerFactory) {
    return new TDqDataProviderSource(state, execTransformerFactory);
}

} // namespace NYql
