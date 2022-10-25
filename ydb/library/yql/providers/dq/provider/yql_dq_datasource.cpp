#include "yql_dq_datasource.h"
#include "yql_dq_datasource_type_ann.h"
#include "yql_dq_state.h"

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

class TDqDataProviderSource: public TDataProviderBase {
public:
    TDqDataProviderSource(const TDqStatePtr& state, TExecTransformerFactory execTransformerFactory)
        : State(state)
        , ConfigurationTransformer([this]() {
            return MakeHolder<NCommon::TProviderConfigurationTransformer>(State->Settings, *State->TypeCtx, TString{DqProviderName});
        })
        , ExecTransformer([this, execTransformerFactory] () { return THolder<IGraphTransformer>(execTransformerFactory(State)); })
        , TypeAnnotationTransformer([] () { return CreateDqsDataSourceTypeAnnotationTransformer(); })
    { }

    TStringBuf GetName() const override {
        return DqProviderName;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer;
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        if (!node.IsComplete()) {
            return false;
        }

        bool canBuild = true;
        VisitExpr(node, [&canBuild] (const TExprNode& n) {
            if (!canBuild) {
                return false;
            }
            if (TDqConnection::Match(&n) || TDqPhyPrecompute::Match(&n)) {
                // Don't go deeper
                return false;
            }
            if (!n.IsComposable()) {
                canBuild = false;
                return false;
            }
            return true;
        });

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

        if (!TDqCnUnionAll::Match(node.Get())) {
            ctx.AddError(TIssue(node->Pos(ctx), "Last connection must be union all"));
            return {};
        }

        TExprNode::TListType worlds;
        VisitExpr(node, [&worlds] (const TExprNode::TPtr& item) {
            if (ETypeAnnotationKind::World == item->GetTypeAnn()->GetKind()) {
                worlds.emplace_back(item);
                return false;
            }
            return true;
        });

        const auto newWorld = ctx.NewWorld(node->Pos());
        TNodeOnNodeOwnedMap replaces(worlds.size());
        for (const auto& w : worlds)
            replaces.emplace(w.Get(), newWorld);

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
        if (!TDqCnUnionAll::Match(&node)) {
            return false;
        }

        if (auto type = GetItemType(*node.GetTypeAnn()); !type || type->GetKind() != ETypeAnnotationKind::Struct) {
            return false;
        }

        canRef = State->Settings->EnableFullResultWrite.Get().GetOrElse(false);
        if (canRef) {
            if (auto fullResultTableProvider = State->TypeCtx->DataSinkMap.Value(State->TypeCtx->FullResultDataSink, nullptr)) {
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

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer->CanParse(node);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer;
    }

    void Reset() final {
        if (ExecTransformer) {
            ExecTransformer->Rewind();
            TypeAnnotationTransformer->Rewind();
        }
    }

private:
    TDqStatePtr State;
    TLazyInitHolder<IGraphTransformer> ConfigurationTransformer;
    TLazyInitHolder<IGraphTransformer> ExecTransformer;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer;
};

TIntrusivePtr<IDataProvider> CreateDqDataSource(const TDqStatePtr& state, TExecTransformerFactory execTransformerFactory) {
    return new TDqDataProviderSource(state, execTransformerFactory);
}

} // namespace NYql
