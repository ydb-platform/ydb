#include <ydb/library/yql/providers/ydb/query/common/provider_names.h>
#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>

namespace NYql {
namespace {

using namespace NNodes;
using NYdbQuery::TState;

bool ValidateCluster(const TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster, const TState& state, bool sink) {
    if (!node.IsCallable(sink ? "DataSink" : "DataSource") || node.ChildrenSize() < 2 ||
        !node.Child(0)->IsAtom(YdbQueryProviderName) || !EnsureAtom(*node.Child(1), ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Ydb provider parameters"));
        return false;
    }
    if (!state.ValidClusters.contains(node.Child(1)->Content())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unknown Ydb cluster"));
        return false;
    }
    cluster = node.Child(1)->Content();
    return true;
}

class TDataSource final : public TDataProviderBase {
public:
    explicit TDataSource(TState::TPtr state)
        : State_(std::move(state))
        , Metadata_(NYdbQuery::CreateLoadMetadataTransformer(State_))
        , TypeAnnotation_(NYdbQuery::CreateTypeAnnotationTransformer(State_))
        , PhysicalOptimizer_(NYdbQuery::CreatePhysicalOptimizer())
        , DqIntegration_(NYdbQuery::CreateDqIntegration(State_))
    {
    }

    TStringBuf GetName() const override {
        return YdbQueryProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        return ValidateCluster(node, ctx, cluster, *State_, false);
    }

    bool CanParse(const TExprNode& node) override {
        if (TYdbQueryRead::Match(&node)) {
            return node.ChildrenSize() > 1 && TYdbQueryDataSource::Match(node.Child(1));
        }
        return TypeAnnotation_->CanParse(node);
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        NYdbQuery::AddCluster(*State_, name, properties);
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Tokens;
    }

    const THashSet<TString>& GetValidClusters() override {
        return State_->ValidClusters;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *Metadata_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool) override {
        return *TypeAnnotation_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptimizer_;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext&) override {
        return node;
    }

    bool CanPullResult(const TExprNode& node, TSyncMap&, bool& canRef) override {
        canRef = false;
        return node.IsCallable("Right!") && TYdbQueryReadTable::Match(node.Child(0));
    }

    bool CanExecute(const TExprNode& node) override {
        return TYdbQueryReadTable::Match(&node);
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool) override {
        if (!TYdbQueryReadTable::Match(&node)) {
            return false;
        }
        for (const auto& child : node.Children()) {
            children.push_back(child.Get());
        }
        return true;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool) override {
        if (!TYdbQueryReadTable::Match(&node)) {
            return 0;
        }
        const TYdbQueryReadTable read(&node);
        inputs.emplace_back(read.DataSource().Raw(), nullptr, read.Table().Raw(),
            read.DataSource().Cluster().StringValue() + "." + read.Table().StringValue(), false);
        return 1;
    }

private:
    const TState::TPtr State_;
    const THolder<IGraphTransformer> Metadata_;
    const THolder<TVisitorTransformerBase> TypeAnnotation_;
    const THolder<IGraphTransformer> PhysicalOptimizer_;
    const THolder<IDqIntegration> DqIntegration_;
};

class TSinkTypeAnnotation final : public TVisitorTransformerBase {
public:
    TSinkTypeAnnotation()
        : TVisitorTransformerBase(true)
    {
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSinkTypeAnnotation::Commit));
        AddHandler({TCoWrite::CallableName()}, Hndl(&TSinkTypeAnnotation::Write));
    }

    TStatus Commit(const TExprNode::TPtr& node, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*node, 2, ctx) || !EnsureWorldType(*node->Child(0), ctx)) {
            return TStatus::Error;
        }
        node->SetTypeAnn(node->Child(0)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus Write(const TExprNode::TPtr& node, TExprContext& ctx) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Ydb writes are not supported yet"));
        return TStatus::Error;
    }
};

class TSinkExecution final : public TExecTransformerBase {
public:
    TSinkExecution() {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
    }
};

class TDataSink final : public TDataProviderBase {
public:
    explicit TDataSink(TState::TPtr state)
        : State_(std::move(state))
        , PhysicalOptimizer_(NYdbQuery::CreatePhysicalOptimizer())
        , LogicalOptimizer_(NYdbQuery::CreateLogicalOptimizer(State_))
    {
    }

    TStringBuf GetName() const override {
        return YdbQueryProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        return ValidateCluster(node, ctx, cluster, *State_, true);
    }

    bool CanParse(const TExprNode& node) override {
        return IsNativeSinkOperation(node) && TypeAnnotation_.CanParse(node);
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool) override {
        return TypeAnnotation_;
    }

    bool CanExecute(const TExprNode& node) override {
        return IsNativeSinkOperation(node) && Execution_.CanExec(node);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return Execution_;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptimizer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptimizer_;
    }

private:
    static bool IsNativeSinkOperation(const TExprNode& node) {
        return node.ChildrenSize() > 1 && node.Child(1)->IsCallable("DataSink") &&
            node.Child(1)->ChildrenSize() > 0 && node.Child(1)->Child(0)->IsAtom(YdbQueryProviderName);
    }

    const TState::TPtr State_;
    TSinkTypeAnnotation TypeAnnotation_;
    TSinkExecution Execution_;
    const THolder<IGraphTransformer> PhysicalOptimizer_;
    const THolder<IGraphTransformer> LogicalOptimizer_;
};

} // namespace

namespace NYdbQuery {

THolder<IGraphTransformer> CreatePhysicalOptimizer() {
    return CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
            if (const auto left = TMaybeNode<TCoLeft>(node); left && left.Input().Maybe<TYdbQueryReadTable>()) {
                return left.Cast().Input().Cast<TYdbQueryReadTable>().World().Ptr();
            }
            return node;
        }, ctx, TOptimizeExprSettings(nullptr));
    });
}

} // namespace NYdbQuery

TDataProviderInfo CreateYdbDataProviders(TTypeAnnotationContext* types,
                                              TYdbMetadataClientCacheFactory metadataClientCacheFactory,
                                              IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
                                              TInstant metadataDeadline) {
    auto state = MakeIntrusive<TState>(types, std::move(metadataClientCacheFactory),
        std::move(credentialsFactory), metadataDeadline);
    TDataProviderInfo info;
    info.Names.insert(TString(YdbQueryProviderName));
    info.Source = new TDataSource(state);
    info.Sink = new TDataSink(state);
    return info;
}

} // namespace NYql
