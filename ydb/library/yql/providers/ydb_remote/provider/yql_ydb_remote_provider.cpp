#include "yql_ydb_remote_provider_impl.h"

#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>

namespace NYql {
namespace {

using namespace NNodes;
using NYdbRemote::TState;

bool ValidateCluster(const TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster, const TState& state, bool sink) {
    if (!node.IsCallable(sink ? "DataSink" : "DataSource") || node.ChildrenSize() < 2 ||
        !node.Child(0)->IsAtom(YdbRemoteProviderName) || !EnsureAtom(*node.Child(1), ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid native YDB provider parameters"));
        return false;
    }
    if (!state.ValidClusters.contains(node.Child(1)->Content())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unknown native YDB cluster"));
        return false;
    }
    cluster = node.Child(1)->Content();
    return true;
}

class TDataSource final : public TDataProviderBase {
public:
    explicit TDataSource(TState::TPtr state)
        : State_(std::move(state))
        , Metadata_(NYdbRemote::CreateLoadMetadataTransformer(State_))
        , TypeAnnotation_(NYdbRemote::CreateTypeAnnotationTransformer(State_))
        , PhysicalOptimizer_(NYdbRemote::CreatePhysicalOptimizer())
        , DqIntegration_(NYdbRemote::CreateDqIntegration(State_))
    {
    }

    TStringBuf GetName() const override {
        return YdbRemoteProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        return ValidateCluster(node, ctx, cluster, *State_, false);
    }

    bool CanParse(const TExprNode& node) override {
        if (TYdbRemoteRead::Match(&node)) {
            return node.ChildrenSize() > 1 && TYdbRemoteDataSource::Match(node.Child(1));
        }
        return TypeAnnotation_->CanParse(node);
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        NYdbRemote::AddCluster(*State_, name, properties);
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
        return node.IsCallable("Right!") && TYdbRemoteReadTable::Match(node.Child(0));
    }

    bool CanExecute(const TExprNode& node) override {
        return TYdbRemoteReadTable::Match(&node);
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool) override {
        if (!TYdbRemoteReadTable::Match(&node)) {
            return false;
        }
        for (const auto& child : node.Children()) {
            children.push_back(child.Get());
        }
        return true;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool) override {
        if (!TYdbRemoteReadTable::Match(&node)) {
            return 0;
        }
        const TYdbRemoteReadTable read(&node);
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
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Native YDB writes are not supported yet"));
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
        , PhysicalOptimizer_(NYdbRemote::CreatePhysicalOptimizer())
    {
    }

    TStringBuf GetName() const override {
        return YdbRemoteProviderName;
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

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptimizer_;
    }

private:
    static bool IsNativeSinkOperation(const TExprNode& node) {
        return node.ChildrenSize() > 1 && node.Child(1)->IsCallable("DataSink") &&
            node.Child(1)->ChildrenSize() > 0 && node.Child(1)->Child(0)->IsAtom(YdbRemoteProviderName);
    }

    const TState::TPtr State_;
    TSinkTypeAnnotation TypeAnnotation_;
    TSinkExecution Execution_;
    const THolder<IGraphTransformer> PhysicalOptimizer_;
};

} // namespace

namespace NYdbRemote {

THolder<IGraphTransformer> CreatePhysicalOptimizer() {
    return CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
            if (const auto left = TMaybeNode<TCoLeft>(node); left && left.Input().Maybe<TYdbRemoteReadTable>()) {
                return left.Cast().Input().Cast<TYdbRemoteReadTable>().World().Ptr();
            }
            return node;
        }, ctx, TOptimizeExprSettings(nullptr));
    });
}

} // namespace NYdbRemote

TDataProviderInfo CreateYdbRemoteDataProviders(TTypeAnnotationContext* types, const NYdb::TDriver& driver,
                                              const NYdb::TDriver& tlsDriver,
                                              IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
                                              TInstant metadataDeadline,
                                              std::shared_ptr<NNative::IAsyncMemoryQuota> metadataQuota) {
    auto state = MakeIntrusive<TState>(types, driver, tlsDriver, std::move(credentialsFactory), metadataDeadline, std::move(metadataQuota));
    TDataProviderInfo info;
    info.Names.insert(TString(YdbRemoteProviderName));
    info.Source = new TDataSource(state);
    info.Sink = new TDataSink(state);
    return info;
}

} // namespace NYql
