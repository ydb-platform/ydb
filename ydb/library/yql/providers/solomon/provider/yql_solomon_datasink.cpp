#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSink : public TDataProviderBase {
public:
    TSolomonDataSink(TSolomonState::TPtr state)
        : State_ {state}
        , TypeAnnotationTransformer_(CreateSolomonDataSinkTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreateSolomonDataSinkExecTransformer(State_))
        , PhysicalOptProposalTransformer_(CreateSoPhysicalOptProposalTransformer(State_))
    {
    }

    TStringBuf GetName() const override {
        return SolomonProviderName;
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        auto config = State_->Gateway->GetClusterConfig(cluster);
        if (!config) {
            return {};
        }

        TPositionHandle pos;
        return Build<TCoAtom>(ctx, pos)
            .Value(config->GetCluster())
            .Done().Ptr();
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Configuration->Tokens;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecutionTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoWrite::CallableName())) {
            return TSoDataSink::Match(node.Child(1));
        }

        return TypeAnnotationTransformer_->CanParse(node);
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        callables.insert(TSoWriteToShard::CallableName());
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecutionTransformer_->CanExec(node);
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (node.Child(0)->Content() == SolomonProviderName) {
                auto clusterName = node.Child(1)->Content();
                if (!State_->Gateway->HasCluster(clusterName)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() <<
                        "Unknown cluster name: " << clusterName
                    ));
                    return false;
                }
                cluster = clusterName;
                return true;
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Solomon DataSink parameters"));
        return false;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        auto maybeWrite = TMaybeNode<TSoWrite>(node);
        YQL_ENSURE(maybeWrite);

        YQL_CLOG(INFO, ProviderSolomon) << "RewriteIO";

        auto write = maybeWrite.Cast();
        auto& key = write.Arg(2).Ref();
        if (!key.IsCallable(TStringBuf("Key"))) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuf("Expected key")));
            return {};
        }
        if (key.ChildrenSize() < 1) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuf("Key must have at least one component")));
            return {};
        }

        auto tagName = key.Child(0)->Child(0)->Content();
        if (tagName != TStringBuf("table")) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Child(0)->Pos()),
                TStringBuilder() << "Unexpected tag: " << tagName));
            return {};
        }

        const TExprNode* nameNode = key.Child(0)->Child(1);
        if (!nameNode->IsCallable("String")) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Expected String as table key"));
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

        return Build<TSoWriteToShard>(ctx, write.Pos())
            .World(write.World())
            .DataSink(write.DataSink())
            .Shard<TCoAtom>().Value(tablePath->Content()).Build()
            .Input<TCoRemoveSystemMembers>()
                .Input(write.Arg(3))
                .Build()
            .Done().Ptr();
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
        }
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            return true;
        }
        return false;
    }

    TString GetProviderPath(const TExprNode& node) override {
        return TStringBuilder() << SolomonProviderName << '.' << node.Child(1)->Content();
    }

    IDqIntegration* GetDqIntegration() override {
        return State_->IsRtmrMode() ? nullptr : State_->DqIntegration.Get();
    }

private:
    TSolomonState::TPtr State_;

    THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    THolder<TExecTransformerBase> ExecutionTransformer_;
    THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
};


TIntrusivePtr<IDataProvider> CreateSolomonDataSink(TSolomonState::TPtr state) {
    return new TSolomonDataSink(state);
}

} // namespace NYql
