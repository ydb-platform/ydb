#include "yql_pq_provider_impl.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_topic_key_parser.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

void ScanPlanDependencies(const TExprNode::TPtr& input, TExprNode::TListType& children) {
    VisitExpr(input, [&children](const TExprNode::TPtr& node) {
        if (node->IsCallable("DqCnResult")) {
            children.push_back(node->Child(0));
            return false;
        }

        return true;
    });
}

class TPqDataSinkProvider : public TDataProviderBase {
public:
    TPqDataSinkProvider(TPqState::TPtr state, IPqGateway::TPtr gateway)
        : State_(state)
        , Gateway_(gateway)
        , IODiscoveryTransformer_(CreatePqDataSinkIODiscoveryTransformer(State_))
        , TypeAnnotationTransformer_(CreatePqDataSinkTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreatePqDataSinkExecTransformer(State_))
        , LogicalOptProposalTransformer_(CreatePqLogicalOptProposalTransformer(State_))
        , PhysicalOptProposalTransformer_(CreatePqPhysicalOptProposalTransformer(State_))
    {
    }

    TStringBuf GetName() const override {
        return PqProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoWrite::CallableName())) {
            return TPqDataSink::Match(node.Child(1));
        }

        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *IODiscoveryTransformer_;
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
            if (node.Head().Content() == PqProviderName) {
                const auto& clusterSettings = State_->Configuration->ClustersConfigurationSettings;
                if (const auto clusterName = node.Child(1)->Content(); clusterSettings.FindPtr(clusterName)) {
                    cluster = clusterName;
                    return true;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() << "Unknown cluster name: " << clusterName));
                    return false;
                }
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Pq DataSink parameters"));
        return false;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        auto maybePqWrite = TMaybeNode<TPqWrite>(node);
        YQL_ENSURE(maybePqWrite.DataSink(), "Expected Write!, got: " << node->Content());

        YQL_CLOG(INFO, ProviderPq) << "Rewrite " << node->Content();
        const TCoWrite write(node);
        TTopicKeyParser key;
        YQL_ENSURE(key.Parse(*node->Child(2), nullptr, ctx), "Failed to extract topic name.");
        const auto settings = NCommon::ParseWriteTableSettings(TExprList(node->Child(4)), ctx);
        YQL_ENSURE(settings.Mode.Cast() == "append", "Only append write mode is supported for writing into topic");

        const auto cluster = TString(maybePqWrite.Cast().DataSink().Cluster().Value());
        const auto* found = State_->FindTopicMeta(cluster, key.GetTopicPath());
        if (!found) {
            ctx.AddError(TIssue(ctx.GetPosition(write.Pos()), TStringBuilder() << "Unknown topic `" << cluster << "`.`" << key.GetTopicPath() << "`"));
            return nullptr;
        }

        auto topicNode = Build<TPqTopic>(ctx, write.Pos())
            .Cluster().Value(cluster).Build()
            .Database().Value(State_->Configuration->GetDatabaseForTopic(cluster)).Build()
            .RowSpec(found->RowSpec)
            .Path().Value(key.GetTopicPath()).Build()
            .Props(BuildTopicPropsList(*found, write.Pos(), ctx))
            .Metadata().Build()
            .Done();

        return Build<TPqWriteTopic>(ctx, node->Pos())
            .World(write.World())
            .DataSink(write.DataSink().Ptr())
            .Topic(topicNode)
            .Input<TCoRemoveSystemMembers>()
                .Input(node->Child(3))
                .Build()
            .Mode(settings.Mode.Cast())
            .Settings(settings.Other)
            .Done().Ptr();
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        const auto* config = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
        if (!config) {
            return {};
        }

        TPositionHandle pos;
        return Build<NNodes::TPqClusterConfig>(ctx, pos)
            .Endpoint<TCoAtom>().Build(config->Endpoint)
            .TvmId<TCoAtom>().Build(ToString(config->TvmId))
            .Done().Ptr();
    }

    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override {
        Y_UNUSED(withLimits);
        if (auto maybeOp = TMaybeNode<TPqWriteTopic>(&node)) {
            auto op = maybeOp.Cast();
            outputs.push_back(TPinInfo(nullptr, op.DataSink().Raw(), op.Topic().Raw(), MakeTopicDisplayName(op.Topic().Cluster().Value(), op.Topic().Path().Value()), false));
            return 1;
        }
        return 0;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));

            if (TMaybeNode<TPqWriteTopic>(&node)) {
                ScanPlanDependencies(node.ChildPtr(TPqWriteTopic::idx_Input), children);
            }

            return true;
        }

        return false;
    }

    IDqIntegration* GetDqIntegration() override {
        return State_->DqIntegration.Get();
    }

private:
    TPqState::TPtr State_;
    IPqGateway::TPtr Gateway_;
    THolder<IGraphTransformer> IODiscoveryTransformer_;
    THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    THolder<TExecTransformerBase> ExecutionTransformer_;
    THolder<IGraphTransformer> LogicalOptProposalTransformer_;
    THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
};

}

TIntrusivePtr<IDataProvider> CreatePqDataSink(TPqState::TPtr state, IPqGateway::TPtr gateway) {
    return new TPqDataSinkProvider(state, gateway);
}

} // namespace NYql
