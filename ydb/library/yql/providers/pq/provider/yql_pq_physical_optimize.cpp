#include "yql_pq_provider_impl.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

class TPqPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TPqPhysicalOptProposalTransformer(TPqState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderPq, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TPqPhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        AddHandler(0, &TPqWriteTopic::Match, HNDL(PqWriteTopic));
#undef HNDL

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TPqReadTopic>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    NNodes::TCoNameValueTupleList BuildTopicWriteSettings(const TString& cluster, TPositionHandle pos, TExprContext& ctx) const {
        TVector<TCoNameValueTuple> props;

        auto clusterConfiguration = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
        if (!clusterConfiguration) {
            ythrow yexception() << "Unknown pq cluster \"" << cluster << "\"";
        }

        Add(props, EndpointSetting, clusterConfiguration->Endpoint, pos, ctx);
        if (clusterConfiguration->UseSsl) {
            Add(props, UseSslSetting, "1", pos, ctx);
        }

        if (clusterConfiguration->AddBearerToToken) {
            Add(props, AddBearerToTokenSetting, "1", pos, ctx);
        }

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(props)
            .Done();
    }

    TMaybeNode<TExprBase> PqWriteTopic(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        auto write = node.Cast<TPqWriteTopic>();
        if (!TDqCnUnionAll::Match(write.Input().Raw())) { // => this code is not for RTMR mode.
            return node;
        }

        const auto& topicNode = write.Topic();
        const TString cluster(topicNode.Cluster().Value());

        const TParentsMap* parentsMap = getParents();
        auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }

        const auto* topicMeta = State_->FindTopicMeta(topicNode);
        if (!topicMeta) {
            ctx.AddError(TIssue(ctx.GetPosition(write.Pos()), TStringBuilder() << "Unknown topic `" << topicNode.Cluster().StringValue() << "`.`"
                                << topicNode.Path().StringValue() << "`"));
            return nullptr;
        }

        YQL_CLOG(INFO, ProviderPq) << "Optimize PqWriteTopic `" << topicNode.Cluster().StringValue() << "`.`" << topicNode.Path().StringValue() << "`";

        auto dqPqTopicSinkSettingsBuilder = Build<TDqPqTopicSink>(ctx, write.Pos());
        dqPqTopicSinkSettingsBuilder.Topic(topicNode);
        dqPqTopicSinkSettingsBuilder.Settings(BuildTopicWriteSettings(cluster, write.Pos(), ctx));
        dqPqTopicSinkSettingsBuilder.Token<TCoSecureParam>().Name().Build("cluster:default_" + cluster).Build();
        auto dqPqTopicSinkSettings = dqPqTopicSinkSettingsBuilder.Done();

        auto dqSink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Settings(dqPqTopicSinkSettings)
            .Index(dqUnion.Output().Index())
            .Done();

        TDqStage inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, topicNode.Pos());
        if (inputStage.Outputs()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(dqSink);

        auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Outputs(outputsBuilder.Done())
            .Done();

        auto dqQueryBuilder = Build<TDqQuery>(ctx, write.Pos());
        dqQueryBuilder.World(write.World());
        dqQueryBuilder.SinkStages().Add(dqStageWithSink).Build();

        optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

        return dqQueryBuilder.Done();
    }

private:
    TPqState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreatePqPhysicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqPhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
