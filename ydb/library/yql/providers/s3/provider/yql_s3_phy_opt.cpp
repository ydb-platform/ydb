#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

TExprNode::TPtr GetPartitionBy(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom("partitionedby")) {
            return settings.ChildPtr(i);
        }
    }

    return {};
}

TExprNode::TPtr GetCompression(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom("compression")) {
            return settings.ChildPtr(i);
        }
    }

    return {};
}

TExprNode::TListType GetPartitionKeys(const TExprNode::TPtr& partBy) {
    if (partBy) {
        auto children = partBy->ChildrenList();
        children.erase(children.cbegin());
        return children;
    }

    return {};
}

class TS3PhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TS3PhysicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TS3PhysicalOptProposalTransformer::name)
        AddHandler(0, &TS3WriteObject::Match, HNDL(S3WriteObject));
#undef HNDL
    }

    TMaybeNode<TExprBase> S3WriteObject(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        const auto& write = node.Cast<TS3WriteObject>();
        const auto& targetNode = write.Target();
        const auto& cluster = write.DataSink().Cluster().StringValue();
        const auto token = "cluster:default_" + cluster;
        auto partBy = GetPartitionBy(write.Target().Settings().Ref());
        auto keys = GetPartitionKeys(partBy);

        auto sinkSettingsBuilder = Build<TExprList>(ctx, targetNode.Pos());
        if (partBy)
            sinkSettingsBuilder.Add(std::move(partBy));
        if (auto compression = GetCompression(write.Target().Settings().Ref()))
            sinkSettingsBuilder.Add(std::move(compression));

        if (!FindNode(write.Input().Ptr(), [] (const TExprNode::TPtr& node) { return node->IsCallable(TCoDataSource::CallableName()); })) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite pure S3WriteObject `" << cluster << "`.`" << targetNode.Path().StringValue() << "` as stage with sink.";
            return keys.empty() ?
                Build<TDqQuery>(ctx, write.Pos())
                    .World(write.World())
                    .SinkStages()
                        .Add<TDqStage>()
                            .Inputs().Build()
                            .Program<TCoLambda>()
                                .Args({})
                                .Body<TS3SinkOutput>()
                                    .Input<TCoToFlow>()
                                        .Input(write.Input())
                                        .Build()
                                    .Format(write.Target().Format())
                                    .KeyColumns().Build()
                                    .Build()
                                .Build()
                            .Outputs<TDqStageOutputsList>()
                                .Add<TDqSink>()
                                    .DataSink(write.DataSink())
                                    .Index().Value("0").Build()
                                    .Settings<TS3SinkSettings>()
                                        .Path(write.Target().Path())
                                        .Settings(sinkSettingsBuilder.Done())
                                        .Token<TCoSecureParam>()
                                            .Name().Build(token)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings().Build()
                            .Build()
                        .Build()
                    .Done():
                Build<TDqQuery>(ctx, write.Pos())
                    .World(write.World())
                    .SinkStages()
                        .Add<TDqStage>()
                            .Inputs()
                                .Add<TDqCnHashShuffle>()
                                    .Output<TDqOutput>()
                                        .Stage<TDqStage>()
                                            .Inputs().Build()
                                            .Program<TCoLambda>()
                                                .Args({})
                                                .Body<TCoToFlow>()
                                                    .Input(write.Input())
                                                    .Build()
                                                .Build()
                                            .Settings().Build()
                                            .Build()
                                        .Index().Value("0", TNodeFlags::Default).Build()
                                        .Build()
                                    .KeyColumns().Add(keys).Build()
                                    .Build()
                                .Build()
                            .Program<TCoLambda>()
                                .Args({"in"})
                                .Body<TS3SinkOutput>()
                                    .Input("in")
                                    .Format(write.Target().Format())
                                    .KeyColumns().Add(keys).Build()
                                    .Build()
                                .Build()
                            .Outputs<TDqStageOutputsList>()
                                .Add<TDqSink>()
                                    .DataSink(write.DataSink())
                                    .Index().Value("0", TNodeFlags::Default).Build()
                                    .Settings<TS3SinkSettings>()
                                        .Path(write.Target().Path())
                                        .Settings(sinkSettingsBuilder.Done())
                                        .Token<TCoSecureParam>()
                                            .Name().Build(token)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings().Build()
                            .Build()
                        .Build()
                    .Done();
        }

        if (!TDqCnUnionAll::Match(write.Input().Raw())) {
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        const auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }

        YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << targetNode.Path().StringValue() << "` as sink.";

        const auto inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        const auto sink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Index(dqUnion.Output().Index())
            .Settings<TS3SinkSettings>()
                .Path(write.Target().Path())
                .Settings(sinkSettingsBuilder.Done())
                .Token<TCoSecureParam>()
                    .Name().Build(token)
                    .Build()
                .Build()
            .Done();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, targetNode.Pos());
        if (inputStage.Outputs() && keys.empty()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(sink);

        auto dqStageWithSink = keys.empty() ?
            Build<TDqStage>(ctx, inputStage.Pos())
                .InitFrom(inputStage)
                .Program<TCoLambda>()
                    .Args({"input"})
                    .Body<TS3SinkOutput>()
                        .Input<TExprApplier>()
                            .Apply(inputStage.Program()).With(0, "input")
                            .Build()
                        .Format(write.Target().Format())
                        .KeyColumns()
                            .Add(std::move(keys))
                            .Build()
                        .Build()
                    .Build()
                .Outputs(outputsBuilder.Done())
                .Done():
            Build<TDqStage>(ctx, inputStage.Pos())
                .Inputs()
                    .Add<TDqCnHashShuffle>()
                        .Output<TDqOutput>()
                            .Stage(inputStage)
                            .Index(dqUnion.Output().Index())
                            .Build()
                        .KeyColumns().Add(keys).Build()
                        .Build()
                    .Build()
                .Program<TCoLambda>()
                    .Args({"in"})
                    .Body<TS3SinkOutput>()
                        .Input("in")
                        .Format(write.Target().Format())
                        .KeyColumns().Add(std::move(keys)).Build()
                        .Build()
                    .Build()
                .Settings().Build()
                .Outputs(outputsBuilder.Done())
                .Done();

        return Build<TDqQuery>(ctx, write.Pos())
            .World(write.World())
            .SinkStages().Add(dqStageWithSink).Build()
            .Done();
    }

private:
    const TS3State::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3PhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql

