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

class TS3PhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TS3PhysicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TS3PhysicalOptProposalTransformer::name)
        AddHandler(0, &TS3WriteObject::Match, HNDL(S3WriteObject));
#undef HNDL

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> S3WriteObject(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        auto write = node.Cast<TS3WriteObject>();
        if (!TDqCnUnionAll::Match(write.Input().Raw())) { // => this code is not for RTMR mode.
            return node;
        }

        const auto& targetNode = write.Target();
        const auto& cluster = write.DataSink().Cluster().StringValue();

        const TParentsMap* parentsMap = getParents();
        auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }


        YQL_CLOG(INFO, ProviderS3) << "Optimize S3WriteObject `" << cluster << "`.`" << targetNode.Path().StringValue() << "`";

        const auto token = "cluster:default_" + cluster;

        auto dqSink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Index(dqUnion.Output().Index())
            .Settings<TS3SinkSettings>()
                .Path(write.Target().Path())
                .Settings<TCoNameValueTupleList>().Build()
                .Token<TCoSecureParam>()
                    .Name().Build(token)
                    .Build()
                .Build()
            .Done();

        auto inputStage = dqUnion.Output().Stage().Cast<TDqStage>();
        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, targetNode.Pos());
        if (inputStage.Outputs()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(dqSink);

        auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Program<TCoLambda>()
                .Args({"input"})
                .Body<TS3SinkOutput>()
                    .Input<TExprApplier>()
                        .Apply(inputStage.Program()).With(0, "input")
                        .Build()
                    .Format(write.Target().Format())
                    .Build()
                .Build()
            .Outputs(outputsBuilder.Done())
            .Done();

        auto dqQueryBuilder = Build<TDqQuery>(ctx, write.Pos());
        dqQueryBuilder.World(write.World());
        dqQueryBuilder.SinkStages().Add(dqStageWithSink).Build();

        optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

        return dqQueryBuilder.Done();
    }

private:
    TS3State::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3PhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql

