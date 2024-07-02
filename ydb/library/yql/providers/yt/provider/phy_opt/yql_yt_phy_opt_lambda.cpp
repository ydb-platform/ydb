#include "yql_yt_phy_opt.h"

namespace NYql {

using namespace NNodes;

TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambdaNoArg(TExprBase content, TExprContext& ctx) const {
    if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        content = Build<TCoToFlow>(ctx, content.Pos()).Input(content).Done();
    } else {
        content = Build<TCoToStream>(ctx, content.Pos()).Input(content).Done();
    }

    return Build<TCoLambda>(ctx, content.Pos()).Args({}).Body(content).Done();
}


template<>
TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambda<false>(TCoLambda lambda, bool useFlow, TExprContext& ctx) const
{
    if (useFlow) {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"flow"})
            .Body<TCoToFlow>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoFromFlow>(0)
                        .Input("flow")
                    .Build()
                .Build()
                .FreeArgs()
                    .Add<TCoDependsOn>()
                        .Input("flow")
                    .Build()
                .Build()
            .Build()
        .Done();
    } else {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoToStream>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
            .Build()
        .Done();
    }
}

template<>
TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambda<true>(TCoLambda lambda, bool useFlow, TExprContext& ctx) const
{
    if (useFlow) {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"flow"})
            .Body<TCoToFlow>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoForwardList>(0)
                        .Stream("flow")
                    .Build()
                .Build()
                .FreeArgs()
                    .Add<TCoDependsOn>()
                        .Input("flow")
                    .Build()
                .Build()
            .Build()
        .Done();
    } else {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoToStream>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoForwardList>(0)
                        .Stream("stream")
                    .Build()
                .Build()
            .Build()
        .Done();
    }
}

}  // namespace NYql
