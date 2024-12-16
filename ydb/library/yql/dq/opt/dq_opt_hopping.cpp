#include "dq_opt_hopping.h"

#include <yql/essentials/core/yql_aggregate_expander.h>
#include <yql/essentials/core/yql_opt_hopping.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <yql/essentials/core/dq_integration/yql_dq_optimization.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>

#include <util/generic/bitmap.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NHopping;
using namespace NYql::NNodes;

namespace {

TExprNode::TPtr WrapToShuffle(
    const TKeysDescription& keysDescription,
    const TCoAggregate& aggregate,
    const TDqConnection& input,
    TExprContext& ctx)
{
    auto pos = aggregate.Pos();

    TDqStageBase mappedInput = input.Output().Stage();
    if (keysDescription.NeedPickle()) {
        mappedInput = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add<TDqCnMap>()
                    .Output()
                        .Stage(input.Output().Stage())
                        .Index(input.Output().Index())
                        .Build()
                    .Build()
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoMap>()
                    .Input("stream")
                    .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                .Build()
            .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, pos))
            .Done();
    }

    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(mappedInput)
            .Index().Value("0").Build()
            .Build()
        .KeyColumns()
            .Add(keysDescription.GetKeysList(ctx, pos))
            .Build()
        .Done()
        .Ptr();
}

TMaybe<bool> BuildWatermarkMode(
    const TCoAggregate& aggregate,
    const TCoHoppingTraits& hoppingTraits,
    TExprContext& ctx,
    bool analyticsMode,
    bool defaultWatermarksMode,
    bool syncActor)
{
    const bool enableWatermarks = !analyticsMode &&
        defaultWatermarksMode &&
        hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2";
    if (enableWatermarks && syncActor) {
        ctx.AddError(TIssue(ctx.GetPosition(aggregate.Pos()), "Watermarks should be used only with async compute actor"));
        return Nothing();
    }

    if (hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2" && !enableWatermarks) {
        ctx.AddError(TIssue(
            ctx.GetPosition(aggregate.Pos()),
            "HoppingWindow requires watermarks to be enabled. If you don't want to do that, you can use HOP instead."));
        return Nothing();
    }

    return enableWatermarks;
}

TMaybeNode<TExprBase> RewriteAsHoppingWindowFullOutput(
    const TExprBase node,
    TExprContext& ctx,
    const TDqConnection& input,
    bool analyticsMode,
    TDuration lateArrivalDelay,
    bool defaultWatermarksMode,
    bool syncActor) {
    const auto aggregate = node.Cast<TCoAggregate>();
    const auto pos = aggregate.Pos();

    YQL_CLOG(DEBUG, ProviderDq) << "OptimizeStreamingAggregate";

    EnsureNotDistinct(aggregate);

    const auto maybeHopTraits = ExtractHopTraits(aggregate, ctx, analyticsMode);
    if (!maybeHopTraits) {
        return nullptr;
    }
    const auto hopTraits = *maybeHopTraits;

    const auto aggregateInputType = GetSeqItemType(*node.Ptr()->Head().GetTypeAnn()).Cast<TStructExprType>();
    TKeysDescription keysDescription(*aggregateInputType, aggregate.Keys(), hopTraits.Column);

    if (keysDescription.NeedPickle()) {
        return Build<TCoMap>(ctx, pos)
            .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
            .Input<TCoAggregate>()
                .InitFrom(aggregate)
                .Input<TCoMap>()
                    .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                    .Input(input)
                .Build()
                .Settings(RemoveSetting(aggregate.Settings().Ref(), "output_columns", ctx))
            .Build()
            .Done();
    }

    const auto keyLambda = keysDescription.GetKeySelector(ctx, pos, aggregateInputType);
    const auto timeExtractorLambda = BuildTimeExtractor(hopTraits.Traits, ctx);
    const auto initLambda = BuildInitHopLambda(aggregate, ctx);
    const auto updateLambda = BuildUpdateHopLambda(aggregate, ctx);
    const auto saveLambda = BuildSaveHopLambda(aggregate, ctx);
    const auto loadLambda = BuildLoadHopLambda(aggregate, ctx);
    const auto mergeLambda = BuildMergeHopLambda(aggregate, ctx);
    const auto finishLambda = BuildFinishHopLambda(aggregate, keysDescription.GetActualGroupKeys(), hopTraits.Column, ctx);
    const auto enableWatermarks = BuildWatermarkMode(aggregate, hopTraits.Traits, ctx, analyticsMode, defaultWatermarksMode, syncActor);
    if (!enableWatermarks) {
        return nullptr;
    }

    const auto streamArg = Build<TCoArgument>(ctx, pos).Name("stream").Done();
    auto multiHoppingCoreBuilder = Build<TCoMultiHoppingCore>(ctx, pos)
        .KeyExtractor(keyLambda)
        .TimeExtractor(timeExtractorLambda)
        .Hop(hopTraits.Traits.Hop())
        .Interval(hopTraits.Traits.Interval())
        .DataWatermarks(hopTraits.Traits.DataWatermarks())
        .InitHandler(initLambda)
        .UpdateHandler(updateLambda)
        .MergeHandler(mergeLambda)
        .FinishHandler(finishLambda)
        .SaveHandler(saveLambda)
        .LoadHandler(loadLambda)
        .template WatermarkMode<TCoAtom>().Build(ToString(*enableWatermarks));

    if (*enableWatermarks) {
        const auto hop = TDuration::MicroSeconds(hopTraits.Hop);
        multiHoppingCoreBuilder.template Delay<TCoInterval>()
            .Literal().Build(ToString(Max(hop, lateArrivalDelay).MicroSeconds()))
            .Build();
    } else {
        multiHoppingCoreBuilder.Delay(hopTraits.Traits.Delay());
    }

    if (analyticsMode) {
        return Build<TCoPartitionsByKeys>(ctx, node.Pos())
            .Input(input.Ptr())
            .KeySelectorLambda(keyLambda)
            .SortDirections<TCoBool>()
                    .Literal()
                    .Value("true")
                    .Build()
                .Build()
            .SortKeySelectorLambda(timeExtractorLambda)
            .ListHandlerLambda()
                .Args(streamArg)
                .template Body<TCoForwardList>()
                    .Stream(multiHoppingCoreBuilder
                        .template Input<TCoIterator>()
                            .List(streamArg)
                            .Build()
                        .Done())
                    .Build()
                .Build()
            .Done();
    } else {
        auto wrappedInput = input.Ptr();
        if (!keysDescription.MemberKeys.empty()) {
            // Shuffle input connection by keys
            wrappedInput = WrapToShuffle(keysDescription, aggregate, input, ctx);
            if (!wrappedInput) {
                return nullptr;
            }
        }

        const auto stage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(wrappedInput)
                .Build()
            .Program()
                .Args(streamArg)
                .Body<TCoMap>()
                    .Input(multiHoppingCoreBuilder
                        .template Input<TCoFromFlow>()
                            .Input(streamArg)
                            .Build()
                        .Done())
                    .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();

        return Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage(stage)
                .Index().Build(0)
                .Build()
            .Done();
    }
}

} // namespace

namespace NYql::NDq::NHopping {

TMaybeNode<TExprBase> RewriteAsHoppingWindow(
    const TExprBase node,
    TExprContext& ctx,
    const TDqConnection& input,
    bool analyticsMode,
    TDuration lateArrivalDelay,
    bool defaultWatermarksMode,
    bool syncActor)
{
    auto result = RewriteAsHoppingWindowFullOutput(node, ctx, input, analyticsMode, lateArrivalDelay, defaultWatermarksMode, syncActor);
    if (!result) {
        return result;
    }

    const auto aggregate = node.Cast<TCoAggregate>();
    auto outputColumnSetting = GetSetting(aggregate.Settings().Ref(), "output_columns");
    if (!outputColumnSetting) {
        return result;
    }

    return Build<TCoExtractMembers>(ctx, node.Pos())
        .Input(result.Cast())
        .Members(outputColumnSetting->ChildPtr(1))
        .Done();
}

} // NYql::NDq::NHopping
