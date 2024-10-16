#include "yql_co.h"
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_hopping.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

TExprNode::TPtr RewriteAsHoppingWindowFullOutput(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();

    NHopping::EnsureNotDistinct(aggregate);

    const auto maybeHopTraits = NHopping::ExtractHopTraits(aggregate, ctx, false);
    if (!maybeHopTraits) {
        return nullptr;
    }
    const auto hopTraits = *maybeHopTraits;

    const auto aggregateInputType = GetSeqItemType(*aggregate.Ptr()->Head().GetTypeAnn()).Cast<TStructExprType>();
    NHopping::TKeysDescription keysDescription(*aggregateInputType, aggregate.Keys(), hopTraits.Column);

    const auto keyLambda = keysDescription.GetKeySelector(ctx, pos, aggregateInputType);
    const auto timeExtractorLambda = NHopping::BuildTimeExtractor(hopTraits.Traits, ctx);
    const auto initLambda = NHopping::BuildInitHopLambda(aggregate, ctx);
    const auto updateLambda = NHopping::BuildUpdateHopLambda(aggregate, ctx);
    const auto saveLambda = NHopping::BuildSaveHopLambda(aggregate, ctx);
    const auto loadLambda = NHopping::BuildLoadHopLambda(aggregate, ctx);
    const auto mergeLambda = NHopping::BuildMergeHopLambda(aggregate, ctx);
    const auto finishLambda = NHopping::BuildFinishHopLambda(aggregate, keysDescription.GetActualGroupKeys(), hopTraits.Column, ctx);

    const auto streamArg = Build<TCoArgument>(ctx, pos).Name("stream").Done();
    auto multiHoppingCoreBuilder = Build<TCoMultiHoppingCore>(ctx, pos)
        .KeyExtractor(keyLambda)
        .TimeExtractor(timeExtractorLambda)
        .Hop(hopTraits.Traits.Hop())
        .Interval(hopTraits.Traits.Interval())
        .Delay(hopTraits.Traits.Delay())
        .DataWatermarks(hopTraits.Traits.DataWatermarks())
        .InitHandler(initLambda)
        .UpdateHandler(updateLambda)
        .MergeHandler(mergeLambda)
        .FinishHandler(finishLambda)
        .SaveHandler(saveLambda)
        .LoadHandler(loadLambda)
        .template WatermarkMode<TCoAtom>().Build(ToString(false));

    return Build<TCoPartitionsByKeys>(ctx, pos)
        .Input(aggregate.Input())
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
                .Stream(Build<TCoMap>(ctx, pos)
                    .Input(multiHoppingCoreBuilder
                        .template Input<TCoIterator>()
                            .List(streamArg)
                            .Build()
                        .Done())
                    .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                    .Done())
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr RewriteAsHoppingWindow(TExprNode::TPtr node, TExprContext& ctx) {
    const auto aggregate = TCoAggregate(node);

    if (!IsPureIsolatedLambda(*aggregate.Ptr())) {
        return nullptr;
    }

    if (!GetSetting(aggregate.Settings().Ref(), "hopping")) {
        return nullptr;
    }

    auto result = RewriteAsHoppingWindowFullOutput(aggregate, ctx);
    if (!result) {
        return result;
    }

    auto outputColumnSetting = GetSetting(aggregate.Settings().Ref(), "output_columns");
    if (!outputColumnSetting) {
        return result;
    }

    return Build<TCoExtractMembers>(ctx, aggregate.Pos())
        .Input(result)
        .Members(outputColumnSetting->ChildPtr(1))
        .Done()
        .Ptr();
}

} // namespace

void RegisterCoFinalCallables(TCallableOptimizerMap& map) {
    map["Aggregate"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& /*optCtx*/) {
        if (auto hopping = RewriteAsHoppingWindow(node, ctx)) {
            YQL_CLOG(DEBUG, Core) << "RewriteAsHoppingWindow";
            return hopping;
        }

        return node;
    };

    map["UnorderedSubquery"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        Y_UNUSED(optCtx);
        if (node->Head().IsCallable("Sort")) {
            if (!WarnUnroderedSubquery(*node, ctx)) {
                return TExprNode::TPtr();
            }
        }
        return ctx.RenameNode(*node, "Unordered");
    };
}

}
