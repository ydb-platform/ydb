#include "dq_opt_log.h"

#include "dq_opt.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_aggregate.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_annotation.h>


using namespace NYql::NNodes;

namespace NYql::NDq {

TExprBase DqRewriteAggregate(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoAggregate>()) {
        return node;
    }

    auto result = ExpandAggregate(true, node.Ptr(), ctx);
    YQL_ENSURE(result);

    return TExprBase(result);
}

// Take . Sort -> TopSort
// Take . Skip . Sort -> Take . Skip . TopSort
TExprBase DqRewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TParentsMap& parents) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }
    auto take = node.Cast<TCoTake>();

    auto maybeSkip = take.Input().Maybe<TCoSkip>();
    auto maybeSort = maybeSkip ? maybeSkip.Cast().Input().Maybe<TCoSort>() : take.Input().Maybe<TCoSort>();

    if (!maybeSort) {
        return node;
    }
    auto sort = maybeSort.Cast();

    if (!IsSingleConsumer(sort, parents)) {
        return node;
    }

    if (!IsDqPureExpr(take.Count())) {
        return node;
    }

    if (maybeSkip) {
        auto skip = maybeSkip.Cast();

        if (!IsSingleConsumer(skip, parents)) {
            return node;
        }

        if (!IsDqPureExpr(skip)) {
            return node;
        }

        return Build<TCoTake>(ctx, node.Pos())
            .Input<TCoSkip>()
                .Input<TCoTopSort>()
                    .Input(sort.Input())
                    .KeySelectorLambda(sort.KeySelectorLambda())
                    .SortDirections(sort.SortDirections())
                    .Count<TCoPlus>()
                        .Left(take.Count())
                        .Right(skip.Count())
                        .Build()
                    .Build()
                .Count(skip.Count())
                .Build()
            .Count(take.Count())
            .Done();
    }

    return Build<TCoTopSort>(ctx, node.Pos())
        .Input(sort.Input())
        .KeySelectorLambda(sort.KeySelectorLambda())
        .SortDirections(sort.SortDirections())
        .Count(take.Count())
        .Done();
}

/*
 * Enforce PARTITION COMPACT BY as it avoids generating join in favour of Fold1Map.
 */
TExprBase DqEnforceCompactPartition(TExprBase node, TExprList frames, TExprContext& ctx) {

    for (const auto &frameNode : frames.Ref().Children()) {
        YQL_ENSURE(TCoWinOnBase::Match(frameNode.Get()));

        auto frameSpec = frameNode->Child(0);
        if (frameSpec->Type() == TExprNode::List) {
            TVector<TExprBase> values;
            bool compact = false;

            for (const auto& setting : frameSpec->Children()) {
                const auto settingName = setting->Head().Content();
                if (settingName == "compact") {
                    compact = true;
                    break;
                }
                values.push_back(TExprBase(setting));
            }

            if (!compact) {
                auto newFrameSpec = Build<TExprList>(ctx, frameNode->Pos())
                    .Add(values)
                    .Add<TExprList>()
                        .Add<TCoAtom>()
                        .Value("compact")
                        .Build()
                    .Build()
                .Done();

                TNodeOnNodeOwnedMap replaces;
                replaces[frameNode->Child(0)] = newFrameSpec.Ptr();
                node = TExprBase(ctx.ReplaceNodes(node.Ptr(), replaces));
            }
        }
    }

    return node;
}

TExprBase DqExpandWindowFunctions(TExprBase node, TExprContext& ctx, bool enforceCompact) {
    if (node.Maybe<TCoCalcOverWindowBase>() || node.Maybe<TCoCalcOverWindowGroup>()) {
        if (enforceCompact) {
            auto calcs = ExtractCalcsOverWindow(node.Ptr(), ctx);
            bool changed = false;
            for (auto& c : calcs) {
                TCoCalcOverWindowTuple win(c);
                auto enforced = DqEnforceCompactPartition(node, win.Frames(), ctx);
                changed = changed || (enforced.Raw() != node.Raw());
                node = enforced;
            }

            if (changed) {
                return node;
            }
        }

        return TExprBase(ExpandCalcOverWindow(node.Ptr(), ctx));
    } else {
        return node;
    }
}

static void CollectSinkStages(const NNodes::TDqQuery& dqQuery, THashSet<TExprNode::TPtr, TExprNode::TPtrHash>& sinkStages) {
    for (const auto& stage : dqQuery.SinkStages()) {
        sinkStages.insert(stage.Ptr());
    }
}

NNodes::TExprBase DqMergeQueriesWithSinks(NNodes::TExprBase dqQueryNode, TExprContext& ctx) {
    NNodes::TDqQuery dqQuery = dqQueryNode.Cast<NNodes::TDqQuery>();

    THashSet<TExprNode::TPtr, TExprNode::TPtrHash> sinkStages;
    CollectSinkStages(dqQuery, sinkStages);
    TOptimizeExprSettings settings{nullptr};
    settings.VisitLambdas = false;
    bool deletedDqQueryChild = false;
    TExprNode::TPtr newDqQueryNode;
    auto status = OptimizeExpr(dqQueryNode.Ptr(), newDqQueryNode, [&sinkStages, &deletedDqQueryChild](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        for (ui32 childIndex = 0; childIndex < node->ChildrenSize(); ++childIndex) {
            TExprNode* child = node->Child(childIndex);
            if (child->IsCallable(NNodes::TDqQuery::CallableName())) {
                NNodes::TDqQuery dqQueryChild(child);
                CollectSinkStages(dqQueryChild, sinkStages);
                deletedDqQueryChild = true;
                return ctx.ChangeChild(*node, childIndex, dqQueryChild.World().Ptr());
            }
        }
        return node;
    }, ctx, settings);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error, "Failed to merge DqQuery nodes: " << status);

    if (deletedDqQueryChild) {
        auto dqQueryBuilder = Build<TDqQuery>(ctx, dqQuery.Pos());
        dqQueryBuilder.World(newDqQueryNode->ChildPtr(TDqQuery::idx_World));

        auto sinkStagesBuilder = dqQueryBuilder.SinkStages();
        for (const TExprNode::TPtr& stage : sinkStages) {
            sinkStagesBuilder.Add(stage);
        }
        sinkStagesBuilder.Build();

        return dqQueryBuilder.Done();
    }
    return dqQueryNode;
}

NNodes::TMaybeNode<NNodes::TExprBase> DqUnorderedInStage(NNodes::TExprBase node,
    const std::function<bool(const TExprNode*)>& stopTraverse, TExprContext& ctx, TTypeAnnotationContext* typeCtx)
{
    auto stage = node.Cast<TDqStageBase>();

    TExprNode::TPtr newProgram;
    auto status = LocalUnorderedOptimize(stage.Program().Ptr(), newProgram, stopTraverse, ctx, typeCtx);
    if (status.Level == IGraphTransformer::TStatus::Error) {
        return {};
    }

    if (stage.Program().Ptr() != newProgram) {
        return NNodes::TExprBase(ctx.ChangeChild(node.Ref(), TDqStageBase::idx_Program, std::move(newProgram)));
    }

    return node;
}

NNodes::TExprBase DqFlatMapOverExtend(NNodes::TExprBase node, TExprContext& ctx)
{
    auto maybeFlatMap = node.Maybe<TCoFlatMapBase>();
    if (!maybeFlatMap) {
        return node;
    }
    auto flatMap = maybeFlatMap.Cast();
    if (!flatMap.Input().Maybe<TCoExtendBase>()) {
        return node;
    }

    bool hasDqConnection = false;;
    auto input = flatMap.Input();
    for (auto child: input.Ref().Children()) {
        hasDqConnection |= !!TExprBase{child}.Maybe<TDqConnection>();
    }

    if (!hasDqConnection) {
        return node;
    }

    const bool ordered = flatMap.Maybe<TCoOrderedFlatMap>() && !input.Maybe<TCoExtend>();
    TExprNode::TListType extendChildren;
    for (auto child: input.Ref().Children()) {
        extendChildren.push_back(ctx.Builder(child->Pos())
            .Callable(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                .Add(0, child)
                .Add(1, flatMap.Lambda().Ptr())
            .Seal()
            .Build());
    }
    TStringBuf extendName = input.Maybe<TCoMerge>()
        ? TCoMerge::CallableName()
        : (ordered ? TCoOrderedExtend::CallableName() : TCoExtend::CallableName());

    auto res = ctx.NewCallable(node.Pos(), extendName, std::move(extendChildren));
    return TExprBase(res);
}

}
