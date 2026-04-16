#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

bool SubtreeContains(const TExprNode::TPtr& root, const TExprNode* needle) {
    if (!root) {
        return false;
    }
    return !!NYql::FindNode(root, [needle](const TExprNode::TPtr& n) { return n.Get() == needle; });
}

std::optional<ui64> TryFindOuterLiteralTakeLimit(const TExprNode* start, const TParentsMap* parents) {
    if (!parents || !start) {
        return std::nullopt;
    }

    THashSet<const TExprNode*> visited;
    TDeque<const TExprNode*> queue;
    queue.push_back(start);

    while (!queue.empty()) {
        const auto* cur = queue.front();
        queue.pop_front();
        if (!cur || !visited.insert(cur).second) {
            continue;
        }

        auto it = parents->find(cur);
        if (it == parents->end()) {
            continue;
        }

        for (const auto& parentPtr : it->second) {
            const auto parent = TExprBase(parentPtr);
            if (!parent.Raw()) {
                continue;
            }

            if (parent.Maybe<TCoTake>()) {
                const auto take = parent.Cast<TCoTake>();
                if (SubtreeContains(take.Input().Ptr(), start)) {
                    if (const auto lit = take.Count().Maybe<TCoUint64>()) {
                        return FromString<ui64>(lit.Cast().Literal().Value());
                    }
                }
            }

            queue.push_back(parent.Raw());
        }
    }

    return std::nullopt;
}

bool TryGetSingleKeyColumnName(const TCoAggregateCombine& aggCombine, TString& outKey) {
    if (aggCombine.Keys().Size() != 1) {
        return false;
    }
    const auto key = aggCombine.Keys().Item(0);
    if (!key.Maybe<TCoAtom>()) {
        return false;
    }
    outKey = TString(key.Cast<TCoAtom>().StringValue());
    return !outKey.empty();
}

bool ColumnNameEqualsKey(const TCoAggregateTuple& tuple, const TString& key) {
    const auto col = tuple.ColumnName();
    if (col.Maybe<TCoAtom>()) {
        return col.Cast<TCoAtom>().StringValue() == key;
    }
    return false;
}

bool TraitIsSomeOnMember(const TCoAggregateTuple& tuple, const TString& key) {
    if (!tuple.Trait().Maybe<TCoAggApply>()) {
        return false;
    }
    const auto apply = tuple.Trait().Cast<TCoAggApply>();
    if (apply.Name().StringValue() != "some") {
        return false;
    }
    const auto extractor = apply.Extractor();
    if (!extractor.Body().Maybe<TCoMember>()) {
        return false;
    }
    const auto member = extractor.Body().Cast<TCoMember>();
    return member.Name().StringValue() == key;
}

bool IsSimpleSingleColumnDistinctOverBareOlapRead(const TCoAggregateCombine& aggCombine, TString& outKey) {
    if (!aggCombine.Input().Maybe<TKqpReadOlapTableRanges>()) {
        return false;
    }

    TString keyColumn;
    if (!TryGetSingleKeyColumnName(aggCombine, keyColumn)) {
        return false;
    }

    if (aggCombine.Handlers().Size() != 1) {
        return false;
    }

    const auto tuple = aggCombine.Handlers().Item(0).Cast<TCoAggregateTuple>();
    if (tuple.DistinctName()) {
        if (TString(tuple.DistinctName().Cast().Value()) != keyColumn) {
            return false;
        }
    }

    if (!ColumnNameEqualsKey(tuple, keyColumn)) {
        return false;
    }

    if (!TraitIsSomeOnMember(tuple, keyColumn)) {
        return false;
    }

    outKey = std::move(keyColumn);
    return true;
}

bool ProcessBodyHasOlapDistinctOrAgg(const TCoLambda& process) {
    const auto pred = [](const TExprNode::TPtr& n) {
        return TKqpOlapDistinct::Match(n.Get()) || TKqpOlapAgg::Match(n.Get());
    };
    return !!NYql::FindNode(process.Body().Ptr(), pred);
}

const TExprNode* FindEnclosingPhysicalTx(const TExprNode* start, const TParentsMap* parents) {
    if (!start || !parents) {
        return nullptr;
    }
    THashSet<const TExprNode*> visited;
    TDeque<const TExprNode*> queue;
    queue.push_back(start);
    while (!queue.empty()) {
        const auto* cur = queue.front();
        queue.pop_front();
        if (!cur || !visited.insert(cur).second) {
            continue;
        }
        auto it = parents->find(cur);
        if (it == parents->end()) {
            continue;
        }
        for (const TExprNode* p : it->second) {
            if (TKqpPhysicalTx::Match(p)) {
                return p;
            }
            queue.push_back(p);
        }
    }
    return nullptr;
}

std::optional<ui64> TryUniformLiteralTakeLimitInPhysicalTx(const TExprNode* physicalTx) {
    if (!physicalTx || !TKqpPhysicalTx::Match(physicalTx)) {
        return std::nullopt;
    }
    THashSet<ui64> limits;
    VisitExpr(*physicalTx, [&](const TExprNode& n) {
        const TExprBase expr(&n);
        if (expr.Maybe<TCoTake>()) {
            const auto take = expr.Cast<TCoTake>();
            if (const auto lit = take.Count().Maybe<TCoUint64>()) {
                limits.insert(FromString<ui64>(lit.Cast().Literal().Value()));
            }
        }
        return true;
    });
    if (limits.size() == 1) {
        return *limits.begin();
    }
    return std::nullopt;
}

bool PathToBlockOlapReadViaAllowedWrappers(const TExprBase& cur, const TExprNode* targetRead) {
    if (cur.Raw() == targetRead) {
        return true;
    }
    if (cur.Maybe<TCoToFlow>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoToFlow>().Input(), targetRead);
    }
    if (cur.Maybe<TCoWideFromBlocks>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoWideFromBlocks>().Input(), targetRead);
    }
    if (cur.Maybe<TCoFromFlow>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoFromFlow>().Input(), targetRead);
    }
    if (cur.Maybe<TCoWideMap>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoWideMap>().Input(), targetRead);
    }
    if (cur.Maybe<TCoWideToBlocks>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoWideToBlocks>().Input(), targetRead);
    }
    if (cur.Maybe<TCoNarrowMap>()) {
        return PathToBlockOlapReadViaAllowedWrappers(cur.Cast<TCoNarrowMap>().Input(), targetRead);
    }
    return false;
}

TExprBase RebuildCombineInputReplacingBlockRead(
    const TExprBase& input,
    const TKqpBlockReadOlapTableRanges& oldRead,
    const TKqpBlockReadOlapTableRanges& newRead,
    TExprContext& ctx,
    TPositionHandle pos)
{
    if (input.Raw() == oldRead.Raw()) {
        return newRead;
    }
    if (input.Maybe<TCoToFlow>()) {
        const auto inner = input.Cast<TCoToFlow>().Input();
        return Build<TCoToFlow>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(inner, oldRead, newRead, ctx, pos))
            .Done();
    }
    if (input.Maybe<TCoWideFromBlocks>()) {
        const auto inner = input.Cast<TCoWideFromBlocks>().Input();
        return Build<TCoWideFromBlocks>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(inner, oldRead, newRead, ctx, pos))
            .Done();
    }
    if (input.Maybe<TCoFromFlow>()) {
        const auto inner = input.Cast<TCoFromFlow>().Input();
        return Build<TCoFromFlow>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(inner, oldRead, newRead, ctx, pos))
            .Done();
    }
    if (input.Maybe<TCoWideMap>()) {
        const auto wm = input.Cast<TCoWideMap>();
        return Build<TCoWideMap>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(wm.Input(), oldRead, newRead, ctx, pos))
            .Lambda(wm.Lambda())
            .Done();
    }
    if (input.Maybe<TCoWideToBlocks>()) {
        const auto wtb = input.Cast<TCoWideToBlocks>();
        return Build<TCoWideToBlocks>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(wtb.Input(), oldRead, newRead, ctx, pos))
            .Done();
    }
    if (input.Maybe<TCoNarrowMap>()) {
        const auto nm = input.Cast<TCoNarrowMap>();
        return Build<TCoNarrowMap>(ctx, pos)
            .Input(RebuildCombineInputReplacingBlockRead(nm.Input(), oldRead, newRead, ctx, pos))
            .Lambda(nm.Lambda())
            .Done();
    }
    YQL_ENSURE(false, "Unexpected combine input while rebuilding OLAP block read");
    return input;
}

bool IsIdentityProcessLambda(const TCoLambda& lam) {
    if (lam.Args().Size() != 1) {
        return false;
    }
    const auto arg0 = lam.Args().Arg(0);
    if (!arg0.Maybe<TCoArgument>() || !lam.Body().Maybe<TCoArgument>()) {
        return false;
    }
    return lam.Body().Cast<TCoArgument>().Name() == arg0.Cast<TCoArgument>().Name();
}

std::optional<TExprBase> TryReplaceBlockOlapReadInputWithDistinct(
    TExprBase combineInput,
    const TExprNode* combineForParents,
    TExprContext& ctx,
    const TParentsMap* parentsMap,
    TPositionHandle pos)
{
    const auto blockReads = FindNodes(combineInput.Ptr(), [](const TExprNode::TPtr& n) {
        return TKqpBlockReadOlapTableRanges::Match(n.Get());
    });
    if (blockReads.size() != 1) {
        return std::nullopt;
    }
    const auto read = TExprBase(blockReads[0]).Cast<TKqpBlockReadOlapTableRanges>();
    if (!PathToBlockOlapReadViaAllowedWrappers(combineInput, read.Raw())) {
        return std::nullopt;
    }

    if (read.Columns().Size() != 1) {
        return std::nullopt;
    }
    const auto keyAtom = read.Columns().Item(0).Maybe<TCoAtom>();
    if (!keyAtom) {
        return std::nullopt;
    }
    const TString keyColumn = TString(keyAtom.Cast().StringValue());
    if (keyColumn.empty()) {
        return std::nullopt;
    }

    if (!IsIdentityProcessLambda(read.Process())) {
        return std::nullopt;
    }

    if (NYql::HasSetting(read.Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return std::nullopt;
    }

    if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
        return std::nullopt;
    }

    auto olapDistinct = Build<TKqpOlapDistinct>(ctx, pos)
        .Input(read.Process().Args().Arg(0))
        .Key().Build(keyColumn)
        .Done();

    auto olapDistinctLambda = Build<TCoLambda>(ctx, pos)
        .Args({"olap_dist_row"})
        .Body<TExprApplier>()
            .Apply(olapDistinct)
            .With(read.Process().Args().Arg(0), "olap_dist_row")
            .Build()
        .Done();

    const auto newProcessLambda = ctx.FuseLambdas(olapDistinctLambda.Ref(), read.Process().Ref());

    auto settings = TKqpReadTableSettings::Parse(read);
    std::optional<ui64> maybeLimit = TryFindOuterLiteralTakeLimit(combineForParents, parentsMap);
    if (!maybeLimit) {
        if (const auto* tx = FindEnclosingPhysicalTx(combineForParents, parentsMap)) {
            maybeLimit = TryUniformLiteralTakeLimitInPhysicalTx(tx);
        }
    }
    if (maybeLimit && !settings.ItemsLimit) {
        const auto limitNode = Build<TCoUint64>(ctx, pos)
            .Literal<TCoAtom>()
            .Value(ToString(*maybeLimit))
            .Build()
            .Done();
        settings.SetItemsLimit(limitNode.Ptr());
    }

    const auto newSettings = settings.BuildNode(ctx, read.Pos());

    const auto newRead = Build<TKqpBlockReadOlapTableRanges>(ctx, read.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(newSettings)
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();

    return RebuildCombineInputReplacingBlockRead(combineInput, read, newRead, ctx, pos);
}

} // namespace

TExprBase KqpPushOlapDistinct(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, const TParentsMap* parentsMap) {
    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return node;
    }
    const auto pragmaDistinct = kqpCtx.Config->OptEnableOlapPushdownDistinct.Get();
    if (!kqpCtx.Config->GetEnableOlapPushdownDistinct() && !(pragmaDistinct && pragmaDistinct.GetRef())) {
        return node;
    }

    if (node.Maybe<TDqPhyHashCombine>()) {
        const auto combine = node.Cast<TDqPhyHashCombine>();
        if (const auto newInput = TryReplaceBlockOlapReadInputWithDistinct(
                combine.Input(), combine.Raw(), ctx, parentsMap, node.Pos()))
        {
            return Build<TDqPhyHashCombine>(ctx, node.Pos())
                .Input(*newInput)
                .MemLimit(combine.MemLimit())
                .KeyExtractor(combine.KeyExtractor())
                .InitHandler(combine.InitHandler())
                .UpdateHandler(combine.UpdateHandler())
                .FinishHandler(combine.FinishHandler())
                .Done();
        }
        return node;
    }

    if (node.Maybe<TCoCombineCore>()) {
        const auto combine = node.Cast<TCoCombineCore>();
        if (const auto newInput = TryReplaceBlockOlapReadInputWithDistinct(
                combine.Input(), combine.Raw(), ctx, parentsMap, node.Pos()))
        {
            return Build<TCoCombineCore>(ctx, node.Pos())
                .Input(*newInput)
                .KeyExtractor(combine.KeyExtractor())
                .InitHandler(combine.InitHandler())
                .UpdateHandler(combine.UpdateHandler())
                .FinishHandler(combine.FinishHandler())
                .MemLimit(combine.MemLimit())
                .Done();
        }
        return node;
    }

    if (!node.Maybe<TCoAggregateCombine>()) {
        return node;
    }

    const auto aggCombine = node.Cast<TCoAggregateCombine>();

    TString keyColumn;
    if (!IsSimpleSingleColumnDistinctOverBareOlapRead(aggCombine, keyColumn)) {
        return node;
    }

    const auto read = aggCombine.Input().Cast<TKqpReadOlapTableRanges>();

    if (NYql::HasSetting(read.Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return node;
    }

    if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
        return node;
    }

    auto olapDistinct = Build<TKqpOlapDistinct>(ctx, node.Pos())
        .Input(read.Process().Args().Arg(0))
        .Key().Build(keyColumn)
        .Done();

    auto olapDistinctLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_dist_row"})
        .Body<TExprApplier>()
            .Apply(olapDistinct)
            .With(read.Process().Args().Arg(0), "olap_dist_row")
            .Build()
        .Done();

    const auto newProcessLambda = ctx.FuseLambdas(olapDistinctLambda.Ref(), read.Process().Ref());

    auto settings = TKqpReadTableSettings::Parse(read);
    if (const auto maybeLimit = TryFindOuterLiteralTakeLimit(aggCombine.Raw(), parentsMap)) {
        if (!settings.ItemsLimit) {
            const auto limitNode = Build<TCoUint64>(ctx, node.Pos())
                .Literal<TCoAtom>()
                .Value(ToString(*maybeLimit))
                .Build()
                .Done();
            settings.SetItemsLimit(limitNode.Ptr());
        }
    }

    const auto newSettings = settings.BuildNode(ctx, read.Pos());

    return Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(newSettings)
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
