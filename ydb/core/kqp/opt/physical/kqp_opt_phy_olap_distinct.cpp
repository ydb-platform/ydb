#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <ydb/core/kqp/opt/physical/kqp_opt_phy.h>

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

void CollectKqpOlapSsaColumnNames(const TExprBase& root, THashSet<TString>& out) {
    VisitExpr(root.Ptr(), [&](const TExprNode::TPtr& n) {
        const TExprBase expr(n.Get());
        if (expr.Maybe<TKqpOlapApplyColumnArg>()) {
            out.insert(TString(expr.Cast<TKqpOlapApplyColumnArg>().ColumnName().StringValue()));
        } else if (expr.Maybe<TKqpOlapFilterExists>()) {
            out.insert(TString(expr.Cast<TKqpOlapFilterExists>().Column().StringValue()));
        }
        return true;
    });
}

bool InputChainEndsWithRowArg(TExprBase cur, const TCoArgument& rowArg) {
    if (cur.Raw() == rowArg.Raw()) {
        return true;
    }
    if (cur.Maybe<TKqpOlapFilter>()) {
        return InputChainEndsWithRowArg(cur.Cast<TKqpOlapFilter>().Input(), rowArg);
    }
    return false;
}

bool OlapFilterPredicatesUseOnlyFirstPkColumn(TStringBuf firstPkCol, const TCoLambda& process) {
    THashSet<TString> cols;
    CollectKqpOlapSsaColumnNames(TExprBase(process.Body().Ptr()), cols);
    if (cols.empty()) {
        return false;
    }
    for (const auto& c : cols) {
        if (c != firstPkCol) {
            return false;
        }
    }
    return true;
}

bool ReadColumnsListContains(const TKqpBlockReadOlapTableRanges& read, TStringBuf col) {
    for (ui32 i = 0; i < read.Columns().Size(); ++i) {
        if (read.Columns().Item(i).Maybe<TCoAtom>()) {
            if (read.Columns().Item(i).Cast<TCoAtom>().StringValue() == col) {
                return true;
            }
        }
    }
    return false;
}

bool ProcessAllowsDistinctPushWithOptionalFirstPkFilter(
    const TCoLambda& process,
    TStringBuf firstPkCol,
    const TKqpBlockReadOlapTableRanges& read,
    const TString& distinctCol)
{
    if (!ReadColumnsListContains(read, distinctCol)) {
        return false;
    }
    if (IsIdentityProcessLambda(process)) {
        return true;
    }
    if (process.Args().Size() != 1 || !process.Args().Arg(0).Maybe<TCoArgument>()) {
        return false;
    }
    const auto rowArg = process.Args().Arg(0).Cast<TCoArgument>();
    if (!process.Body().Maybe<TKqpOlapFilter>()) {
        return false;
    }
    const auto topFilter = process.Body().Cast<TKqpOlapFilter>();
    if (!InputChainEndsWithRowArg(topFilter.Input(), rowArg)) {
        return false;
    }
    if (!OlapFilterPredicatesUseOnlyFirstPkColumn(firstPkCol, process)) {
        return false;
    }
    if (!ReadColumnsListContains(read, TString{firstPkCol})) {
        return false;
    }
    return true;
}

TMaybe<TString> DistinctColFromKeyExtractor(const TCoLambda& keyExtractor) {
    if (keyExtractor.Args().Size() != 1 || !keyExtractor.Body().Maybe<TCoMember>()) {
        return Nothing();
    }
    const auto member = keyExtractor.Body().Cast<TCoMember>();
    if (member.Struct().Raw() != keyExtractor.Args().Arg(0).Raw()) {
        return Nothing();
    }
    return TString(member.Name().StringValue());
}

TMaybe<TString> TryGetDistinctColumnFromCombineKey(const TExprBase& combineNode) {
    if (combineNode.Maybe<TDqPhyHashCombine>()) {
        return DistinctColFromKeyExtractor(combineNode.Cast<TDqPhyHashCombine>().KeyExtractor());
    }
    if (combineNode.Maybe<TCoCombineCore>()) {
        return DistinctColFromKeyExtractor(combineNode.Cast<TCoCombineCore>().KeyExtractor());
    }
    return Nothing();
}

std::optional<TExprBase> TryReplaceBlockOlapReadInputWithDistinct(
    TExprBase combineInput,
    const TExprNode* combineForParents,
    TExprContext& ctx,
    const TParentsMap* parentsMap,
    TPositionHandle pos,
    const TKqpOptimizeContext& kqpCtx,
    const TExprBase& combineNode)
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

    const auto maybeDistinctCol = TryGetDistinctColumnFromCombineKey(combineNode);
    if (!maybeDistinctCol) {
        return std::nullopt;
    }
    const TString& keyColumn = *maybeDistinctCol;

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());
    if (tableData.Metadata->KeyColumnNames.empty()) {
        return std::nullopt;
    }
    const TStringBuf firstPkCol = tableData.Metadata->KeyColumnNames[0];

    if (!ProcessAllowsDistinctPushWithOptionalFirstPkFilter(
            read.Process(), firstPkCol, read, keyColumn))
    {
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
                combine.Input(), combine.Raw(), ctx, parentsMap, node.Pos(), kqpCtx, node))
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
                combine.Input(), combine.Raw(), ctx, parentsMap, node.Pos(), kqpCtx, node))
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

    return node;
}

namespace {

class TKqpPushOlapDistinctPhysicalQueryTransformer : public TSyncTransformerBase {
public:
    explicit TKqpPushOlapDistinctPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
        : KqpCtx(kqpCtx)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        for (;;) {
            TParentsMap parentsMap;
            GatherParents(*output, parentsMap);
            const auto combines = FindNodes(output, [](const TExprNode::TPtr& n) {
                return TDqPhyHashCombine::Match(n.Get()) || TCoCombineCore::Match(n.Get());
            });
            bool changed = false;
            for (const auto& combine : combines) {
                const TExprBase pushed = KqpPushOlapDistinct(TExprBase(combine), ctx, *KqpCtx, &parentsMap);
                if (pushed.Ptr() != combine) {
                    output = ctx.ReplaceNode(std::move(output), *combine.Get(), pushed.Ptr());
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                break;
            }
        }
        return TStatus::Ok;
    }

    void Rewind() final {
    }

private:
    TIntrusivePtr<TKqpOptimizeContext> KqpCtx;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpPushOlapDistinctOnPhysicalQueryTransformer(
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    return new TKqpPushOlapDistinctPhysicalQueryTransformer(kqpCtx);
}

} // namespace NKikimr::NKqp::NOpt
