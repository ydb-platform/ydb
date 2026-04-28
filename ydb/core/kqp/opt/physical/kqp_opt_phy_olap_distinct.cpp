#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <ydb/core/kqp/opt/physical/kqp_opt_phy.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {
bool ProcessBodyHasOlapDistinctOrAgg(const TCoLambda& process) {
    const auto pred = [](const TExprNode::TPtr& n) {
        return TKqpOlapDistinct::Match(n.Get()) || TKqpOlapAgg::Match(n.Get());
    };
    return !!NYql::FindNode(process.Body().Ptr(), pred);
}

std::optional<TString> DistinctColFromKeyExtractor(const TCoLambda& keyExtractor) {
    if (keyExtractor.Args().Size() != 1 || !keyExtractor.Body().Maybe<TCoMember>()) {
        return std::nullopt;
    }
    const auto member = keyExtractor.Body().Cast<TCoMember>();
    if (member.Struct().Raw() != keyExtractor.Args().Arg(0).Raw()) {
        return std::nullopt;
    }
    return TString(member.Name().StringValue());
}

std::optional<TString> TryGetDistinctColumnFromCombineKey(const TExprBase& combineNode) {
    if (combineNode.Maybe<TDqPhyHashCombine>()) {
        return DistinctColFromKeyExtractor(combineNode.Cast<TDqPhyHashCombine>().KeyExtractor());
    }
    if (combineNode.Maybe<TCoCombineCore>()) {
        return DistinctColFromKeyExtractor(combineNode.Cast<TCoCombineCore>().KeyExtractor());
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

std::optional<TExprBase> TryReplaceBlockOlapReadInputWithDistinct(
    TExprBase combineInput,
    TExprContext& ctx,
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
    if (read.Process().Args().Size() != 1) {
        return std::nullopt;
    }

    const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
    if (!forceDistinct || forceDistinct->empty()) {
        return std::nullopt;
    }
    const TString& keyColumn = *forceDistinct;

    const auto combineKeyColumn = TryGetDistinctColumnFromCombineKey(combineNode);
    if (!combineKeyColumn) {
        return std::nullopt;
    }
    if (*combineKeyColumn != keyColumn) {
        ctx.AddError(TIssue(
            ctx.GetPosition(pos),
            TStringBuilder()
                << "OptForceOlapPushdownDistinct = '" << keyColumn
                << "' does not match DISTINCT key column '" << *combineKeyColumn << "'"
        ));
        return std::nullopt;
    }
    if (!ReadColumnsListContains(read, keyColumn)) {
        ctx.AddError(TIssue(
            ctx.GetPosition(pos),
            TStringBuilder()
                << "OptForceOlapPushdownDistinct = '" << keyColumn
                << "' refers to a column that is not available in OLAP read columns"
        ));
        return std::nullopt;
    }

    // Make rewrite idempotent: do not inject OLAP operations twice.
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
    const auto forceLimit = kqpCtx.Config->OptForceOlapPushdownDistinctLimit.Get();
    if (forceLimit && forceLimit.GetRef() > 0 && !settings.ItemsLimit) {
        const auto limitNode = Build<TCoUint64>(ctx, pos)
            .Literal<TCoAtom>()
            .Value(ToString(forceLimit.GetRef()))
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

TExprBase KqpPushOlapDistinct(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (node.Maybe<TDqPhyHashCombine>()) {
        const auto combine = node.Cast<TDqPhyHashCombine>();
        if (const auto newInput = TryReplaceBlockOlapReadInputWithDistinct(
                combine.Input(), ctx, node.Pos(), kqpCtx, node))
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
                combine.Input(), ctx, node.Pos(), kqpCtx, node))
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
        if (!KqpCtx->Config->HasOptEnableOlapPushdown()) {
            return TStatus::Ok;
        }
        const auto forceDistinct = KqpCtx->Config->OptForceOlapPushdownDistinct.Get();
        if (!forceDistinct || forceDistinct->empty()) {
            return TStatus::Ok;
        }

        for (;;) {
            const auto combines = FindNodes(output, [](const TExprNode::TPtr& n) {
                return TDqPhyHashCombine::Match(n.Get()) || TCoCombineCore::Match(n.Get());
            });
            bool changed = false;
            for (const auto& combine : combines) {
                const TExprBase pushed = KqpPushOlapDistinct(TExprBase(combine), ctx, *KqpCtx);
                if (!ctx.IssueManager.GetIssues().Empty()) {
                    return TStatus::Error;
                }
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
