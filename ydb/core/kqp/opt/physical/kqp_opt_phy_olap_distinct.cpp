#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {
bool IsAnyArgReturnLambda(const TCoLambda& lambda) {
    for (ui32 i = 0; i < lambda.Args().Size(); ++i) {
        if (lambda.Body().Raw() == lambda.Args().Arg(i).Raw()) {
            return true;
        }
    }
    return false;
}

bool IsTrivialDistinctCombine(const TDqPhyHashCombine& combine) {
    // Accept common patterns where handlers are simple passthrough lambdas.
    // This lets us avoid matching non-trivial aggregation combines (SUM/COUNT/etc.).
    return IsAnyArgReturnLambda(combine.InitHandler())
        && IsAnyArgReturnLambda(combine.UpdateHandler())
        && IsAnyArgReturnLambda(combine.FinishHandler());
}

bool IsTrivialDistinctCombine(const TCoCombineCore& combine) {
    return IsAnyArgReturnLambda(combine.InitHandler())
        && IsAnyArgReturnLambda(combine.UpdateHandler())
        && IsAnyArgReturnLambda(combine.FinishHandler());
}

bool IsTrivialDistinctCombineNode(const TExprBase& combineNode) {
    if (combineNode.Maybe<TDqPhyHashCombine>()) {
        return IsTrivialDistinctCombine(combineNode.Cast<TDqPhyHashCombine>());
    }
    if (combineNode.Maybe<TCoCombineCore>()) {
        return IsTrivialDistinctCombine(combineNode.Cast<TCoCombineCore>());
    }
    return false;
}

bool ProcessBodyHasOlapDistinctOrAgg(const TCoLambda& process) {
    const auto pred = [](const TExprNode::TPtr& n) {
        return TKqpOlapDistinct::Match(n.Get()) || TKqpOlapAgg::Match(n.Get());
    };
    return !!NYql::FindNode(process.Body().Ptr(), pred);
}

std::optional<TString> DistinctColFromKeyExtractor(const TCoLambda& keyExtractor) {
    if (keyExtractor.Args().Size() != 1 || !keyExtractor.Body().Maybe<TCoMember>()) {
        // Support identity key extractor for 1-column DISTINCT:
        // if keyExtractor is `($row) -> $row` and the row type is a struct with a single field,
        // treat that field as the DISTINCT key.
        if (keyExtractor.Args().Size() == 1 && keyExtractor.Body().Raw() == keyExtractor.Args().Arg(0).Raw()) {
            const TTypeAnnotationNode* argType = keyExtractor.Args().Arg(0).Ref().GetTypeAnn();
            if (!argType) {
                return std::nullopt;
            }
            if (argType->GetKind() == ETypeAnnotationKind::Optional) {
                argType = argType->Cast<TOptionalExprType>()->GetItemType();
            }
            if (argType->GetKind() != ETypeAnnotationKind::Struct) {
                return std::nullopt;
            }
            const auto items = argType->Cast<TStructExprType>()->GetItems();
            if (items.size() != 1) {
                return std::nullopt;
            }
            return TString(items.front()->GetName());
        }
        return std::nullopt;
    }
    const auto member = keyExtractor.Body().Cast<TCoMember>();
    if (member.Struct().Raw() != keyExtractor.Args().Arg(0).Raw()) {
        return std::nullopt;
    }
    return TString(member.Name().StringValue());
}

/// DISTINCT grouping key as understood by the combine's KeyExtractor.
/// Handler lambdas may be non-trivial even for DISTINCT; we still need the key column to validate pragma.
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

/// OLAP forced-distinct pragma only considers combines that TryReplaceBlockOlapReadInputWithDistinct would inspect:
/// a single BlockRead reachable via allowed wrappers under HashCombine/CombineCore input.
bool OlapDistinctForcePragmaAppliesToCombine(const TExprBase& combineInput, const TExprBase& combineNode) {
    if (!(combineNode.Maybe<TDqPhyHashCombine>() || combineNode.Maybe<TCoCombineCore>())) {
        return false;
    }
    const auto blockReads = FindNodes(combineInput.Ptr(), [](const TExprNode::TPtr& n) {
        return TKqpBlockReadOlapTableRanges::Match(n.Get());
    });
    if (blockReads.size() != 1) {
        return false;
    }
    const auto read = TExprBase(blockReads[0]).Cast<TKqpBlockReadOlapTableRanges>();
    return PathToBlockOlapReadViaAllowedWrappers(combineInput, read.Raw());
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

bool OlapProjectionColumnPredicate(const TExprNode::TPtr& n, TStringBuf colName) {
    if (TKqpOlapProjection::Match(n.Get())) {
        return TExprBase(n).Cast<TKqpOlapProjection>().ColumnName().StringValue() == colName;
    }
    if (!TKqpOlapProjections::Match(n.Get())) {
        return false;
    }
    const auto projections = TExprBase(n).Cast<TKqpOlapProjections>().Projections();
    for (const auto& child : projections) {
        const auto projection = child.Cast<TKqpOlapProjection>();
        if (projection.ColumnName().StringValue() == colName) {
            return true;
        }
    }
    return false;
}

bool ProcessHasProjectionOutputColumn(const TCoLambda& process, TStringBuf colName) {
    const auto pred = [&colName](const TExprNode::TPtr& n) { return OlapProjectionColumnPredicate(n, colName); };
    // Search the whole process lambda: JSON_VALUE projections may sit under nested nodes (not only Body root).
    return !!FindNode(process.Ptr(), pred);
}

bool ExprTreeHasOlapProjectionColumn(const TExprBase& root, TStringBuf colName) {
    const auto pred = [&colName](const TExprNode::TPtr& n) { return OlapProjectionColumnPredicate(n, colName); };
    return !!FindNode(root.Ptr(), pred);
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
    const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
    if (!forceDistinct || forceDistinct->empty()) {
        return std::nullopt;
    }
    const TString& keyColumn = *forceDistinct;

    auto combineKeyColumn = TryGetDistinctColumnFromCombineKey(combineNode);
    if (!combineKeyColumn && IsTrivialDistinctCombineNode(combineNode)) {
        // In some plans keyExtractor is identity over a single-column row.
        // When handlers are trivial passthrough and read columns list is exactly one column,
        // treat that column as DISTINCT key.
        if (read.Columns().Size() == 1 && read.Columns().Item(0).Maybe<TCoAtom>()) {
            combineKeyColumn = TString(read.Columns().Item(0).Cast<TCoAtom>().StringValue());
        }
    }
    if (!combineKeyColumn) {
        // Cannot relate this combine to a single DISTINCT column (or not an OLAP DISTINCT shape).
        return std::nullopt;
    }

    const bool aggPushdownEnabled =
        kqpCtx.Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false);

    if (*combineKeyColumn != keyColumn) {
        if (!aggPushdownEnabled) {
            ctx.AddError(TIssue(
                ctx.GetPosition(pos),
                TStringBuilder()
                    << "OptForceOlapPushdownDistinct = '" << keyColumn
                    << "' does not match DISTINCT key column '" << *combineKeyColumn << "'"
            ));
        }
        // Either an error (standalone DISTINCT + wrong pragma), or an unrelated forced column while building
        // SUM(DISTINCT …) / OLAP aggregate pipelines — never fuse the forced column name into the read.
        return std::nullopt;
    }

    const bool onReadColumns = ReadColumnsListContains(read, keyColumn);
    const bool onProjections = ProcessHasProjectionOutputColumn(read.Process(), keyColumn)
        || ExprTreeHasOlapProjectionColumn(combineInput, keyColumn);
    // DISTINCT key is validated against the combine key above. TKqpOlapProjection nodes may appear only
    // after later lowering passes, so we do not require projection IR here when the name is not a table column.
    if (onReadColumns && onProjections) {
        ctx.AddError(TIssue(
            ctx.GetPosition(pos),
            TStringBuilder()
                << "OptForceOlapPushdownDistinct = '" << keyColumn
                << "' is ambiguous: the same name is both a stored table column and an OLAP projection output "
                << "(e.g. JSON_VALUE AS alias). Rename the DISTINCT output (AS ...) or the table column."
        ));
        return std::nullopt;
    }
    if (onReadColumns && !onProjections) {
        const auto hasJsonInTree = [](const TExprNode::TPtr& root) {
            return !!FindNode(root, [](const TExprNode::TPtr& n) { return TKqpOlapJsonValue::Match(n.Get()); });
        };
        const bool hasOlapJsonValueInProcess = hasJsonInTree(read.Process().Ptr()) || hasJsonInTree(combineInput.Ptr());
        if (hasOlapJsonValueInProcess) {
            ctx.AddError(TIssue(
                ctx.GetPosition(pos),
                TStringBuilder()
                    << "OptForceOlapPushdownDistinct = '" << keyColumn
                    << "' is ambiguous: a stored table column and OLAP JSON_VALUE share the same DISTINCT name. "
                    << "Rename the DISTINCT output (AS ...) or the table column."
            ));
            return std::nullopt;
        }
    }

    // OLAP DISTINCT fusion into BlockRead.Process is only supported for trivial DISTINCT combines.
    if (!IsTrivialDistinctCombineNode(combineNode)) {
        return std::nullopt;
    }

    if (read.Process().Args().Size() != 1) {
        // Cannot fuse OLAP DISTINCT into read.Process(); mismatch vs pragma was validated above.
        return std::nullopt;
    }

    // Make rewrite idempotent: do not inject OLAP operations twice.
    if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
        return std::nullopt;
    }

    auto olapDistinct = Build<TKqpOlapDistinct>(ctx, pos)
        .Input(read.Process().Body())
        .Key().Build(keyColumn)
        .Done();

    auto olapDistinctLambda = Build<TCoLambda>(ctx, pos)
        .Args({"olap_dist_row"})
        .Body<TExprApplier>()
            .Apply(olapDistinct)
            .With(read.Process().Args().Arg(0), "olap_dist_row")
            .Build()
        .Done();

    const auto newProcessLambda = olapDistinctLambda;

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

        const bool aggPushdownEnabled =
            KqpCtx->Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false);

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

            if (!changed && !aggPushdownEnabled) {
                // Fallback / extra validation interact badly with unrelated OptForceOlapPushdownDistinct values
                // under aggregate pushdown (SUM(DISTINCT …)); those paths are skipped entirely in that mode.
                for (const auto& combine : combines) {
                    const TExprBase combineInput = TExprBase(combine).Maybe<TDqPhyHashCombine>()
                        ? TExprBase(combine).Cast<TDqPhyHashCombine>().Input()
                        : TExprBase(combine).Cast<TCoCombineCore>().Input();
                    if (!OlapDistinctForcePragmaAppliesToCombine(combineInput, TExprBase(combine))) {
                        continue;
                    }
                    if (const auto col = TryGetDistinctColumnFromCombineKey(TExprBase(combine))) {
                        if (*col != *forceDistinct) {
                            ctx.AddError(TIssue(
                                ctx.GetPosition(combine->Pos()),
                                TStringBuilder()
                                    << "OptForceOlapPushdownDistinct = '" << *forceDistinct
                                    << "' does not match DISTINCT key column '" << *col << "'"
                            ));
                            return TStatus::Error;
                        }
                    }
                }

                // Fallback: if we couldn't match a combine shape, still allow injecting DISTINCT marker
                // directly into the OLAP block read when the user explicitly forced it.
                const TString& keyColumn = *forceDistinct;
                const auto reads = FindNodes(output, [](const TExprNode::TPtr& n) {
                    return TKqpBlockReadOlapTableRanges::Match(n.Get());
                });
                bool hasTrivialDistinctCombine = false;
                const auto combinesForFallback = FindNodes(output, [](const TExprNode::TPtr& n) {
                    return TDqPhyHashCombine::Match(n.Get()) || TCoCombineCore::Match(n.Get());
                });
                for (const auto& combineForFb : combinesForFallback) {
                    if (IsTrivialDistinctCombineNode(TExprBase(combineForFb))) {
                        hasTrivialDistinctCombine = true;
                        break;
                    }
                }
                std::optional<TString> fallbackDistinctColumn;
                for (const auto& combineForFb : combinesForFallback) {
                    const TExprBase combineBase(combineForFb);
                    const TExprBase combineInput = combineBase.Maybe<TDqPhyHashCombine>()
                        ? combineBase.Cast<TDqPhyHashCombine>().Input()
                        : combineBase.Cast<TCoCombineCore>().Input();
                    if (!OlapDistinctForcePragmaAppliesToCombine(combineInput, combineBase)) {
                        continue;
                    }
                    fallbackDistinctColumn = TryGetDistinctColumnFromCombineKey(combineBase);
                    if (fallbackDistinctColumn) {
                        break;
                    }
                }
                std::optional<TString> mismatchSingleColumn;
                TExprNode::TPtr combineSubgraphForKey;
                for (const auto& combineForFb : combinesForFallback) {
                    const TExprBase combineBase(combineForFb);
                    const TExprBase combineInputFb = combineBase.Maybe<TDqPhyHashCombine>()
                        ? combineBase.Cast<TDqPhyHashCombine>().Input()
                        : combineBase.Cast<TCoCombineCore>().Input();
                    if (!OlapDistinctForcePragmaAppliesToCombine(combineInputFb, combineBase)) {
                        continue;
                    }
                    const auto fd = TryGetDistinctColumnFromCombineKey(combineBase);
                    if (fd && *fd == keyColumn) {
                        combineSubgraphForKey = combineInputFb.Ptr();
                        break;
                    }
                }
                for (const auto& readNode : reads) {
                    const auto read = TExprBase(readNode).Cast<TKqpBlockReadOlapTableRanges>();
                    if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
                        continue;
                    }
                    // Same constraint as TryReplaceBlockOlapReadInputWithDistinct: fusion wraps Process in a single-arg lambda.
                    if (read.Process().Args().Size() != 1) {
                        continue;
                    }
                    const bool fbOnRead = ReadColumnsListContains(read, keyColumn);
                    const bool fbOnProj = ProcessHasProjectionOutputColumn(read.Process(), keyColumn)
                        || ExprTreeHasOlapProjectionColumn(TExprBase(readNode), keyColumn);
                    // With multiple OLAP reads, only fuse into reads that clearly carry the key or projection;
                    // otherwise we might attach DISTINCT to the wrong scan.
                    if (!(fbOnRead || fbOnProj) && reads.size() != 1) {
                        if (!mismatchSingleColumn && read.Columns().Size() == 1 && read.Columns().Item(0).Maybe<TCoAtom>()) {
                            mismatchSingleColumn = TString(read.Columns().Item(0).Cast<TCoAtom>().StringValue());
                        }
                        continue;
                    }
                    if (fbOnRead && fbOnProj) {
                        ctx.AddError(TIssue(
                            ctx.GetPosition(input->Pos()),
                            TStringBuilder()
                                << "OptForceOlapPushdownDistinct = '" << keyColumn
                                << "' is ambiguous: the same name is both a stored table column and an OLAP projection output "
                                << "(e.g. JSON_VALUE AS alias). Rename the DISTINCT output (AS ...) or the table column."
                        ));
                        return TStatus::Error;
                    }
                    if (fbOnRead && !fbOnProj) {
                        const auto hasJsonInTree = [](const TExprNode::TPtr& root) {
                            return !!FindNode(root, [](const TExprNode::TPtr& n) { return TKqpOlapJsonValue::Match(n.Get()); });
                        };
                        const bool hasOlapJsonValueConflict = hasJsonInTree(read.Process().Ptr())
                            || (combineSubgraphForKey && hasJsonInTree(combineSubgraphForKey));
                        if (hasOlapJsonValueConflict) {
                            ctx.AddError(TIssue(
                                ctx.GetPosition(input->Pos()),
                                TStringBuilder()
                                    << "OptForceOlapPushdownDistinct = '" << keyColumn
                                    << "' is ambiguous: a stored table column and OLAP JSON_VALUE share the same DISTINCT name. "
                                    << "Rename the DISTINCT output (AS ...) or the table column."
                            ));
                            return TStatus::Error;
                        }
                    }

                    // Fallback must not fire for multi-column DISTINCT: fuse shape assumes a single named key column.
                    if (!fallbackDistinctColumn || *fallbackDistinctColumn != keyColumn) {
                        continue;
                    }

                    auto olapDistinct = Build<TKqpOlapDistinct>(ctx, read.Pos())
                        .Input(read.Process().Body())
                        .Key().Build(keyColumn)
                        .Done();

                    auto newProcessLambda = Build<TCoLambda>(ctx, read.Pos())
                        .Args({"olap_dist_row"})
                        .Body<TExprApplier>()
                            .Apply(olapDistinct)
                            .With(read.Process().Args().Arg(0), "olap_dist_row")
                            .Build()
                        .Done();

                    auto settings = TKqpReadTableSettings::Parse(read);
                    const auto forceLimit = KqpCtx->Config->OptForceOlapPushdownDistinctLimit.Get();
                    if (forceLimit && forceLimit.GetRef() > 0 && !settings.ItemsLimit) {
                        const auto limitNode = Build<TCoUint64>(ctx, read.Pos())
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

                    output = ctx.ReplaceNode(std::move(output), *readNode.Get(), newRead.Ptr());
                    changed = true;
                    break;
                }

                if (!changed && mismatchSingleColumn && hasTrivialDistinctCombine) {
                    ctx.AddError(TIssue(
                        ctx.GetPosition(input->Pos()),
                        TStringBuilder()
                            << "OptForceOlapPushdownDistinct = '" << keyColumn
                            << "' does not match DISTINCT key column '" << *mismatchSingleColumn << "'"
                    ));
                    return TStatus::Error;
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
