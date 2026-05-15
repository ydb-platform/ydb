#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/cast.h>

#include <limits>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// --- OLAP read / combine discovery (centralized FindNodes predicates, see PR review §40) ---
// Physical opt applies handlers per node; helpers below only factor repeated subtree scans.

struct TOlapReadsUnderCombineInput {
    TExprNode::TListType BlockReads;
    TExprNode::TListType WideReads;
};

TExprNode::TListType FindBlockOlapReadsInExpr(const TExprNode::TPtr& root) {
    return FindNodes(root, [](const TExprNode::TPtr& n) {
        return TKqpBlockReadOlapTableRanges::Match(n.Get());
    });
}

TExprNode::TListType FindWideOlapReadsInExpr(const TExprNode::TPtr& root) {
    return FindNodes(root, [](const TExprNode::TPtr& n) {
        return TKqpReadOlapTableRanges::Match(n.Get());
    });
}

TExprNode::TListType FindPhyHashCombineOrCombineCoreInExpr(const TExprNode::TPtr& root) {
    return FindNodes(root, [](const TExprNode::TPtr& n) {
        return TDqPhyHashCombine::Match(n.Get()) || TCoCombineCore::Match(n.Get());
    });
}

void CollectOlapReadsUnderCombineInput(const TExprBase& combineInput, TOlapReadsUnderCombineInput& out) {
    const TExprNode::TPtr ptr = combineInput.Ptr();
    out.BlockReads = FindBlockOlapReadsInExpr(ptr);
    out.WideReads = FindWideOlapReadsInExpr(ptr);
}

bool ExprTreeContainsOlapJsonValue(const TExprNode::TPtr& root) {
    if (!root) {
        return false;
    }
    return !!FindNode(root, [](const TExprNode::TPtr& n) { return TKqpOlapJsonValue::Match(n.Get()); });
}

std::optional<ui64> TryParseLiteralItemsLimit(const TExprNode::TPtr& node) {
    if (!node) {
        return std::nullopt;
    }
    if (const auto maybe = TMaybeNode<TCoUint64>(node)) {
        return FromString<ui64>(maybe.Cast().Literal().Value());
    }
    return std::nullopt;
}

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

TExprBase GetPhyHashCombineOrCombineCoreInput(const TExprBase& combineNode) {
    AFL_VERIFY(combineNode.Maybe<TDqPhyHashCombine>() || combineNode.Maybe<TCoCombineCore>());
    if (combineNode.Maybe<TDqPhyHashCombine>()) {
        return combineNode.Cast<TDqPhyHashCombine>().Input();
    }
    return combineNode.Cast<TCoCombineCore>().Input();
}

bool PathToBlockOlapReadViaAllowedWrappers(const TExprBase& cur, const TExprNode* targetRead) {
    // Wrapper list must stay in sync with RebuildCombineInputReplacingBlockRead (same OLAP combine input shapes).
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

/// OLAP forced-distinct pragma only considers combines over a single OLAP read (block or wide) reachable via
/// allowed wrappers under HashCombine/CombineCore input — the same shapes `TryReplaceBlockOlapReadInputWithDistinct` inspects.
bool OlapDistinctForcePragmaAppliesToCombine(const TExprBase& combineInput, const TExprBase& combineNode) {
    if (!(combineNode.Maybe<TDqPhyHashCombine>() || combineNode.Maybe<TCoCombineCore>())) {
        return false;
    }
    TOlapReadsUnderCombineInput reads;
    CollectOlapReadsUnderCombineInput(combineInput, reads);
    if (reads.BlockReads.size() == 1) {
        const auto read = TExprBase(reads.BlockReads[0]).Cast<TKqpBlockReadOlapTableRanges>();
        return PathToBlockOlapReadViaAllowedWrappers(combineInput, read.Raw());
    }
    if (reads.WideReads.size() == 1) {
        return true;
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
    // Wrapper list must stay in sync with PathToBlockOlapReadViaAllowedWrappers.
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
    TOlapReadsUnderCombineInput olapReads;
    CollectOlapReadsUnderCombineInput(combineInput, olapReads);
    if (olapReads.BlockReads.size() != 1) {
        if (olapReads.WideReads.size() != 1) {
            return std::nullopt;
        }
        const auto wideRead = TExprBase(olapReads.WideReads[0]).Cast<TKqpReadOlapTableRanges>();
        const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
        if (!forceDistinct || forceDistinct->empty()) {
            return std::nullopt;
        }
        const TString& keyColumn = *forceDistinct;
        auto combineKeyColumn = TryGetDistinctColumnFromCombineKey(combineNode);
        if (!combineKeyColumn && IsTrivialDistinctCombineNode(combineNode)) {
            if (wideRead.Columns().Size() == 1 && wideRead.Columns().Item(0).Maybe<TCoAtom>()) {
                combineKeyColumn = TString(wideRead.Columns().Item(0).Cast<TCoAtom>().StringValue());
            }
        }
        if (!combineKeyColumn) {
            return std::nullopt;
        }
        const bool aggPushdownEnabled =
            kqpCtx.Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false);
        if (*combineKeyColumn != keyColumn && !aggPushdownEnabled) {
            ctx.AddError(TIssue(
                ctx.GetPosition(pos),
                TStringBuilder()
                    << "OptForceOlapPushdownDistinct = '" << keyColumn
                    << "' does not match DISTINCT key column '" << *combineKeyColumn << "'"
            ));
        }
        return std::nullopt;
    }
    const auto read = TExprBase(olapReads.BlockReads[0]).Cast<TKqpBlockReadOlapTableRanges>();
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
        const bool hasOlapJsonValueInProcess = ExprTreeContainsOlapJsonValue(read.Process().Ptr())
            || ExprTreeContainsOlapJsonValue(combineInput.Ptr());
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
    if (forceLimit && forceLimit.GetRef() > 0) {
        if (settings.ItemsLimit) {
            if (const auto existingLimit = TryParseLiteralItemsLimit(settings.ItemsLimit)) {
                if (*existingLimit != forceLimit.GetRef()) {
                    ctx.AddError(TIssue(
                        ctx.GetPosition(pos),
                        TStringBuilder()
                            << "OptForceOlapPushdownDistinctLimit (" << forceLimit.GetRef()
                            << ") conflicts with the existing scan ItemsLimit (" << *existingLimit
                            << "). Use the same LIMIT in SQL as in the pragma, or remove one of them."));
                    return std::nullopt;
                }
            }
        } else {
            const auto limitNode = Build<TCoUint64>(ctx, pos)
                .Literal<TCoAtom>()
                .Value(ToString(forceLimit.GetRef()))
                .Build()
                .Done();
            settings.SetItemsLimit(limitNode.Ptr());
        }
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

ui32 KqpCountFatalCompletedIssues(const TExprContext& ctx) {
    ui32 cnt = 0;
    const TIssues& issues = ctx.IssueManager.GetCompletedIssues();
    for (const TIssue& top : issues) {
        WalkThroughIssues(top, true, [&](const TIssue& issue, ui16 /*level*/) {
            if (issue.GetSeverity() == TSeverityIds::S_FATAL || issue.GetSeverity() == TSeverityIds::S_ERROR) {
                ++cnt;
            }
        });
    }
    return cnt;
}

namespace {

struct TDistinctFallbackScan {
    TString ForceDistinctKey;
    bool AggPushdownEnabled = false;
    bool HasTrivialDistinctCombine = false;
    std::optional<TString> MismatchSingleColumn;
    std::optional<TString> FallbackDistinctColumn;
    NYql::TExprNode::TPtr CombineSubgraphForKey;
    ui32 BlockOlapReadsCount = 0;
};

void PopulateDistinctFallbackScan(const TExprNode::TPtr& root, const TKqpOptimizeContext& kqpCtx,
    TDistinctFallbackScan& out)
{
    out = {};
    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return;
    }
    const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
    if (!forceDistinct || forceDistinct->empty()) {
        return;
    }
    out.ForceDistinctKey = *forceDistinct;
    out.AggPushdownEnabled = kqpCtx.Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false);
    const TString& keyColumn = out.ForceDistinctKey;

    const auto reads = FindBlockOlapReadsInExpr(root);
    out.BlockOlapReadsCount = reads.size();

    const auto combinesForFallback = FindPhyHashCombineOrCombineCoreInExpr(root);
    for (const auto& combineForFb : combinesForFallback) {
        if (IsTrivialDistinctCombineNode(TExprBase(combineForFb))) {
            out.HasTrivialDistinctCombine = true;
            break;
        }
    }
    for (const auto& combineForFb : combinesForFallback) {
        const TExprBase combineBase(combineForFb);
        const TExprBase combineInput = GetPhyHashCombineOrCombineCoreInput(combineBase);
        if (!OlapDistinctForcePragmaAppliesToCombine(combineInput, combineBase)) {
            continue;
        }
        const auto fd = TryGetDistinctColumnFromCombineKey(combineBase);
        if (fd) {
            if (!out.FallbackDistinctColumn) {
                out.FallbackDistinctColumn = fd;
            }
            if (*fd == keyColumn && !out.CombineSubgraphForKey) {
                out.CombineSubgraphForKey = combineInput.Ptr();
            }
        }
    }

    for (const auto& readNode : reads) {
        const auto read = TExprBase(readNode).Cast<TKqpBlockReadOlapTableRanges>();
        const bool fbOnRead = ReadColumnsListContains(read, keyColumn);
        const bool fbOnProj = ProcessHasProjectionOutputColumn(read.Process(), keyColumn)
            || ExprTreeHasOlapProjectionColumn(TExprBase(readNode), keyColumn);
        if (!(fbOnRead || fbOnProj) && reads.size() != 1) {
            if (!out.MismatchSingleColumn && read.Columns().Size() == 1 && read.Columns().Item(0).Maybe<TCoAtom>()) {
                out.MismatchSingleColumn = TString(read.Columns().Item(0).Cast<TCoAtom>().StringValue());
            }
        }
    }
}

bool ValidateOlapForceDistinctCombinesPragmaOnRootImpl(const TExprNode::TPtr& root, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
    if (!forceDistinct || forceDistinct->empty()) {
        return true;
    }
    const auto combines = FindPhyHashCombineOrCombineCoreInExpr(root);
    const bool aggPushdownEnabled = kqpCtx.Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false);
    for (const auto& combine : combines) {
        const TExprBase combineInput = GetPhyHashCombineOrCombineCoreInput(TExprBase(combine));
        if (!OlapDistinctForcePragmaAppliesToCombine(combineInput, TExprBase(combine))) {
            continue;
        }
        if (const auto col = TryGetDistinctColumnFromCombineKey(TExprBase(combine))) {
            if (*col != *forceDistinct && !aggPushdownEnabled) {
                ctx.AddError(TIssue(
                    ctx.GetPosition(combine->Pos()),
                    TStringBuilder()
                        << "OptForceOlapPushdownDistinct = '" << *forceDistinct
                        << "' does not match DISTINCT key column '" << *col << "'"
                ));
                return false;
            }
        }
    }
    return true;
}

bool AnyBlockReadHasOlapDistinctInProcess(const TExprNode::TPtr& root) {
    const auto reads = FindBlockOlapReadsInExpr(root);
    for (const auto& readNode : reads) {
        const auto read = TExprBase(readNode).Cast<TKqpBlockReadOlapTableRanges>();
        if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
            return true;
        }
    }
    return false;
}

TExprBase ApplyOlapDistinctReadFallback(const TExprBase& readNode, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TDistinctFallbackScan& scan)
{
    if (!readNode.Maybe<TKqpBlockReadOlapTableRanges>()) {
        return readNode;
    }
    if (scan.AggPushdownEnabled || scan.ForceDistinctKey.empty()) {
        return readNode;
    }
    const TString& keyColumn = scan.ForceDistinctKey;
    const auto read = readNode.Cast<TKqpBlockReadOlapTableRanges>();

    if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
        return readNode;
    }
    if (read.Process().Args().Size() != 1) {
        return readNode;
    }

    const bool fbOnRead = ReadColumnsListContains(read, keyColumn);
    const bool fbOnProj = ProcessHasProjectionOutputColumn(read.Process(), keyColumn)
        || ExprTreeHasOlapProjectionColumn(readNode, keyColumn);
    if (!(fbOnRead || fbOnProj) && scan.BlockOlapReadsCount != 1) {
        return readNode;
    }
    if (fbOnRead && fbOnProj) {
        ctx.AddError(TIssue(
            ctx.GetPosition(readNode.Pos()),
            TStringBuilder()
                << "OptForceOlapPushdownDistinct = '" << keyColumn
                << "' is ambiguous: the same name is both a stored table column and an OLAP projection output "
                << "(e.g. JSON_VALUE AS alias). Rename the DISTINCT output (AS ...) or the table column."
        ));
        return readNode;
    }
    if (fbOnRead && !fbOnProj) {
        const bool hasOlapJsonValueConflict = ExprTreeContainsOlapJsonValue(read.Process().Ptr())
            || (scan.CombineSubgraphForKey && ExprTreeContainsOlapJsonValue(scan.CombineSubgraphForKey));
        if (hasOlapJsonValueConflict) {
            ctx.AddError(TIssue(
                ctx.GetPosition(readNode.Pos()),
                TStringBuilder()
                    << "OptForceOlapPushdownDistinct = '" << keyColumn
                    << "' is ambiguous: a stored table column and OLAP JSON_VALUE share the same DISTINCT name. "
                    << "Rename the DISTINCT output (AS ...) or the table column."
            ));
            return readNode;
        }
    }

    if (!scan.FallbackDistinctColumn || *scan.FallbackDistinctColumn != keyColumn) {
        return readNode;
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
    const auto forceLimit = kqpCtx.Config->OptForceOlapPushdownDistinctLimit.Get();
    if (forceLimit && forceLimit.GetRef() > 0) {
        if (settings.ItemsLimit) {
            if (const auto existingLimit = TryParseLiteralItemsLimit(settings.ItemsLimit)) {
                if (*existingLimit != forceLimit.GetRef()) {
                    ctx.AddError(TIssue(
                        ctx.GetPosition(readNode.Pos()),
                        TStringBuilder()
                            << "OptForceOlapPushdownDistinctLimit (" << forceLimit.GetRef()
                            << ") conflicts with the existing scan ItemsLimit (" << *existingLimit
                            << "). Use the same LIMIT in SQL as in the pragma, or remove one of them."));
                    return readNode;
                }
            }
        } else {
            const auto limitNode = Build<TCoUint64>(ctx, read.Pos())
                .Literal<TCoAtom>()
                .Value(ToString(forceLimit.GetRef()))
                .Build()
                .Done();
            settings.SetItemsLimit(limitNode.Ptr());
        }
    }
    const auto newSettings = settings.BuildNode(ctx, read.Pos());

    return Build<TKqpBlockReadOlapTableRanges>(ctx, read.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(newSettings)
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();
}

void EmitOlapDistinctFallbackMismatchIfNeeded(const TExprNode::TPtr& root, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const TDistinctFallbackScan& scan, const TExprBase& readAfterApply)
{
    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return;
    }
    const auto forceDistinct = kqpCtx.Config->OptForceOlapPushdownDistinct.Get();
    if (!forceDistinct || forceDistinct->empty()) {
        return;
    }
    if (kqpCtx.Config->OptEnableOlapPushdownAggregate.Get().GetOrElse(false)) {
        return;
    }
    if (AnyBlockReadHasOlapDistinctInProcess(root)) {
        return;
    }
    if (readAfterApply.Maybe<TKqpBlockReadOlapTableRanges>()) {
        const auto read = readAfterApply.Cast<TKqpBlockReadOlapTableRanges>();
        if (ProcessBodyHasOlapDistinctOrAgg(read.Process())) {
            return;
        }
    }
    if (!scan.MismatchSingleColumn || !scan.HasTrivialDistinctCombine) {
        return;
    }
    const TString& keyColumn = *forceDistinct;
    ctx.AddError(TIssue(
        ctx.GetPosition(root->Pos()),
        TStringBuilder()
            << "OptForceOlapPushdownDistinct = '" << keyColumn
            << "' does not match DISTINCT key column '" << *scan.MismatchSingleColumn << "'"
    ));
}

} // namespace

bool KqpValidateOlapForceDistinctCombinesPragmaOnRoot(const TExprNode::TPtr& root, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    return ValidateOlapForceDistinctCombinesPragmaOnRootImpl(root, ctx, kqpCtx);
}

TExprBase KqpPushOlapDistinctOnBlockReadForGraph(TExprBase readNode, const TExprNode* graphRoot, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    if (!readNode.Maybe<TKqpBlockReadOlapTableRanges>()) {
        return readNode;
    }
    if (!graphRoot) {
        return readNode;
    }
    const TExprNode::TPtr rootPtr(const_cast<TExprNode*>(graphRoot));
    if (!KqpValidateOlapForceDistinctCombinesPragmaOnRoot(rootPtr, ctx, kqpCtx)) {
        return readNode;
    }
    TDistinctFallbackScan scan;
    PopulateDistinctFallbackScan(rootPtr, kqpCtx, scan);
    const TExprBase out = ApplyOlapDistinctReadFallback(readNode, ctx, kqpCtx, scan);

    const auto reads = FindBlockOlapReadsInExpr(rootPtr);
    const TExprNode* canonicalRead = nullptr;
    ui64 minUid = std::numeric_limits<ui64>::max();
    for (const auto& r : reads) {
        const ui64 uid = r->UniqueId();
        if (uid < minUid) {
            minUid = uid;
            canonicalRead = r.Get();
        }
    }
    if (canonicalRead && readNode.Raw() == canonicalRead) {
        EmitOlapDistinctFallbackMismatchIfNeeded(rootPtr, ctx, kqpCtx, scan, out);
    }
    return out;
}

} // namespace NKikimr::NKqp::NOpt
