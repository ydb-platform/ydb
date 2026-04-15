#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <util/string/cast.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

bool KqpPeelOneRxMapOrFlowWrapper(TExprBase& in) {
    if (in.Maybe<TCoExtractMembers>()) {
        in = in.Cast<TCoExtractMembers>().Input();
        return true;
    }
    if (in.Maybe<TCoFromFlow>()) {
        in = in.Cast<TCoFromFlow>().Input();
        return true;
    }
    if (in.Maybe<TCoToFlow>()) {
        in = in.Cast<TCoToFlow>().Input();
        return true;
    }
    if (in.Maybe<TCoWideFromBlocks>()) {
        in = in.Cast<TCoWideFromBlocks>().Input();
        return true;
    }
    if (in.Maybe<TCoWideToBlocks>()) {
        in = in.Cast<TCoWideToBlocks>().Input();
        return true;
    }
    if (in.Maybe<TCoListFromBlocks>()) {
        in = in.Cast<TCoListFromBlocks>().Input();
        return true;
    }
    if (in.Maybe<TCoListToBlocks>()) {
        in = in.Cast<TCoListToBlocks>().Input();
        return true;
    }
    if (in.Ref().IsCallable("ExpandMap")) {
        in = TExprBase(in.Ref().ChildPtr(0));
        return true;
    }
    if (in.Maybe<TCoMapBase>()) {
        in = in.Cast<TCoMapBase>().Input();
        return true;
    }
    return false;
}

TExprBase KqpSubstituteReadPreservingRxWrappers(
    TExprBase root,
    const TExprNode* oldReadRaw,
    TExprBase newRead,
    TExprContext& ctx,
    TPositionHandle pos)
{
    if (root.Raw() == oldReadRaw) {
        return newRead;
    }
    if (root.Maybe<TCoExtractMembers>()) {
        const auto em = root.Cast<TCoExtractMembers>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(em.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == em.Input().Raw()) {
            return root;
        }
        return Build<TCoExtractMembers>(ctx, pos)
            .Input(inner)
            .Members(em.Members())
            .Done();
    }
    if (root.Maybe<TCoFromFlow>()) {
        const auto n = root.Cast<TCoFromFlow>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoFromFlow>(ctx, pos).Input(inner).Done();
    }
    if (root.Maybe<TCoToFlow>()) {
        const auto n = root.Cast<TCoToFlow>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoToFlow>(ctx, pos).Input(inner).Done();
    }
    if (root.Maybe<TCoWideFromBlocks>()) {
        const auto n = root.Cast<TCoWideFromBlocks>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoWideFromBlocks>(ctx, pos).Input(inner).Done();
    }
    if (root.Maybe<TCoWideToBlocks>()) {
        const auto n = root.Cast<TCoWideToBlocks>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoWideToBlocks>(ctx, pos).Input(inner).Done();
    }
    if (root.Maybe<TCoListFromBlocks>()) {
        const auto n = root.Cast<TCoListFromBlocks>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoListFromBlocks>(ctx, pos).Input(inner).Done();
    }
    if (root.Maybe<TCoListToBlocks>()) {
        const auto n = root.Cast<TCoListToBlocks>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(n.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == n.Input().Raw()) {
            return root;
        }
        return Build<TCoListToBlocks>(ctx, pos).Input(inner).Done();
    }
    if (root.Ref().IsCallable("ExpandMap")) {
        const auto inner = KqpSubstituteReadPreservingRxWrappers(TExprBase(root.Ref().ChildPtr(0)), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == root.Ref().ChildPtr(0).Get()) {
            return root;
        }
        return TExprBase(ctx.ChangeChild(root.Ref(), 0u, inner.Ptr()));
    }
    if (root.Maybe<TCoMapBase>()) {
        const auto m = root.Cast<TCoMapBase>();
        const auto inner = KqpSubstituteReadPreservingRxWrappers(m.Input(), oldReadRaw, newRead, ctx, pos);
        if (inner.Raw() == m.Input().Raw()) {
            return root;
        }
        return TExprBase(ctx.ChangeChild(root.Ref(), 0u, inner.Ptr()));
    }
    return root;
}

namespace {

/// `Take` / `Limit` count is often `Uint64`, but some lowerings use `Uint32` or other plain unsigned literals.
std::optional<ui64> TryParsePlainUintLiteralForTakeCount(TExprBase count) {
    if (count.Maybe<TCoUint64>()) {
        return FromString<ui64>(count.Cast<TCoUint64>().Literal().Value());
    }
    if (count.Maybe<TCoUint32>()) {
        return FromString<ui32>(count.Cast<TCoUint32>().Literal().Value());
    }
    if (count.Maybe<TCoDataCtor>()) {
        const auto ctor = count.Cast<TCoDataCtor>();
        const auto* ann = ctor.Ref().GetTypeAnn();
        if (ann && ann->GetKind() == ETypeAnnotationKind::Data) {
            const auto slot = ann->Cast<TDataExprType>()->GetSlot();
            if (slot == EDataSlot::Uint64) {
                return FromString<ui64>(ctor.Literal().Value());
            }
            if (slot == EDataSlot::Uint32) {
                return FromString<ui32>(ctor.Literal().Value());
            }
        }
    }
    return std::nullopt;
}

TCoUint64 BuildCoUint64Literal(TExprContext& ctx, TPositionHandle pos, ui64 value) {
    return Build<TCoUint64>(ctx, pos)
        .Literal<TCoAtom>()
        .Value(ToString(value))
        .Build()
        .Done();
}

bool TryGetDistinctColumn(const TCoAggregateCombine& aggCombine, TString& column) {
    if (aggCombine.Handlers().Size() != 1) {
        return false;
    }
    const auto handler = aggCombine.Handlers().Item(0);
    if (!handler.Trait().Maybe<TCoAggApply>()) {
        return false;
    }
    const auto aggApply = handler.Trait().Cast<TCoAggApply>();
    if (aggApply.Name().StringValue() != "distinct") {
        return false;
    }
    const auto body = aggApply.Extractor().Body();
    if (!body.Maybe<TCoMember>()) {
        return false;
    }
    column = TString(body.Cast<TCoMember>().Name().StringValue());
    return true;
}

bool IsOlapDistinctCombine(const TCoAggregateCombine& aggCombine, TString& column) {
    if (!TryGetDistinctColumn(aggCombine, column)) {
        return false;
    }
    if (aggCombine.Keys().Size() != 1) {
        return false;
    }
    if (aggCombine.Keys().Item(0).StringValue() != column) {
        return false;
    }
    // Logical SQL `SELECT DISTINCT col` usually carries the `distinct_all` setting, but some lowering
    // passes into the physical/DQ shape may omit it while preserving the narrow AggregateCombine form
    // (single key, single `distinct` handler). Rely on the structural checks above for OLAP pushdown.
    return true;
}

TExprBase PeelFlowAndBlockWrappersToInner(TExprBase in) {
    while (KqpPeelOneRxMapOrFlowWrapper(in)) {
    }
    return in;
}

TMaybeNode<TExprBase> TryGetUnderlyingOlapRead(TExprBase dataInput) {
    const auto peeled = PeelFlowAndBlockWrappersToInner(dataInput);
    if (peeled.Maybe<TKqpReadOlapTableRanges>()) {
        return peeled;
    }
    if (peeled.Maybe<TKqpBlockReadOlapTableRanges>()) {
        return peeled;
    }
    if (peeled.Maybe<TKqpWideReadOlapTableRanges>()) {
        return peeled;
    }
    return {};
}

TCoLambda OlapReadProcess(TExprBase readExpr) {
    if (readExpr.Maybe<TKqpReadOlapTableRanges>()) {
        return readExpr.Cast<TKqpReadOlapTableRanges>().Process();
    }
    if (readExpr.Maybe<TKqpBlockReadOlapTableRanges>()) {
        return readExpr.Cast<TKqpBlockReadOlapTableRanges>().Process();
    }
    if (readExpr.Maybe<TKqpWideReadOlapTableRanges>()) {
        return readExpr.Cast<TKqpWideReadOlapTableRanges>().Process();
    }
    YQL_ENSURE(false, "Unexpected OLAP read in OlapReadProcess");
}

TExprBase RebuildOlapReadWithProcess(
    TExprBase readExpr,
    const TCoNameValueTupleList& newSettings,
    const TExprNode::TPtr& newProcess,
    TExprContext& ctx,
    TPositionHandle pos)
{
    if (readExpr.Maybe<TKqpReadOlapTableRanges>()) {
        const auto read = readExpr.Cast<TKqpReadOlapTableRanges>();
        return Build<TKqpReadOlapTableRanges>(ctx, pos)
            .Table(read.Table())
            .Ranges(read.Ranges())
            .Columns(read.Columns())
            .Settings(newSettings)
            .ExplainPrompt(read.ExplainPrompt())
            .Process(newProcess)
            .Done();
    }
    if (readExpr.Maybe<TKqpBlockReadOlapTableRanges>()) {
        const auto read = readExpr.Cast<TKqpBlockReadOlapTableRanges>();
        return Build<TKqpBlockReadOlapTableRanges>(ctx, pos)
            .Table(read.Table())
            .Ranges(read.Ranges())
            .Columns(read.Columns())
            .Settings(newSettings)
            .ExplainPrompt(read.ExplainPrompt())
            .Process(newProcess)
            .Done();
    }
    if (readExpr.Maybe<TKqpWideReadOlapTableRanges>()) {
        const auto read = readExpr.Cast<TKqpWideReadOlapTableRanges>();
        return Build<TKqpWideReadOlapTableRanges>(ctx, pos)
            .Table(read.Table())
            .Ranges(read.Ranges())
            .Columns(read.Columns())
            .Settings(newSettings)
            .ExplainPrompt(read.ExplainPrompt())
            .Process(newProcess)
            .Done();
    }
    YQL_ENSURE(false, "Unexpected OLAP read node in RebuildOlapReadWithProcess");
}

} // namespace

bool TryGetSingleOlapReadProjectionColumn(TExprBase read, TString& column) {
    if (read.Maybe<TKqpReadOlapTableRanges>()) {
        const auto r = read.Cast<TKqpReadOlapTableRanges>();
        if (r.Columns().Size() != 1) {
            return false;
        }
        column = TString(r.Columns().Item(0).StringValue());
        return true;
    }
    if (read.Maybe<TKqpBlockReadOlapTableRanges>()) {
        const auto r = read.Cast<TKqpBlockReadOlapTableRanges>();
        if (r.Columns().Size() != 1) {
            return false;
        }
        column = TString(r.Columns().Item(0).StringValue());
        return true;
    }
    if (read.Maybe<TKqpWideReadOlapTableRanges>()) {
        const auto r = read.Cast<TKqpWideReadOlapTableRanges>();
        if (r.Columns().Size() != 1) {
            return false;
        }
        column = TString(r.Columns().Item(0).StringValue());
        return true;
    }
    return false;
}

/// Strip leading `Take`/`Limit` and stream/block wrappers until `TryGetUnderlyingOlapRead` succeeds.
/// Needed for shapes like `FromFlow(Limit(ToFlow(Read)))` where a single `Take` peel is not enough.
bool PeelTowardsUnderlyingOlapRead(
    TExprBase& dataInput,
    TMaybeNode<TCoUint64>& maybeLimit,
    TExprContext& ctx,
    TPositionHandle pos)
{
    for (;;) {
        if (TryGetUnderlyingOlapRead(dataInput)) {
            return true;
        }
        if (dataInput.Maybe<TCoTake>() || dataInput.Maybe<TCoLimit>()) {
            const auto takeBase = dataInput.Cast<TCoTakeBase>();
            if (const auto lit = TryParsePlainUintLiteralForTakeCount(takeBase.Count())) {
                if (maybeLimit) {
                    return false;
                }
                maybeLimit = BuildCoUint64Literal(ctx, pos, *lit);
            } else {
                return false;
            }
            dataInput = takeBase.Input();
            continue;
        }
        if (KqpPeelOneRxMapOrFlowWrapper(dataInput)) {
            continue;
        }
        return false;
    }
}

TExprBase KqpPushOlapDistinct(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.Config->HasOptEnableOlapPushdown() || !kqpCtx.Config->GetEnableOlapPushdownDistinct()) {
        return node;
    }

    if (!node.Maybe<TCoAggregateCombine>()) {
        return node;
    }
    const auto aggCombine = node.Cast<TCoAggregateCombine>();

    TString column;
    if (!IsOlapDistinctCombine(aggCombine, column)) {
        return node;
    }

    TMaybeNode<TCoUint64> maybeLimit;
    TExprBase dataInput = aggCombine.Input();

    if (!PeelTowardsUnderlyingOlapRead(dataInput, maybeLimit, ctx, node.Pos())) {
        return node;
    }
    const auto read = TryGetUnderlyingOlapRead(dataInput).Cast();
    const auto process = OlapReadProcess(read);

    if (NYql::HasSetting(read.Cast<TKqlReadTableRangesBase>().Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return node;
    }

    auto distinctBuilder = Build<TKqpOlapDistinct>(ctx, node.Pos())
        .Input(process.Args().Arg(0))
        .Column().Build(column);
    if (maybeLimit) {
        distinctBuilder.Limit(maybeLimit.Cast());
    }
    const auto olapDistinct = distinctBuilder.Done();

    const auto olapDistinctLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_distinct_row"})
        .Body<TExprApplier>()
            .Apply(olapDistinct)
            .With(process.Args().Arg(0), "olap_distinct_row")
            .Build()
        .Done();

    const auto newProcessLambda = ctx.FuseLambdas(olapDistinctLambda.Ref(), process.Ref());

    YQL_CLOG(INFO, ProviderKqp) << "Pushed OLAP Distinct lambda: " << KqpExprToPrettyString(*newProcessLambda, ctx);

    auto settings = TKqpReadTableSettings::Parse(read.Cast<TKqlReadTableRangesBase>());
    if (maybeLimit && !settings.ItemsLimit) {
        settings.SetItemsLimit(maybeLimit.Cast().Ptr());
    }
    const auto newSettings = settings.BuildNode(ctx, read.Pos());

    const auto newRead = RebuildOlapReadWithProcess(read, newSettings, newProcessLambda, ctx, node.Pos());
    return KqpSubstituteReadPreservingRxWrappers(dataInput, read.Raw(), newRead, ctx, node.Pos());
}

static TExprBase PushOlapDistinctWideCombinerImpl(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoWideCombiner>()) {
        return node;
    }
    const auto wc = node.Cast<TCoWideCombiner>();
    TMaybeNode<TCoUint64> maybeLimit;
    TExprBase dataInput = wc.Input();
    if (!PeelTowardsUnderlyingOlapRead(dataInput, maybeLimit, ctx, node.Pos())) {
        return node;
    }
    const auto read = TryGetUnderlyingOlapRead(dataInput).Cast();
    TString column;
    if (!TryGetSingleOlapReadProjectionColumn(read, column)) {
        return node;
    }
    if (NYql::HasSetting(read.Cast<TKqlReadTableRangesBase>().Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return node;
    }

    const auto process = OlapReadProcess(read);
    auto distinctBuilder = Build<TKqpOlapDistinct>(ctx, node.Pos())
        .Input(process.Args().Arg(0))
        .Column().Build(column);
    if (maybeLimit) {
        distinctBuilder.Limit(maybeLimit.Cast());
    }
    const auto olapDistinct = distinctBuilder.Done();

    const auto olapDistinctLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_distinct_row"})
        .Body<TExprApplier>()
            .Apply(olapDistinct)
            .With(process.Args().Arg(0), "olap_distinct_row")
            .Build()
        .Done();

    const auto newProcessLambda = ctx.FuseLambdas(olapDistinctLambda.Ref(), process.Ref());

    YQL_CLOG(INFO, ProviderKqp) << "Pushed OLAP Distinct (WideCombiner) lambda: " << KqpExprToPrettyString(*newProcessLambda, ctx);

    auto settings = TKqpReadTableSettings::Parse(read.Cast<TKqlReadTableRangesBase>());
    if (maybeLimit && !settings.ItemsLimit) {
        settings.SetItemsLimit(maybeLimit.Cast().Ptr());
    }
    const auto newSettings = settings.BuildNode(ctx, read.Pos());
    const auto newRead = RebuildOlapReadWithProcess(read, newSettings, newProcessLambda, ctx, node.Pos());
    return KqpSubstituteReadPreservingRxWrappers(dataInput, read.Raw(), newRead, ctx, node.Pos());
}

static TExprBase PushOlapDistinctTakeInjectLimitImpl(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoTake>() && !node.Maybe<TCoLimit>()) {
        return node;
    }
    const auto takeBase = node.Cast<TCoTakeBase>();
    const auto limitLit = TryParsePlainUintLiteralForTakeCount(takeBase.Count());
    if (!limitLit) {
        return node;
    }
    const auto limit = BuildCoUint64Literal(ctx, node.Pos(), *limitLit);

    const auto olapReadPred = [](const TExprNode::TPtr& n) {
        return TKqpReadOlapTableRanges::Match(n.Get()) || TKqpBlockReadOlapTableRanges::Match(n.Get())
            || TKqpWideReadOlapTableRanges::Match(n.Get());
    };
    TExprBase probe = takeBase.Input();
    TExprNode::TPtr readNode;
    for (ui32 hop = 0; hop < 32u; ++hop) {
        readNode = FindNode(probe.Ptr(), olapReadPred);
        if (readNode) {
            break;
        }
        if (!KqpPeelOneRxMapOrFlowWrapper(probe)) {
            break;
        }
    }
    if (!readNode) {
        return node;
    }
    const TExprBase read(readNode);
    if (NYql::HasSetting(read.Cast<TKqlReadTableRangesBase>().Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return node;
    }

    const auto process = OlapReadProcess(read);
    const auto distinctNode = FindNode(process.Ptr(), [](const TExprNode::TPtr& n) { return TKqpOlapDistinct::Match(n.Get()); });
    if (!distinctNode) {
        return node;
    }
    const auto oldDistinct = TExprBase(distinctNode).Cast<TKqpOlapDistinct>();
    if (oldDistinct.Limit()) {
        return node;
    }

    const auto newDistinct = Build<TKqpOlapDistinct>(ctx, oldDistinct.Pos())
        .Input(oldDistinct.Input())
        .Column(oldDistinct.Column())
        .Limit(limit)
        .Done();

    const auto newProcessPtr = ctx.ReplaceNode(process.Ptr(), *distinctNode, newDistinct.Ptr());
    if (newProcessPtr == process.Ptr()) {
        return node;
    }

    auto settings = TKqpReadTableSettings::Parse(read.Cast<TKqlReadTableRangesBase>());
    if (!settings.ItemsLimit) {
        settings.SetItemsLimit(limit.Ptr());
    }
    const auto newSettings = settings.BuildNode(ctx, read.Pos());
    const auto newRead = RebuildOlapReadWithProcess(read, newSettings, newProcessPtr, ctx, node.Pos());
    return KqpSubstituteReadPreservingRxWrappers(takeBase.Input(), read.Raw(), newRead, ctx, node.Pos());
}

TExprBase KqpPushOlapDistinctPeephole(TExprBase node, TExprContext& ctx, const TKikimrConfiguration::TPtr& config) {
    if (!config->HasOptEnableOlapPushdown() || !config->GetEnableOlapPushdownDistinct()) {
        return node;
    }
    if (node.Maybe<TCoTake>() || node.Maybe<TCoLimit>()) {
        return PushOlapDistinctTakeInjectLimitImpl(node, ctx);
    }
    if (node.Maybe<TCoWideCombiner>()) {
        return PushOlapDistinctWideCombinerImpl(node, ctx);
    }
    return node;
}

TAutoPtr<NYql::IGraphTransformer> CreateKqpOlapDistinctPrePhyStagesTransformer(
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx)
{
    return NYql::CreateFunctorTransformer(
        [kqpCtx, &typesCtx](const NYql::TExprNode::TPtr& input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) -> NYql::IGraphTransformer::TStatus {
            NYql::TOptimizeExprSettings settings(&typesCtx);
            settings.VisitLambdas = true;
            settings.VisitChanges = true;
            NYql::TCallableOptimizer opt = [kqpCtx](const NYql::TExprNode::TPtr& node, NYql::TExprContext& ctxInner) -> NYql::TExprNode::TPtr {
                if (!TCoAggregateCombine::Match(node.Get())) {
                    return node;
                }
                const TExprBase repl = KqpPushOlapDistinct(TExprBase(node), ctxInner, *kqpCtx);
                return repl.Ptr();
            };
            return NYql::OptimizeExpr(input, output, opt, ctx, settings);
        });
}

} // namespace NKikimr::NKqp::NOpt
