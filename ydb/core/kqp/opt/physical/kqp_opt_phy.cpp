#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/effects/kqp_opt_phy_effects_rules.h>

#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

class TKqpPhysicalOptTransformer : public TOptimizeTransformerBase {
public:
    TKqpPhysicalOptTransformer(TTypeAnnotationContext& typesCtx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
    {
#define HNDL(name) "KqpPhysical-"#name, Hndl(&TKqpPhysicalOptTransformer::name)
        AddHandler(0, &TKqlReadTable::Match, HNDL(BuildReadTableStage));
        AddHandler(0, &TKqlReadTableRanges::Match, HNDL(BuildReadTableRangesStage));
        AddHandler(0, &TKqlLookupTable::Match, HNDL(BuildLookupTableStage));
        AddHandler(0, &TKqlStreamLookupTable::Match, HNDL(BuildStreamLookupTableStages));
        AddHandler(0, [](auto) { return true; }, HNDL(RemoveRedundantSortByPk));
        AddHandler(0, &TCoTake::Match, HNDL(ApplyLimitToReadTable));
        AddHandler(0, &TCoFlatMap::Match, HNDL(PushOlapFilter));
        AddHandler(0, &TCoAggregateCombine::Match, HNDL(PushAggregateCombineToStage));
        AddHandler(0, &TCoAggregateCombine::Match, HNDL(PushOlapAggregate));
        AddHandler(0, &TDqPhyLength::Match, HNDL(PushOlapLength));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<false>));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<false>));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(BuildPureFlatmapStage));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<false>));
        AddHandler(0, &TCoCombineByKey::Match, HNDL(PushCombineToStage<false>));
        AddHandler(0, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage));
        AddHandler(0, &TCoFinalizeByKey::Match, HNDL(BuildFinalizeByKeyStage));
        AddHandler(0, &TCoShuffleByKeys::Match, HNDL(BuildShuffleStage));
        AddHandler(0, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage));
        AddHandler(0, &TCoTopSort::Match, HNDL(BuildTopSortStage<false>));
        AddHandler(0, &TCoTakeBase::Match, HNDL(BuildTakeSkipStage<false>));
        AddHandler(0, &TCoSortBase::Match, HNDL(BuildSortStage<false>));
        AddHandler(0, &TCoTakeBase::Match, HNDL(BuildTakeStage<false>));
        AddHandler(0, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput<false>));
        AddHandler(0, &TCoExtendBase::Match, HNDL(BuildExtendStage));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteRightJoinToLeft));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteLeftPureJoin<false>));
        AddHandler(0, &TDqJoin::Match, HNDL(BuildJoin<false>));
        AddHandler(0, &TDqPrecompute::Match, HNDL(BuildPrecompute));
        AddHandler(0, &TCoLMap::Match, HNDL(PushLMapToStage<false>));
        AddHandler(0, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<false>));
        AddHandler(0, &TKqlInsertRows::Match, HNDL(BuildInsertStages));
        AddHandler(0, &TKqlUpdateRows::Match, HNDL(BuildUpdateStages));
        AddHandler(0, &TKqlUpdateRowsIndex::Match, HNDL(BuildUpdateIndexStages));
        AddHandler(0, &TKqlUpsertRowsIndex::Match, HNDL(BuildUpsertIndexStages));
        AddHandler(0, &TKqlInsertRowsIndex::Match, HNDL(BuildInsertIndexStages));
        AddHandler(0, &TKqlDeleteRowsIndex::Match, HNDL(BuildDeleteIndexStages));
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(DropUnordered));
        AddHandler(0, &TDqStage::Match, HNDL(PrecomputeToInput));
        AddHandler(0, &TDqStage::Match, HNDL(FloatUpStage));
        AddHandler(0, &TCoHasItems::Match, HNDL(BuildHasItems));
        AddHandler(0, &TCoSqlIn::Match, HNDL(BuildSqlIn<false>));
        AddHandler(0, &TCoHead::Match, HNDL(BuildScalarPrecompute<false>));
        AddHandler(0, &TCoToOptional::Match, HNDL(BuildScalarPrecompute<false>));
        AddHandler(0, &TCoAsList::Match, HNDL(PropagatePrecomuteScalarRowset<false>));
        AddHandler(0, &TCoTake::Match, HNDL(PropagatePrecomuteTake<false>));
        AddHandler(0, &TCoFlatMap::Match, HNDL(PropagatePrecomuteFlatmap<false>));

        AddHandler(0, &TCoAggregateCombine::Match, HNDL(ExpandAggregatePhase));
        AddHandler(0, &TCoAggregateCombineState::Match, HNDL(ExpandAggregatePhase));
        AddHandler(0, &TCoAggregateMergeState::Match, HNDL(ExpandAggregatePhase));
        AddHandler(0, &TCoAggregateMergeFinalize::Match, HNDL(ExpandAggregatePhase));
        AddHandler(0, &TCoAggregateMergeManyFinalize::Match, HNDL(ExpandAggregatePhase));
        AddHandler(0, &TCoAggregateFinalize::Match, HNDL(ExpandAggregatePhase));

        AddHandler(1, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<true>));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<true>));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<true>));
        AddHandler(1, &TCoCombineByKey::Match, HNDL(PushCombineToStage<true>));
        AddHandler(1, &TCoTopSort::Match, HNDL(BuildTopSortStage<true>));
        AddHandler(1, &TCoTakeBase::Match, HNDL(BuildTakeSkipStage<true>));
        AddHandler(1, &TCoSortBase::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoTakeBase::Match, HNDL(BuildTakeStage<true>));
        AddHandler(1, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(RewriteLeftPureJoin<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(BuildJoin<true>));
        AddHandler(1, &TCoLMap::Match, HNDL(PushLMapToStage<true>));
        AddHandler(1, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<true>));
        AddHandler(1, &TCoSqlIn::Match, HNDL(BuildSqlIn<true>));
        AddHandler(1, &TCoHead::Match, HNDL(BuildScalarPrecompute<true>));
        AddHandler(1, &TCoToOptional::Match, HNDL(BuildScalarPrecompute<true>));
        AddHandler(1, &TCoAsList::Match, HNDL(PropagatePrecomuteScalarRowset<true>));
        AddHandler(1, &TCoTake::Match, HNDL(PropagatePrecomuteTake<true>));
        AddHandler(1, &TCoFlatMap::Match, HNDL(PropagatePrecomuteFlatmap<true>));

        AddHandler(2, &TDqStage::Match, HNDL(RewriteKqpReadTable));
#undef HNDL

        SetGlobal(1u);
    }

protected:
    TMaybeNode<TExprBase> BuildReadTableStage(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildReadTableStage(node, ctx, KqpCtx);
        DumpAppliedRule("BuildReadTableStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildReadTableRangesStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto parents = getParents();
        TExprBase output = KqpBuildReadTableRangesStage(node, ctx, KqpCtx, *parents);
        DumpAppliedRule("BuildReadTableRangesStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildLookupTableStage(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildLookupTableStage(node, ctx);
        DumpAppliedRule("BuildLookupTableStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildStreamLookupTableStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildStreamLookupTableStages(node, ctx);
        DumpAppliedRule("BuildStreamLookupTableStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RemoveRedundantSortByPk(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRemoveRedundantSortByPk(node, ctx, KqpCtx);
        DumpAppliedRule("RemoveRedundantSortByPk", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteKqpReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteReadTable(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteKqpReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ApplyLimitToReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpApplyLimitToReadTable(node, ctx, KqpCtx);
        DumpAppliedRule("ApplyLimitToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushOlapFilter(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushOlapFilter(node, ctx, KqpCtx, TypesCtx);
        DumpAppliedRule("PushOlapFilter", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushAggregateCombineToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushAggregateCombineToStage(node, ctx, optCtx, *getParents(), false);
        DumpAppliedRule("PushAggregateCombineToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushOlapAggregate(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushOlapAggregate(node, ctx, KqpCtx);
        DumpAppliedRule("PushOlapAggregate", node.Ptr(), output.Ptr(), ctx);
        return output;
    }


    TMaybeNode<TExprBase> ExpandAggregatePhase(TExprBase node, TExprContext& ctx) {
        auto output = ExpandAggregatePeepholeImpl(node.Ptr(), ctx, TypesCtx, KqpCtx.Config->HasOptUseFinalizeByKey(), false);
        DumpAppliedRule("ExpandAggregatePhase", node.Ptr(), output, ctx);
        return TExprBase(output);
    }

    TMaybeNode<TExprBase> PushOlapLength(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushOlapLength(node, ctx, KqpCtx);
        DumpAppliedRule("PushOlapLength", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushSkipNullMembersToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushSkipNullMembersToStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PushSkipNullMembersToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushExtractMembersToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushExtractMembersToStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PushExtractMembersToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildPureFlatmapStage(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqBuildPureFlatmapStage(node, ctx);
        DumpAppliedRule("BuildPureFlatmapStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildFlatmapStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildFlatmapStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildFlatmapStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushCombineToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushCombineToStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PushCombineToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildShuffleStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqBuildShuffleStage(node, ctx, *getParents());
        DumpAppliedRule("BuildShuffleStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildFinalizeByKeyStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqBuildFinalizeByKeyStage(node, ctx, *getParents());
        DumpAppliedRule("BuildFinalizeByKeyStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildPartitionsStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqBuildPartitionsStage(node, ctx, *getParents());
        DumpAppliedRule("BuildPartitionsStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildPartitionStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqBuildPartitionStage(node, ctx, *getParents());
        DumpAppliedRule("BuildPartitionStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTopSortStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildTopSortStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildTopSortStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTakeSkipStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildTakeSkipStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildTakeSkipStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildSortStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildSortStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildSortStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTakeStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildTakeStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildTakeStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> RewriteLengthOfStageOutput(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqRewriteLengthOfStageOutput(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("RewriteLengthOfStageOutput", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildExtendStage(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqBuildExtendStage(node, ctx);
        DumpAppliedRule("BuildExtendStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteRightJoinToLeft(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteRightJoinToLeft(node, ctx);
        DumpAppliedRule("RewriteRightJoinToLeft", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> RewriteLeftPureJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents)
    {
        TExprBase output = DqRewriteLeftPureJoin(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("RewriteLeftPureJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildJoin(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildJoin(node, ctx, optCtx, *getParents(), IsGlobal, /*pushLeftStage =*/ !KqpCtx.IsDataQuery() && AllowFuseJoinInputs(node));
        DumpAppliedRule("BuildJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildPrecompute(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqBuildPrecompute(node, ctx);
        DumpAppliedRule("BuildPrecompute", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushLMapToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushLMapToStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PushLMapToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushOrderedLMapToStage(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPushOrderedLMapToStage(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PushOrderedLMapToStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildInsertStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildInsertStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildInsertStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildUpdateStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildUpdateStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildUpdateStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildUpdateIndexStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildUpdateIndexStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildUpdateIndexStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildUpsertIndexStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildUpsertIndexStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildUpsertIndexStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildInsertIndexStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildInsertIndexStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildInsertIndexStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildDeleteIndexStages(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildDeleteIndexStages(node, ctx, KqpCtx);
        DumpAppliedRule("BuildDeleteIndexStages", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> DropUnordered(TExprBase node, TExprContext& ctx) {
        TExprBase output = node;
        if (node.Maybe<TCoUnorderedBase>().Input().Maybe<TDqCnUnionAll>()) {
            output = node.Cast<TCoUnorderedBase>().Input();
        }
        DumpAppliedRule("DropUnordered", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> FloatUpStage(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpFloatUpStage(node, ctx);
        DumpAppliedRule("FloatUpStage", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildHasItems(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        TExprBase output = DqBuildHasItems(node, ctx, optCtx);
        DumpAppliedRule("DqBuildHasItems", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildSqlIn(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildSqlIn(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildSqlIn", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildScalarPrecompute(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqBuildScalarPrecompute(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("BuildScalarPrecompute", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PropagatePrecomuteScalarRowset(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = KqpPropagatePrecomuteScalarRowset(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PropagatePrecomuteScalarRowset", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PropagatePrecomuteTake(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPropagatePrecomuteTake(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PropagatePrecomuteTake", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PropagatePrecomuteFlatmap(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        TExprBase output = DqPropagatePrecomuteFlatmap(node, ctx, optCtx, *getParents(), IsGlobal);
        DumpAppliedRule("PropagatePrecomuteFlatmap", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PrecomputeToInput(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPrecomputeToInput(node, ctx);
        DumpAppliedRule("PrecomputeToInput", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

private:
    TTypeAnnotationContext& TypesCtx;
    const TKqpOptimizeContext& KqpCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpPhyOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx)
{
    return THolder<IGraphTransformer>(new TKqpPhysicalOptTransformer(typesCtx, kqpCtx));
}

} // namespace NKikimr::NKqp::NOpt
