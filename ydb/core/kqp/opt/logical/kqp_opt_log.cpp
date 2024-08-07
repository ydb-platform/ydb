#include "kqp_opt_log_rules.h"
#include "kqp_opt_cbo.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_match_recognize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TKqpLogicalOptTransformer : public TOptimizeTransformerBase {
public:
    TKqpLogicalOptTransformer(TTypeAnnotationContext& typesCtx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TKikimrConfiguration::TPtr& config, TKqpProviderContext& pctx)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
        , Config(config)
        , Pctx(pctx)
    {
#define HNDL(name) "KqpLogical-"#name, Hndl(&TKqpLogicalOptTransformer::name)
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(PushPredicateToReadTable));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(PushExtractedPredicateToReadTable));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoAggregateCombine::Match, HNDL(PushdownOlapGroupByKeys));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInToEquiJoin));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInCompactToJoin));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(OptimizeEquiJoinWithCosts));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(JoinToIndexLookup));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoTopSort::Match, HNDL(RewriteTopSortOverFlatMap));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverExtend));
        AddHandler(0, &TKqlDeleteRows::Match, HNDL(DeleteOverLookup));
        AddHandler(0, &TKqlUpsertRowsBase::Match, HNDL(ExcessUpsertInputColumns));
        AddHandler(0, &TCoTake::Match, HNDL(DropTakeOverLookupTable));
        AddHandler(0, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTableRanges<false>));
        AddHandler(0, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<false>));
        AddHandler(0, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToLookupTable<false>));
        AddHandler(0, &TCoTop::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoTopSort::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoMatchRecognize::Match, HNDL(MatchRecognize));

        AddHandler(1, &TCoFlatMap::Match, HNDL(LatePushExtractedPredicateToReadTable));
        AddHandler(1, &TCoTop::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTopSort::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTake::Match, HNDL(RewriteTakeOverIndexRead));

        AddHandler(2, &TKqlReadTableIndex::Match, HNDL(RewriteIndexRead));
        AddHandler(2, &TKqlLookupIndex::Match, HNDL(RewriteLookupIndex));
        AddHandler(2, &TKqlStreamLookupIndex::Match, HNDL(RewriteStreamLookupIndex));
        AddHandler(2, &TKqlReadTableIndexRanges::Match, HNDL(RewriteIndexRead));

        AddHandler(3, &TKqlLookupTable::Match, HNDL(RewriteLookupTable));

        AddHandler(4, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(4, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTableRanges<true>));
        AddHandler(4, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<true>));
        AddHandler(4, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToLookupTable<true>));

#undef HNDL

        SetGlobal(4u);
    }

protected:
    TMaybeNode<TExprBase> PushPredicateToReadTable(TExprBase node, TExprContext& ctx) {
        if (KqpCtx.Config->PredicateExtract20) {
            return node;
        }
        TExprBase output = KqpPushPredicateToReadTable(node, ctx, KqpCtx);
        DumpAppliedRule("PushPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        if (!KqpCtx.Config->PredicateExtract20) {
            return node;
        }
        TExprBase output = KqpPushExtractedPredicateToReadTable(node, ctx, KqpCtx, TypesCtx, *getParents());
        DumpAppliedRule("PushExtractedPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> LatePushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        if (KqpCtx.Config->PredicateExtract20) {
            return node;
        }
        TExprBase output = KqpPushExtractedPredicateToReadTable(node, ctx, KqpCtx, TypesCtx, *getParents());
        DumpAppliedRule("PushExtractedPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushdownOlapGroupByKeys(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushDownOlapGroupByKeys(node, ctx, KqpCtx);
        DumpAppliedRule("PushdownOlapGroupByKeys", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteAggregate(node, ctx, TypesCtx, false, KqpCtx.Config->HasOptEnableOlapPushdown() || KqpCtx.Config->HasOptUseFinalizeByKey(), KqpCtx.Config->HasOptUseFinalizeByKey());
        DumpAppliedRule("RewriteAggregate", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqRewriteTakeSortToTopSort(node, ctx, *getParents());
        DumpAppliedRule("RewriteTakeSortToTopSort", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteSqlInToEquiJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteSqlInToEquiJoin(node, ctx, KqpCtx, Config);
        DumpAppliedRule("RewriteSqlInToEquiJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteSqlInCompactToJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteSqlInCompactToJoin(node, ctx);
        DumpAppliedRule("KqpRewriteSqlInCompactToJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx) {
        auto maxDPccpDPTableSize = Config->MaxDPccpDPTableSize.Get().GetOrElse(TDqSettings::TDefault::MaxDPccpDPTableSize);
        TExprBase output = DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel), 
            maxDPccpDPTableSize, Pctx, [](auto& rels, auto label, auto node, auto stat) {
                rels.emplace_back(std::make_shared<TKqpRelOptimizerNode>(TString(label), stat, node));
            });
        DumpAppliedRule("OptimizeEquiJoinWithCosts", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteEquiJoin(node, KqpCtx.Config->GetHashJoinMode(), ctx);
        DumpAppliedRule("RewriteEquiJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> JoinToIndexLookup(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpJoinToIndexLookup(node, ctx, KqpCtx);
        DumpAppliedRule("JoinToIndexLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqExpandWindowFunctions(node, ctx, true);
        DumpAppliedRule("ExpandWindowFunctions", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverFlatMap(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteTopSortOverFlatMap(node, ctx);
        DumpAppliedRule("RewriteTopSortOverFlatMap", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverIndexRead(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpRewriteTopSortOverIndexRead(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("RewriteTopSortOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTakeOverIndexRead(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpRewriteTakeOverIndexRead(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("RewriteTakeOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverExtend(TExprBase node, TExprContext& ctx) {
        auto output = DqFlatMapOverExtend(node, ctx);
        DumpAppliedRule("RewriteFlatMapOverExtend", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteIndexRead(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteIndexRead(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteLookupIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteLookupIndex(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteLookupIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteStreamLookupIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteStreamLookupIndex(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteStreamLookupIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteLookupTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteLookupTable(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteLookupTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> DeleteOverLookup(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpDeleteOverLookup(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("DeleteOverLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExcessUpsertInputColumns(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpExcessUpsertInputColumns(node, ctx);
        DumpAppliedRule("ExcessUpsertInputColumns", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> DropTakeOverLookupTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpDropTakeOverLookupTable(node, ctx, KqpCtx);
        DumpAppliedRule("DropTakeOverLookupTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> TopSortOverExtend(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = KqpTopSortOverExtend(node, ctx, *getParents());
        DumpAppliedRule("TopSortOverExtend", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> MatchRecognize(TExprBase node, TExprContext& ctx) {
        auto output = ExpandMatchRecognize(node.Ptr(), ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("MatchRecognize", node.Ptr(), output, ctx);
        }
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToReadTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToReadTableRanges(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadTableRanges(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadTableRanges", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadOlapTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadOlapTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToLookupTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToLookupTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToLookupTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

private:
    TTypeAnnotationContext& TypesCtx;
    const TKqpOptimizeContext& KqpCtx;
    const TKikimrConfiguration::TPtr& Config;
    TKqpProviderContext& Pctx;
};

TAutoPtr<IGraphTransformer> CreateKqpLogOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typesCtx, const TKikimrConfiguration::TPtr& config,
    TKqpProviderContext& pctx)
{
    return THolder<IGraphTransformer>(new TKqpLogicalOptTransformer(typesCtx, kqpCtx, config, pctx));
}

} // namespace NKikimr::NKqp::NOpt
