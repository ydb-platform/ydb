#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
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
        const TKikimrConfiguration::TPtr& config)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
        , Config(config)
    {
#define HNDL(name) "KqpLogical-"#name, Hndl(&TKqpLogicalOptTransformer::name)
        AddHandler(0, &TCoFlatMap::Match, HNDL(PushPredicateToReadTable));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInToEquiJoin));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInCompactToJoin));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(JoinToIndexLookup));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoTop::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(0, &TCoTopSort::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeOverIndexRead));
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

        AddHandler(1, &TCoFlatMap::Match, HNDL(PushExtractedPredicateToReadTable));
        AddHandler(1, &TKqlReadTableIndex::Match, HNDL(RewriteIndexRead));
        AddHandler(1, &TKqlLookupIndex::Match, HNDL(RewriteLookupIndex));
        AddHandler(1, &TKqlStreamLookupIndex::Match, HNDL(RewriteStreamLookupIndex));

        AddHandler(2, &TKqlReadTableIndexRanges::Match, HNDL(RewriteIndexRead));
        AddHandler(2, &TKqlLookupTable::Match, HNDL(RewriteLookupTable));

        AddHandler(3, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(3, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTableRanges<true>));
        AddHandler(3, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<true>));
        AddHandler(3, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToLookupTable<true>));

#undef HNDL

        SetGlobal(3u);
    }

protected:

    TMaybeNode<TExprBase> PushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushExtractedPredicateToReadTable(node, ctx, KqpCtx, TypesCtx);
        DumpAppliedRule("PushExtractedPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushPredicateToReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushPredicateToReadTable(node, ctx, KqpCtx);
        DumpAppliedRule("PushPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteAggregate(node, ctx, TypesCtx, false, KqpCtx.Config->HasOptEnableOlapPushdown(), KqpCtx.Config->HasOptUseFinalizeByKey());
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

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteEquiJoin(node, ctx);
        DumpAppliedRule("RewriteEquiJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> JoinToIndexLookup(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpJoinToIndexLookup(node, ctx, KqpCtx, Config);
        DumpAppliedRule("JoinToIndexLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqExpandWindowFunctions(node, ctx, true);
        DumpAppliedRule("ExpandWindowFunctions", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverIndexRead(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteTopSortOverIndexRead(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteTopSortOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTakeOverIndexRead(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteTakeOverIndexRead(node, ctx, KqpCtx);
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

    TMaybeNode<TExprBase> DeleteOverLookup(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpDeleteOverLookup(node, ctx, KqpCtx);
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
};

TAutoPtr<IGraphTransformer> CreateKqpLogOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typesCtx, const TKikimrConfiguration::TPtr& config)
{
    return THolder<IGraphTransformer>(new TKqpLogicalOptTransformer(typesCtx, kqpCtx, config));
}

} // namespace NKikimr::NKqp::NOpt
