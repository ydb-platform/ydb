#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TKqpLogicalOptTransformer : public TOptimizeTransformerBase {
public:
    TKqpLogicalOptTransformer(TTypeAnnotationContext& typesCtx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TKikimrConfiguration::TPtr& config)
        : TOptimizeTransformerBase(nullptr, NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
        , Config(config)
    {
#define HNDL(name) "KqpLogical-"#name, Hndl(&TKqpLogicalOptTransformer::name)
        AddHandler(0, &TCoFlatMap::Match, HNDL(PushExtractedPredicateToReadTable));
        AddHandler(0, &TCoFlatMap::Match, HNDL(PushPredicateToReadTable));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ApplyExtractMembersToReadTable));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ApplyExtractMembersToReadTableRanges));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ApplyExtractMembersToReadOlapTable));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInToEquiJoin));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInCompactToJoin));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(JoinToIndexLookup));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoTopSort::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeOverIndexRead));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverExtend));
        AddHandler(0, &TKqlDeleteRows::Match, HNDL(DeleteOverLookup));
        AddHandler(0, &TKqlUpsertRowsBase::Match, HNDL(ExcessUpsertInputColumns));

        AddHandler(1, &TKqlReadTableIndex::Match, HNDL(RewriteIndexRead));
        AddHandler(1, &TKqlLookupIndex::Match, HNDL(RewriteLookupIndex));
#undef HNDL
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
        TExprBase output = DqRewriteAggregate(node, ctx);
        DumpAppliedRule("RewriteAggregate", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ApplyExtractMembersToReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpApplyExtractMembersToReadTable(node, ctx);
        DumpAppliedRule("ApplyExtractMembersToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ApplyExtractMembersToReadTableRanges(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpApplyExtractMembersToReadTableRanges(node, ctx);
        DumpAppliedRule("ApplyExtractMembersToReadTableRanges", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpApplyExtractMembersToReadOlapTable(node, ctx);
        DumpAppliedRule("ApplyExtractMembersToReadOlapTable", node.Ptr(), output.Ptr(), ctx);
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
