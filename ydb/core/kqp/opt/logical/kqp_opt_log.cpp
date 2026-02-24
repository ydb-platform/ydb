#include "kqp_opt_log_rules.h"
#include "kqp_opt_cbo.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/core/yql_opt_match_recognize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/opt/dq_opt_hopping.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TKqpLogicalOptTransformer : public TOptimizeTransformerBase {
public:
    TKqpLogicalOptTransformer(TTypeAnnotationContext& typesCtx, TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TKikimrConfiguration::TPtr& config)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
        , Config(config)
    {
#define HNDL(name) "KqpLogical-"#name, Hndl(&TKqpLogicalOptTransformer::name)
        AddHandler(0, &TCoTop::Match, HNDL(TopSortSelectIndex));
        AddHandler(0, &TCoTopSort::Match, HNDL(TopSortSelectIndex));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(PushExtractedPredicateToReadTable));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoAggregateCombine::Match, HNDL(PushdownOlapGroupByKeys));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInToEquiJoin));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInCompactToJoin));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteStreamEquiJoinWithLookup));
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
        AddHandler(0, &TCoTopBase::Match, HNDL(PushLimitOverFullText));
        AddHandler(0, &TCoTop::Match, HNDL(RewriteFlatMapOverFullTextMatch));
        AddHandler(0, &TCoTopSort::Match, HNDL(RewriteFlatMapOverFullTextMatch));

        AddHandler(0, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<false>));
        AddHandler(0, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqlReadTableFullTextIndex::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TCoTop::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoTopSort::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(UnorderedOverDqReadWrap));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqReadWrap));
        AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkipOverDqReadWrap));
        AddHandler(0, &TCoExtendBase::Match, HNDL(ExtendOverDqReadWrap));
        AddHandler(0, &TCoNarrowMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowFlatMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowMultiMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoWideMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoMatchRecognize::Match, HNDL(MatchRecognize));

        AddHandler(1, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverFullTextMatch));
        AddHandler(1, &TCoTop::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTopSort::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTake::Match, HNDL(RewriteTakeOverIndexRead));
        AddHandler(1, &TDqReadWrapBase::Match, HNDL(DqReadWrapByProvider));

        AddHandler(2, &TKqlReadTableIndex::Match, HNDL(RewriteIndexRead));
        AddHandler(2, &TKqlStreamLookupIndex::Match, HNDL(RewriteStreamLookupIndex));
        AddHandler(2, &TKqlReadTableIndexRanges::Match, HNDL(RewriteIndexRead));
        AddHandler(2, &TDqReadWrap::Match, HNDL(ExtractMembersOverDqReadWrapMultiUsage));
        AddHandler(2, &TDqReadWrapBase::Match, HNDL(UnorderedOverDqReadWrapMultiUsage));

        AddHandler(3, &TKqlLookupTableBase::Match, HNDL(RewriteLookupTable));

        AddHandler(4, &TKqlReadTableFullTextIndex::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(4, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(4, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(4, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<true>));
        AddHandler(4, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(5, TOptimizeTransformerBase::Any(), HNDL(InspectErroneousIndexAccess));

#undef HNDL

        SetGlobal(4u);
    }

public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = TOptimizeTransformerBase::DoTransform(input, output, ctx);

        if (status == TStatus::Ok) {
            for (const auto& hint: KqpCtx.GetOptimizerHints().GetUnappliedString()) {
                ctx.AddWarning(YqlIssue({}, TIssuesIds::YQL_UNUSED_HINT, "Unapplied hint: " + hint));
            }
        }

        return status;
    }

protected:
    TMaybeNode<TExprBase> TopSortSelectIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpTopSortSelectIndex(node, ctx, KqpCtx);
        DumpAppliedRule("KqpTopSortSelectIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpPushExtractedPredicateToReadTable(node, ctx, KqpCtx, TypesCtx, *getParents());
        DumpAppliedRule("PushExtractedPredicateToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushdownOlapGroupByKeys(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushDownOlapGroupByKeys(node, ctx, KqpCtx);
        DumpAppliedRule("PushdownOlapGroupByKeys", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> InspectErroneousIndexAccess(TExprBase node, TExprContext& ctx) {
        if (IsIn({"FulltextScore", "FulltextMatch"}, node.Ref().Content())) {
            auto message = TStringBuilder{} << "Failed to rewrite " << node.Ref().Content() << " callable";
            TIssue baseIssue{ctx.GetPosition(node.Pos()), message};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, baseIssue);

            TIssue subIssue{ctx.GetPosition(node.Pos()), "Fulltext index is not specified or unsupported predicate is used to access index"};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, subIssue);
            baseIssue.AddSubIssue(MakeIntrusive<TIssue>(std::move(subIssue)));
            ctx.AddError(baseIssue);
            return {};
        }

        DumpAppliedRule("InspectErroneousIndexAccess", node.Ptr(), node.Ptr(), ctx);
        return node;
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx) {
        TMaybeNode<TExprBase> output;
        auto aggregate = node.Cast<TCoAggregateBase>();
        auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (hopSetting) {
            auto input = aggregate.Input().Maybe<TDqConnection>();
            if (!input) {
                return node;
            }
            output = NHopping::RewriteAsHoppingWindow(
                node,
                ctx,
                input.Cast(),
                false,
                TDuration::MilliSeconds(TDqSettings::TDefault::WatermarksLateArrivalDelayMs),
                KqpCtx.Config->GetEnableWatermarks());
        } else {
            NDq::TSpillingSettings spillingSettings(KqpCtx.Config->GetEnabledSpillingNodes());
            output = DqRewriteAggregate(node, ctx, TypesCtx, false, KqpCtx.Config->HasOptEnableOlapPushdown() || KqpCtx.Config->HasOptUseFinalizeByKey(), KqpCtx.Config->HasOptUseFinalizeByKey(), spillingSettings.IsAggregationSpillingEnabled());
        }
        if (output) {
            DumpAppliedRule("RewriteAggregate", node.Ptr(), output.Cast().Ptr(), ctx);
        }
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

    TMaybeNode<TExprBase> RewriteStreamEquiJoinWithLookup(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqRewriteStreamEquiJoinWithLookup(node, ctx, TypesCtx);
        DumpAppliedRule("KqpRewriteStreamEquiJoinWithLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx) {
        TCBOSettings settings{
            .MaxDPhypDPTableSize = Config->MaxDPHypDPTableSize.Get().GetOrElse(TDqSettings::TDefault::MaxDPHypDPTableSize),
            .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
        };

        auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());
        bool enableShuffleElimination = KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(KqpCtx.Config->GetDefaultEnableShuffleElimination());
        auto providerCtx = TKqpProviderContext(KqpCtx, optLevel);
        auto stats = TypesCtx.GetStats(node.Raw());
        TTableAliasMap* tableAliases = stats? stats->TableAliases.Get(): nullptr;
        auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(providerCtx, settings, ctx, enableShuffleElimination, TypesCtx.OrderingsFSM, tableAliases));
        TExprBase output = DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, optLevel,
            *opt, [](auto& rels, auto label, auto node, auto stat) {
                rels.emplace_back(std::make_shared<TKqpRelOptimizerNode>(TString(label), *stat, node));
            },
            KqpCtx.EquiJoinsCount,
            KqpCtx.GetOptimizerHints(),
            enableShuffleElimination,
            &KqpCtx.ShufflingOrderingsByJoinLabels
        );
        DumpAppliedRule("OptimizeEquiJoinWithCosts", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        bool useCBO = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel()) >= 2;
        TExprBase output = DqRewriteEquiJoin(node, KqpCtx.Config->GetHashJoinMode(), useCBO, ctx, TypesCtx, KqpCtx.JoinsCount, KqpCtx.GetOptimizerHints());
        DumpAppliedRule("RewriteEquiJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> JoinToIndexLookup(TExprBase node, TExprContext& ctx) {
        bool useCBO = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel()) >= 2;
        TExprBase output = KqpJoinToIndexLookup(node, ctx, KqpCtx, useCBO, KqpCtx.GetOptimizerHints());
        DumpAppliedRule("JoinToIndexLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqExpandWindowFunctions(node, ctx, TypesCtx, true);
        DumpAppliedRule("ExpandWindowFunctions", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverFlatMap(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteTopSortOverFlatMap(node, ctx);
        DumpAppliedRule("RewriteTopSortOverFlatMap", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushLimitOverFullText(TExprBase node, TExprContext& ctx) {
        auto output = KqpPushLimitOverFullText(node, ctx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("PushLimitOverFullText", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverFullTextMatch(TExprBase node, TExprContext& ctx) {
        auto output = KqpRewriteFlatMapOverFullTextMatch(node, ctx, KqpCtx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("RewriteFlatMapOverFullTextMatch", node.Ptr(), output.Cast().Ptr(), ctx);
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

    TMaybeNode<TExprBase> UnorderedOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::UnorderedOverDqReadWrap(node, ctx, getParents, true, TypesCtx);
        if (output) {
            DumpAppliedRule("UnorderedOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::ExtractMembersOverDqReadWrap(node, ctx, getParents, true, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtractMembersOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> TakeOrSkipOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        auto output = NDq::TakeOrSkipOverDqReadWrap(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("TakeOrSkipOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtendOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        auto output = NDq::ExtendOverDqReadWrap(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtendOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> DqReadWideWrapFieldSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::DqReadWideWrapFieldSubset(node, ctx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("DqReadWideWrapFieldSubset", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> MatchRecognize(TExprBase node, TExprContext& ctx) {
        auto output = ExpandMatchRecognize(node.Ptr(), ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("MatchRecognize", node.Ptr(), output, ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> DqReadWrapByProvider(TExprBase node, TExprContext& ctx) {
        auto output = NDq::DqReadWrapByProvider(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("DqReadWrapByProvider", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto output = NDq::ExtractMembersOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtractMembersOverDqReadWrapMultiUsage", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> UnorderedOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto output = NDq::UnorderedOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("UnorderedOverDqReadWrapMultiUsage", node.Ptr(), output.Cast().Ptr(), ctx);
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
    TMaybeNode<TExprBase> ApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadOlapTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadOlapTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

private:
    TTypeAnnotationContext& TypesCtx;
    TKqpOptimizeContext& KqpCtx;
    const TKikimrConfiguration::TPtr& Config;
};

TAutoPtr<IGraphTransformer> CreateKqpLogOptTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typesCtx, const TKikimrConfiguration::TPtr& config)
{
    return THolder<IGraphTransformer>(new TKqpLogicalOptTransformer(typesCtx, kqpCtx, config));
}

} // namespace NKikimr::NKqp::NOpt
