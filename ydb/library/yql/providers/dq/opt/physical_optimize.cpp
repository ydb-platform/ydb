#include "physical_optimize.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDqs {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TDqsPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TDqsPhysicalOptProposalTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config)
        : TOptimizeTransformerBase(typeCtx, NLog::EComponent::ProviderDq, {})
        , Config(config)
    {
        const bool enablePrecompute = Config->_EnablePrecompute.Get().GetOrElse(false);

#define HNDL(name) "DqsPhy-"#name, Hndl(&TDqsPhysicalOptProposalTransformer::name)
        AddHandler(0, &TDqSourceWrap::Match, HNDL(BuildStageWithSourceWrap));
        AddHandler(0, &TDqReadWrap::Match, HNDL(BuildStageWithReadWrap));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<false>));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<false>));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<false>));
        AddHandler(0, &TCoCombineByKey::Match, HNDL(PushCombineToStage<false>));
        AddHandler(0, &TCoCombineByKeyWithSpilling::Match, HNDL(PushCombineWithSpillingToStage<false>));
        AddHandler(0, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage<false>));
        AddHandler(0, &TCoShuffleByKeys::Match, HNDL(BuildShuffleStage<false>));
        AddHandler(0, &TCoFinalizeByKey::Match, HNDL(BuildFinalizeByKeyStage<false>));
        AddHandler(0, &TCoFinalizeByKeyWithSpilling::Match, HNDL(BuildFinalizeByKeyWithSpillingStage<false>));
        AddHandler(0, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage<false>));
        AddHandler(0, &TCoAsList::Match, HNDL(BuildAggregationResultStage));
        AddHandler(0, &TCoTopSort::Match, HNDL(BuildTopSortStage<false>));
        AddHandler(0, &TCoSort::Match, HNDL(BuildSortStage<false>));
        AddHandler(0, &TCoTakeBase::Match, HNDL(BuildTakeOrTakeSkipStage<false>));
        AddHandler(0, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput<false>));
        AddHandler(0, &TCoExtendBase::Match, HNDL(BuildExtendStage));
        AddHandler(0, &TDqJoin::Match, HNDL(SuppressSortOnJoinInput));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteRightJoinToLeft));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteLeftPureJoin<false>));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteStreamLookupJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(BuildJoin<false>));
        AddHandler(0, &TCoAssumeSorted::Match, HNDL(BuildSortStage<false>));
        AddHandler(0, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<false>));
        AddHandler(0, &TCoLMap::Match, HNDL(PushLMapToStage<false>));
        AddHandler(0, &TCoOrderedLMap::Match, HNDL(BuildOrderedLMapOverMuxStage));
        AddHandler(0, &TCoLMap::Match, HNDL(BuildLMapOverMuxStage));
        if (enablePrecompute) {
            AddHandler(0, &TCoHasItems::Match, HNDL(BuildHasItems<false>));
            AddHandler(0, &TCoSqlIn::Match, HNDL(BuildSqlIn<false>));
            AddHandler(0, &TCoToOptional::Match, HNDL(BuildScalarPrecompute<false>));
            AddHandler(0, &TCoHead::Match, HNDL(BuildScalarPrecompute<false>));
            AddHandler(0, &TDqPrecompute::Match, HNDL(BuildPrecompute));
            AddHandler(0, &TDqStage::Match, HNDL(PrecomputeToInput));
            AddHandler(0, &TCoTake::Match, HNDL(PropagatePrecomuteTake<false>));
            AddHandler(0, &TCoFlatMap::Match, HNDL(PropagatePrecomuteFlatmap<false>));
        }

        AddHandler(1, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<true>));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<true>));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<true>));
        AddHandler(1, &TCoCombineByKey::Match, HNDL(PushCombineToStage<true>));
        AddHandler(1, &TCoCombineByKeyWithSpilling::Match, HNDL(PushCombineWithSpillingToStage<true>));
        AddHandler(1, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage<true>));
        AddHandler(1, &TCoShuffleByKeys::Match, HNDL(BuildShuffleStage<true>));
        AddHandler(1, &TCoFinalizeByKey::Match, HNDL(BuildFinalizeByKeyStage<true>));
        AddHandler(1, &TCoFinalizeByKeyWithSpilling::Match, HNDL(BuildFinalizeByKeyWithSpillingStage<true>));
        AddHandler(1, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage<true>));
        AddHandler(1, &TCoTopSort::Match, HNDL(BuildTopSortStage<true>));
        AddHandler(1, &TCoSort::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoTakeBase::Match, HNDL(BuildTakeOrTakeSkipStage<true>));
        AddHandler(1, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(RewriteLeftPureJoin<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(BuildJoin<true>));
        AddHandler(1, &TCoAssumeSorted::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<true>));
        AddHandler(1, &TCoLMap::Match, HNDL(PushLMapToStage<true>));
        if (enablePrecompute) {
            AddHandler(1, &TCoHasItems::Match, HNDL(BuildHasItems<true>));
            AddHandler(1, &TCoSqlIn::Match, HNDL(BuildSqlIn<true>));
            AddHandler(1, &TCoToOptional::Match, HNDL(BuildScalarPrecompute<true>));
            AddHandler(1, &TCoHead::Match, HNDL(BuildScalarPrecompute<true>));
            AddHandler(1, &TCoTake::Match, HNDL(PropagatePrecomuteTake<true>));
            AddHandler(1, &TCoFlatMap::Match, HNDL(PropagatePrecomuteFlatmap<true>));
        }
#undef HNDL

        SetGlobal(1u);
    }

protected:
    TMaybeNode<TExprBase> BuildStageWithSourceWrap(TExprBase node, TExprContext& ctx) {
        return DqBuildStageWithSourceWrap(node, ctx);
    }

    TMaybeNode<TExprBase> BuildStageWithReadWrap(TExprBase node, TExprContext& ctx) {
        return DqBuildStageWithReadWrap(node, ctx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushSkipNullMembersToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushSkipNullMembersToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushExtractMembersToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushExtractMembersToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildFlatmapStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildFlatmapStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushOrderedLMapToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushOrderedLMapToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushLMapToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushLMapToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> BuildOrderedLMapOverMuxStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildOrderedLMapOverMuxStage(node, ctx, optCtx, *getParents());
    }

    TMaybeNode<TExprBase> BuildLMapOverMuxStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildLMapOverMuxStage(node, ctx, optCtx, *getParents());
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushCombineToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushCombineToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushCombineWithSpillingToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushCombineWithSpillingToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildPartitionsStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildPartitionsStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildPartitionStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildPartitionStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildShuffleStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildShuffleStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template<bool IsGlobal>
    TMaybeNode<TExprBase> BuildFinalizeByKeyStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return DqBuildFinalizeByKeyStage(node, ctx, *getParents(), IsGlobal);
    }

    template<bool IsGlobal>
    TMaybeNode<TExprBase> BuildFinalizeByKeyWithSpillingStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return DqBuildFinalizeByKeyWithSpillingStage(node, ctx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> BuildAggregationResultStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        return DqBuildAggregationResultStage(node, ctx, optCtx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTopSortStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildTopSortStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildSortStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildSortStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTakeOrTakeSkipStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        if (node.Maybe<TCoTake>().Input().Maybe<TCoSkip>()) {
            return DqBuildTakeSkipStage(node, ctx, optCtx, *getParents(), IsGlobal);
        } else {
            return DqBuildTakeStage(node, ctx, optCtx, *getParents(), IsGlobal);
        }
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> RewriteLengthOfStageOutput(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqRewriteLengthOfStageOutput(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> BuildExtendStage(TExprBase node, TExprContext& ctx) {
        return DqBuildExtendStage(node, ctx);
    }

    TMaybeNode<TExprBase> RewriteRightJoinToLeft(TExprBase node, TExprContext& ctx) {
        return DqRewriteRightJoinToLeft(node, ctx);
    }

    TMaybeNode<TExprBase> SuppressSortOnJoinInput(TExprBase node, TExprContext& ctx) {
        return DqSuppressSortOnJoinInput(node.Cast<TDqJoin>(),ctx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> RewriteLeftPureJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return DqRewriteLeftPureJoin(node, ctx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> RewriteStreamLookupJoin(TExprBase node, TExprContext& ctx) {
        const auto join = node.Cast<TDqJoin>();
        if (join.JoinAlgo().StringValue() != "StreamLookupJoin") {
            return node;
        }

        const auto pos = node.Pos();
        const auto left = join.LeftInput().Maybe<TDqConnection>();
        if (!left) {
            return node;
        }
        auto cn = Build<TDqCnStreamLookup>(ctx, pos)
            .Output(left.Output().Cast())
            .LeftLabel(join.LeftLabel().Cast<NNodes::TCoAtom>())
            .RightInput(join.RightInput())
            .RightLabel(join.RightLabel().Cast<NNodes::TCoAtom>())
            .JoinKeys(join.JoinKeys())
            .JoinType(join.JoinType())
            .LeftJoinKeyNames(join.LeftJoinKeyNames())
            .RightJoinKeyNames(join.RightJoinKeyNames())
            .TTL(ctx.NewAtom(pos, 300)) //TODO configure me
            .MaxCachedRows(ctx.NewAtom(pos, 1'000'000)) //TODO configure me
            .MaxDelay(ctx.NewAtom(pos, 1'000'000)) //Configure me
        .Done();

        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body("stream")
            .Done();
        const auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(cn)
                .Build()
            .Program(lambda)
            .Settings(TDqStageSettings().BuildNode(ctx, pos))
            .Done();

        return Build<TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Done();
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildJoin(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        const auto join = node.Cast<TDqJoin>();
        const TParentsMap* parentsMap = getParents();
        const auto mode = Config->HashJoinMode.Get().GetOrElse(EHashJoinMode::Off);
        const auto useGraceJoin = Config->UseGraceJoinCoreForMap.Get().GetOrElse(false);
        return DqBuildJoin(join, ctx, optCtx, *parentsMap, IsGlobal, /* pushLeftStage = */ false /* TODO */, mode, true, useGraceJoin);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildHasItems(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildHasItems(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildSqlIn(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildSqlIn(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildScalarPrecompute(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildScalarPrecompute(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> BuildPrecompute(TExprBase node, TExprContext& ctx) {
        return DqBuildPrecompute(node, ctx);
    }

    TMaybeNode<TExprBase> PrecomputeToInput(TExprBase node, TExprContext& ctx) {
        return DqPrecomputeToInput(node, ctx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PropagatePrecomuteTake(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        return DqPropagatePrecomuteTake(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PropagatePrecomuteFlatmap(TExprBase node, TExprContext& ctx,
        IOptimizationContext& optCtx, const TGetParents& getParents)
    {
        return DqPropagatePrecomuteFlatmap(node, ctx, optCtx, *getParents(), IsGlobal);
    }

private:
    TDqConfiguration::TPtr Config;
};

THolder<IGraphTransformer> CreateDqsPhyOptTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config) {
    return THolder(new TDqsPhysicalOptProposalTransformer(typeCtx, config));
}

} // NYql::NDqs
