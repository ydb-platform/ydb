#include "physical_optimize.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NDqs {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TDqsPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TDqsPhysicalOptProposalTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config)
        : TOptimizeTransformerBase(/* TODO: typeCtx*/nullptr, NLog::EComponent::ProviderDq, {})
        , Config(config)
    {
        const bool enablePrecompute = Config->_EnablePrecompute.Get().GetOrElse(false);
        const bool enableDqReplicate = Config->IsDqReplicateEnabled(*typeCtx);

#define HNDL(name) "DqsPhy-"#name, Hndl(&TDqsPhysicalOptProposalTransformer::name)
        if (!enableDqReplicate) {
            AddHandler(0, &TDqReplicate::Match, HNDL(FailOnDqReplicate));
        }
        AddHandler(0, &TDqSourceWrap::Match, HNDL(BuildStageWithSourceWrap));
        AddHandler(0, &TDqReadWrap::Match, HNDL(BuildStageWithReadWrap));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<false>));
        AddHandler(0, &TCoPruneKeys::Match, HNDL(PushPruneKeysToStage<false>));
        AddHandler(0, &TCoPruneAdjacentKeys::Match, HNDL(PushPruneAdjacentKeysToStage<false>));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<false>));
        AddHandler(0, &TCoAssumeUnique::Match, HNDL(PushAssumeUniqueToStage<false>));
        AddHandler(0, &TCoAssumeDistinct::Match, HNDL(PushAssumeDistinctToStage<false>));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<false>));
        AddHandler(0, &TCoCombineByKey::Match, HNDL(PushCombineToStage<false>));
        AddHandler(0, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage<false>));
        AddHandler(0, &TCoShuffleByKeys::Match, HNDL(BuildShuffleStage<false>));
        AddHandler(0, &TCoFinalizeByKey::Match, HNDL(BuildFinalizeByKeyStage<false>));
        AddHandler(0, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage<false>));
        AddHandler(0, &TCoAsList::Match, HNDL(BuildAggregationResultStage));
        AddHandler(0, &TCoTopSort::Match, HNDL(BuildTopSortStage<false>));
        AddHandler(0, &TCoTop::Match, HNDL(BuildTopStage<false>));
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
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(PushUnorderedToStage<false>));
        AddHandler(0, &TDqStage::Match, HNDL(UnorderedOverStageInput<false>));
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
        AddHandler(1, &TCoPruneKeys::Match, HNDL(PushPruneKeysToStage<true>));
        AddHandler(1, &TCoPruneAdjacentKeys::Match, HNDL(PushPruneAdjacentKeysToStage<true>));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<true>));
        AddHandler(1, &TCoAssumeUnique::Match, HNDL(PushAssumeUniqueToStage<true>));
        AddHandler(1, &TCoAssumeDistinct::Match, HNDL(PushAssumeDistinctToStage<true>));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<true>));
        AddHandler(1, &TCoCombineByKey::Match, HNDL(PushCombineToStage<true>));
        AddHandler(1, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage<true>));
        AddHandler(1, &TCoShuffleByKeys::Match, HNDL(BuildShuffleStage<true>));
        AddHandler(1, &TCoFinalizeByKey::Match, HNDL(BuildFinalizeByKeyStage<true>));
        AddHandler(1, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage<true>));
        AddHandler(1, &TCoTopSort::Match, HNDL(BuildTopSortStage<true>));
        AddHandler(1, &TCoTop::Match, HNDL(BuildTopStage<true>));
        AddHandler(1, &TCoSort::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoTakeBase::Match, HNDL(BuildTakeOrTakeSkipStage<true>));
        AddHandler(1, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(RewriteLeftPureJoin<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(BuildJoin<true>));
        AddHandler(1, &TCoAssumeSorted::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<true>));
        AddHandler(1, &TCoLMap::Match, HNDL(PushLMapToStage<true>));
        AddHandler(1, &TCoUnorderedBase::Match, HNDL(PushUnorderedToStage<true>));
        AddHandler(1, &TDqStage::Match, HNDL(UnorderedOverStageInput<true>));
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
    TMaybeNode<TExprBase> FailOnDqReplicate(TExprBase node, TExprContext& ctx) {
        ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, "Reading multiple times from the same source is not supported"));
        return {};
    }

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
    TMaybeNode<TExprBase> PushPruneKeysToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushPruneKeysToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushPruneAdjacentKeysToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushPruneAdjacentKeysToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushExtractMembersToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushExtractMembersToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushAssumeDistinctToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushAssumeDistinctToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushAssumeUniqueToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushAssumeUniqueToStage(node, ctx, optCtx, *getParents(), IsGlobal);
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
    TMaybeNode<TExprBase> PushUnorderedToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushUnorderedToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> UnorderedOverStageInput(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqUnorderedOverStageInput(node, ctx, optCtx, *GetTypes(), *getParents(), IsGlobal);
    }


    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushCombineToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushCombineToStage(node, ctx, optCtx, *getParents(), IsGlobal);
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

    TMaybeNode<TExprBase> BuildAggregationResultStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        return DqBuildAggregationResultStage(node, ctx, optCtx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTopSortStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildTopSortStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildTopStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildTopStage(node, ctx, optCtx, *getParents(), IsGlobal);
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

    bool ValidateStreamLookupJoinFlags(const TDqJoin& join, TExprContext& ctx) {
        bool leftAny = false;
        bool rightAny = false;
        if (const auto maybeFlags = join.Flags()) {
            for (auto&& flag: maybeFlags.Cast()) {
                auto&& name = flag.StringValue();
                if (name == "LeftAny"sv) {
                    leftAny = true;
                    continue;
                } else if (name == "RightAny"sv) {
                    rightAny = true;
                    continue;
                }
            }
            if (leftAny) {
                ctx.AddError(TIssue(ctx.GetPosition(maybeFlags.Cast().Pos()), "Streamlookup ANY LEFT join is not implemented"));
                return false;
            }
        }
        if (!rightAny) {
            if (false) { // Tempoarily change to waring to allow for smooth transition
                ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), "Streamlookup: must be LEFT JOIN /*+streamlookup(...)*/ ANY"));
                return false;
            } else {
                ctx.AddWarning(TIssue(ctx.GetPosition(join.Pos()), "(Deprecation) Streamlookup: must be LEFT JOIN /*+streamlookup(...)*/ ANY"));
            }
        }
        return true;
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

        if (!ValidateStreamLookupJoinFlags(join, ctx)) {
            return {};
        }

        TExprNode::TPtr ttl;
        TExprNode::TPtr maxCachedRows;
        TExprNode::TPtr maxDelayedRows;
        TExprNode::TPtr isMultiget;
        if (const auto maybeOptions = join.JoinAlgoOptions()) {
            for (auto&& option: maybeOptions.Cast()) {
                auto&& name = option.Name().Value();
                if (name == "TTL"sv) {
                    ttl = option.Value().Cast().Ptr();
                } else if (name == "MaxCachedRows"sv) {
                    maxCachedRows = option.Value().Cast().Ptr();
                } else if (name == "MaxDelayedRows"sv) {
                    maxDelayedRows = option.Value().Cast().Ptr();
                } else if (name == "MultiGet"sv) {
                    isMultiget = option.Value().Cast().Ptr();
                }
            }
        }

        if (!ttl) {
            ttl = ctx.NewAtom(pos, 300);
        }
        if (!maxCachedRows) {
            maxCachedRows = ctx.NewAtom(pos, 1'000'000);
        }
        if (!maxDelayedRows) {
            maxDelayedRows = ctx.NewAtom(pos, 1'000'000);
        }
        auto rightInput = join.RightInput().Ptr();
        if (auto maybe = TExprBase(rightInput).Maybe<TCoExtractMembers>()) {
            rightInput = maybe.Cast().Input().Ptr();
        }
        auto leftLabel = join.LeftLabel().Maybe<NNodes::TCoAtom>() ? join.LeftLabel().Cast<NNodes::TCoAtom>().Ptr() : ctx.NewAtom(pos, "");
        Y_ENSURE(join.RightLabel().Maybe<NNodes::TCoAtom>());
        auto cn = Build<TDqCnStreamLookup>(ctx, pos)
            .Output(left.Output().Cast())
            .LeftLabel(leftLabel)
            .RightInput(rightInput)
            .RightLabel(join.RightLabel().Cast<NNodes::TCoAtom>())
            .JoinKeys(join.JoinKeys())
            .JoinType(join.JoinType())
            .LeftJoinKeyNames(join.LeftJoinKeyNames())
            .RightJoinKeyNames(join.RightJoinKeyNames())
            .TTL(ttl)
            .MaxCachedRows(maxCachedRows)
            .MaxDelayedRows(maxDelayedRows);

        if (isMultiget) {
            cn.IsMultiget(isMultiget);
        }

        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body("stream")
            .Done();
        const auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(cn.Done())
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
