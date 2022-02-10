#include "physical_optimize.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDqs {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

class TDqsPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TDqsPhysicalOptProposalTransformer(TTypeAnnotationContext* typeCtx)
        : TOptimizeTransformerBase(typeCtx, NLog::EComponent::ProviderDq, {})
    {
#define HNDL(name) "DqsPhy-"#name, Hndl(&TDqsPhysicalOptProposalTransformer::name)
        AddHandler(0, &TDqSourceWrap::Match, HNDL(BuildStageWithSourceWrap));
        AddHandler(0, &TDqReadWrap::Match, HNDL(BuildStageWithReadWrap));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<false>));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<false>));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<false>));
        AddHandler(0, &TCoCombineByKey::Match, HNDL(PushCombineToStage<false>));
        AddHandler(0, &TCoPartitionsByKeys::Match, HNDL(BuildPartitionsStage));
        AddHandler(0, &TCoPartitionByKey::Match, HNDL(BuildPartitionStage));
        AddHandler(0, &TCoAsList::Match, HNDL(BuildAggregationResultStage));
        AddHandler(0, &TCoTopSort::Match, HNDL(BuildTopSortStage<false>));
        AddHandler(0, &TCoSort::Match, HNDL(BuildSortStage<false>));
        AddHandler(0, &TCoTake::Match, HNDL(BuildTakeOrTakeSkipStage<false>));
        AddHandler(0, &TCoLength::Match, HNDL(RewriteLengthOfStageOutput));
        AddHandler(0, &TCoExtendBase::Match, HNDL(BuildExtendStage));
        AddHandler(0, &TDqJoin::Match, HNDL(RewriteRightJoinToLeft));
        AddHandler(0, &TDqJoin::Match, HNDL(PushJoinToStage<false>));
        AddHandler(0, &TDqJoin::Match, HNDL(BuildJoin<false>));
        AddHandler(0, &TDqJoin::Match, HNDL(BuildJoinDict<false>));
        AddHandler(0, &TCoAssumeSorted::Match, HNDL(BuildSortStage<false>));
        AddHandler(0, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<false>));
        AddHandler(0, &TCoLMap::Match, HNDL(PushLMapToStage<false>));
        // (Apply (SqlExternalFunction ..) ..) to stage
        AddHandler(0, &TCoApply::Match, HNDL(BuildExtFunctionStage<false>));
#if 0
        AddHandler(0, &TCoHasItems::Match, HNDL(BuildHasItems));
        AddHandler(0, &TCoToOptional::Match, HNDL(BuildScalarPrecompute));
#endif

        AddHandler(1, &TCoSkipNullMembers::Match, HNDL(PushSkipNullMembersToStage<true>));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(PushExtractMembersToStage<true>));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(BuildFlatmapStage<true>));
        AddHandler(1, &TCoCombineByKey::Match, HNDL(PushCombineToStage<true>));
        AddHandler(1, &TCoTopSort::Match, HNDL(BuildTopSortStage<true>));
        AddHandler(1, &TCoSort::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoTake::Match, HNDL(BuildTakeOrTakeSkipStage<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(PushJoinToStage<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(BuildJoin<true>));
        AddHandler(1, &TDqJoin::Match, HNDL(BuildJoinDict<true>));
        AddHandler(1, &TCoAssumeSorted::Match, HNDL(BuildSortStage<true>));
        AddHandler(1, &TCoOrderedLMap::Match, HNDL(PushOrderedLMapToStage<true>));
        AddHandler(1, &TCoLMap::Match, HNDL(PushLMapToStage<true>));
        AddHandler(1, &TCoApply::Match, HNDL(BuildExtFunctionStage<true>));
#undef HNDL

        SetGlobal(1u);
    }

protected:
    TMaybeNode<TExprBase> BuildStageWithSourceWrap(TExprBase node, TExprContext& ctx) {
        const auto wrap = node.Cast<TDqSourceWrap>();
        const auto& items = GetSeqItemType(wrap.Ref().GetTypeAnn())->Cast<TStructExprType>()->GetItems();
        auto narrow = wrap.Settings() ?
            ctx.Builder(node.Pos())
            .Lambda()
                .Param("source")
                .Callable("NarrowMap")
                    .Callable(0, TDqSourceWideWrap::CallableName())
                        .Arg(0, "source")
                        .Add(1, wrap.DataSource().Ptr())
                        .Add(2, wrap.RowType().Ptr())
                        .Add(3, wrap.Settings().Cast().Ptr())
                    .Seal()
                    .Lambda(1)
                        .Params("fields", items.size())
                        .Callable(TCoAsStruct::CallableName())
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                ui32 i = 0U;
                                for (const auto& item : items) {
                                    parent.List(i)
                                        .Atom(0, item->GetName())
                                        .Arg(1, "fields", i)
                                    .Seal();
                                    ++i;
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build():
            ctx.Builder(node.Pos())
            .Lambda()
                .Param("source")
                .Callable("NarrowMap")
                    .Callable(0, TDqSourceWideWrap::CallableName())
                        .Arg(0, "source")
                        .Add(1, wrap.DataSource().Ptr())
                        .Add(2, wrap.RowType().Ptr())
                    .Seal()
                    .Lambda(1)
                        .Params("fields", items.size())
                        .Callable(TCoAsStruct::CallableName())
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                ui32 i = 0U;
                                for (const auto& item : items) {
                                    parent.List(i)
                                        .Atom(0, item->GetName())
                                        .Arg(1, "fields", i)
                                    .Seal();
                                    ++i;
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();

        return Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage<TDqStage>()
                .Inputs()
                    .Add<TDqSource>()
                        .DataSource(wrap.DataSource())
                        .Settings(wrap.Input())
                        .Build()
                    .Build()
                .Program(narrow)
                    .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                .Build()
                .Index().Build("0")
            .Build().Done();
    }

    TMaybeNode<TExprBase> BuildStageWithReadWrap(TExprBase node, TExprContext& ctx) {
        const auto wrap = node.Cast<TDqReadWrap>();
        const auto read = Build<TDqReadWideWrap>(ctx, node.Pos())
                .Input(wrap.Input())
                .Token(wrap.Token())
            .Done();

        const auto structType = GetSeqItemType(wrap.Ref().GetTypeAnn())->Cast<TStructExprType>();
        auto narrow = ctx.Builder(node.Pos())
            .Lambda()
                .Callable("NarrowMap")
                    .Add(0, read.Ptr())
                    .Lambda(1)
                        .Params("fields", structType->GetSize())
                        .Callable("AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                ui32 i = 0U;
                                for (const auto& item : structType->GetItems()) {
                                    parent.List(i)
                                        .Atom(0, item->GetName())
                                        .Arg(1, "fields", i)
                                    .Seal();
                                    ++i;
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();

        return Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs().Build()
                    .Program(narrow)
                    .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                .Build()
                .Index().Build("0")
            .Build() .Done();
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
 
    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildExtFunctionStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqBuildExtFunctionStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushCombineToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushCombineToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    TMaybeNode<TExprBase> BuildPartitionsStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return DqBuildPartitionsStage(node, ctx, *getParents());
    }

    TMaybeNode<TExprBase> BuildPartitionStage(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return DqBuildPartitionStage(node, ctx, *getParents());
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

    TMaybeNode<TExprBase> RewriteLengthOfStageOutput(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        return DqRewriteLengthOfStageOutput(node, ctx, optCtx);
    }

    TMaybeNode<TExprBase> BuildExtendStage(TExprBase node, TExprContext& ctx) {
        return DqBuildExtendStage(node, ctx);
    }

    TMaybeNode<TExprBase> RewriteRightJoinToLeft(TExprBase node, TExprContext& ctx) {
        return DqRewriteRightJoinToLeft(node, ctx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> PushJoinToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return DqPushJoinToStage(node, ctx, optCtx, *getParents(), IsGlobal);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildJoin(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto join = node.Cast<TDqJoin>();
        const TParentsMap* parentsMap = getParents();
        if (!JoinPrerequisitesVerify(join, parentsMap, IsGlobal)) {
            return node;
        }

        return DqBuildPhyJoin(join, false /* TODO */, ctx, optCtx);
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> BuildJoinDict(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        Y_UNUSED(optCtx);

        auto join = node.Cast<TDqJoin>();
        const TParentsMap* parentsMap = getParents();
        if (!JoinPrerequisitesVerify(join, parentsMap, IsGlobal)) {
            return node;
        }

        return DqBuildJoinDict(join, ctx); // , optCtx);
    }

    TMaybeNode<TExprBase> BuildHasItems(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        return DqBuildHasItems(node, ctx, optCtx);
    }

    TMaybeNode<TExprBase> BuildScalarPrecompute(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
        return DqBuildScalarPrecompute(node, ctx, optCtx);
    }

private:
    bool JoinPrerequisitesVerify(TDqJoin join, const TParentsMap* parentsMap, bool isGlobal) const {
        // KqpBuildJoin copy/paste
        if (!join.LeftInput().Maybe<TDqCnUnionAll>()) {
            return false;
        }
        if (!join.RightInput().Maybe<TDqCnUnionAll>()) {
            return false;
        }

        if (!IsSingleConsumerConnection(join.LeftInput().Cast<TDqCnUnionAll>(), *parentsMap, isGlobal)) {
            return false;
        }
        if (!IsSingleConsumerConnection(join.RightInput().Cast<TDqCnUnionAll>(), *parentsMap, isGlobal)) {
            return false;
        }
        return true;
    }
};

THolder<IGraphTransformer> CreateDqsPhyOptTransformer(TTypeAnnotationContext* typeCtx) {
    return THolder(new TDqsPhysicalOptProposalTransformer(typeCtx));
}

} // NYql::NDqs
