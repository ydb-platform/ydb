#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/string.h>

namespace NYql {

class TYtPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYtPhysicalOptProposalTransformer(TYtState::TPtr state);

private:
    static bool CanBePulledIntoParentEquiJoin(const NNodes::TCoFlatMapBase& flatMap, const TGetParents& getParents);

    NNodes::TCoLambda MakeJobLambdaNoArg(NNodes::TExprBase content, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> Mux(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> YtSortOverAlreadySorted(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> PartitionByKey(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FlatMap(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> CombineByKey(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> DqWrite(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> YtDqProcessWrite(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> Write(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> Fill(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> TakeOrSkip(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> Extend(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> Length(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ResPull(NNodes::TExprBase node, TExprContext& ctx) const;

    struct TRangeBuildResult {
        TVector<TString> Keys;
        TSet<size_t> TableIndexes;
        IPredicateRangeExtractor::TBuildResult BuildResult;
    };

    TMaybe<TVector<TRangeBuildResult>> ExtractKeyRangeFromLambda(NNodes::TCoLambda lambda, NNodes::TYtSection section, TExprContext& ctx) const;

    TExprNode::TPtr UpdateSectionWithKeyRanges(TPositionHandle pos, NNodes::TYtSection section, const TVector<TRangeBuildResult>& results, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ExtractKeyRangeDqReadWrap(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ExtractKeyRange(NNodes::TExprBase node, TExprContext& ctx) const;

    // All keyFilter settings are combined by OR.
    // keyFilter value := '(<memberItem>+) <optional tableIndex>
    // <memberItem> := '(<memberName> '(<cmpItem>+))
    // <cmpItem> := '(<cmpOp> <value>)
    NNodes::TMaybeNode<NNodes::TExprBase> ExtractKeyRangeLegacy(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FuseReduce(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FuseInnerMap(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> FuseOuterMap(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;
    NNodes::TMaybeNode<NNodes::TExprBase> AssumeSorted(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> LambdaFieldsSubset(NNodes::TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> MapFieldsSubset(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ReduceFieldsSubset(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> LambdaVisitFieldsSubset(NNodes::TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> MultiMapFieldsSubset(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> MultiReduceFieldsSubset(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> WeakFields(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> EquiJoin(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> EarlyMergeJoin(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> RuntimeEquiJoin(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> TableContentWithSettings(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> NonOptimalTableContent(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ReadWithSettings(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> TransientOpWithSettings(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> ReplaceStatWriteTable(NNodes::TExprBase node, TExprContext& ctx) const;

    template<bool IsTop>
    NNodes::TMaybeNode<NNodes::TExprBase> Sort(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> TopSort(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> EmbedLimit(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> PushMergeLimitToInput(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> PushDownKeyExtract(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> BypassMerge(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> BypassMergeBeforePublish(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> MapToMerge(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> UnorderedPublishTarget(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> AddTrivialMapperForNativeYtTypes(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> YtDqWrite(NNodes::TExprBase node, TExprContext& ctx) const;

    NNodes::TMaybeNode<NNodes::TExprBase> PushDownYtMapOverSortedMerge(NNodes::TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    NNodes::TMaybeNode<NNodes::TExprBase> MergeToCopy(NNodes::TExprBase node, TExprContext& ctx) const;

    template <typename TLMapType>
    NNodes::TMaybeNode<NNodes::TExprBase> LMap(NNodes::TExprBase node, TExprContext& ctx) const;

    template<bool WithList>
    NNodes::TCoLambda MakeJobLambda(NNodes::TCoLambda lambda, bool useFlow, TExprContext& ctx) const;

    template <class TExpr>
    NNodes::TMaybeNode<TExpr> CleanupWorld(TExpr node, TExprContext& ctx) const {
        return NNodes::TMaybeNode<TExpr>(YtCleanupWorld(node.Ptr(), ctx, State_));
    }

    TMaybe<bool> CanFuseLambdas(const NNodes::TCoLambda& innerLambda, const NNodes::TCoLambda& outerLambda, TExprContext& ctx) const;


private:
    const TYtState::TPtr State_;
};

}  // namespace NYql
