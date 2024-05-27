#pragma once

#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_table.h"
#include "yql_yt_join_impl.h"
#include "yql_yt_optimize.h"
#include "yql_yt_provider_impl.h"
#include "yql_yt_transformer_helper.h"

#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

class TYtPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYtPhysicalOptProposalTransformer(TYtState::TPtr state);

private:
    static bool CanBePulledIntoParentEquiJoin(const TCoFlatMapBase& flatMap, const TGetParents& getParents);

    TCoLambda MakeJobLambdaNoArg(TExprBase content, TExprContext& ctx) const;

    TMaybeNode<TExprBase> Mux(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> YtSortOverAlreadySorted(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> PartitionByKey(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> FlatMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> CombineByKey(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> DqWrite(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> YtDqProcessWrite(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> Write(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> Fill(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> TakeOrSkip(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> Extend(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> Length(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> ResPull(TExprBase node, TExprContext& ctx) const;

    struct TRangeBuildResult {
        TVector<TString> Keys;
        TSet<size_t> TableIndexes;
        IPredicateRangeExtractor::TBuildResult BuildResult;
    };

    TMaybe<TVector<TRangeBuildResult>> ExtractKeyRangeFromLambda(TCoLambda lambda, TYtSection section, TExprContext& ctx) const;

    TExprNode::TPtr UpdateSectionWithKeyRanges(TPositionHandle pos, TYtSection section, const TVector<TRangeBuildResult>& results, TExprContext& ctx) const;

    TMaybeNode<TExprBase> ExtractKeyRangeDqReadWrap(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> ExtractKeyRange(TExprBase node, TExprContext& ctx) const;

    // All keyFilter settings are combined by OR.
    // keyFilter value := '(<memberItem>+) <optional tableIndex>
    // <memberItem> := '(<memberName> '(<cmpItem>+))
    // <cmpItem> := '(<cmpOp> <value>)
    TMaybeNode<TExprBase> ExtractKeyRangeLegacy(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> FuseReduce(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> FuseInnerMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> FuseOuterMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;
    TMaybeNode<TExprBase> AssumeSorted(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> LambdaFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> MapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> ReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> LambdaVisitFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> MultiMapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> MultiReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> WeakFields(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> EquiJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> EarlyMergeJoin(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> RuntimeEquiJoin(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> TableContentWithSettings(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> NonOptimalTableContent(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> ReadWithSettings(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> TransientOpWithSettings(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> ReplaceStatWriteTable(TExprBase node, TExprContext& ctx) const;

    template<bool IsTop>
    TMaybeNode<TExprBase> Sort(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> TopSort(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> EmbedLimit(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> PushMergeLimitToInput(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> PushDownKeyExtract(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> BypassMerge(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> BypassMergeBeforePublish(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> MapToMerge(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> UnorderedPublishTarget(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> AddTrivialMapperForNativeYtTypes(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> YtDqWrite(TExprBase node, TExprContext& ctx) const;

    TMaybeNode<TExprBase> PushDownYtMapOverSortedMerge(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const;

    TMaybeNode<TExprBase> MergeToCopy(TExprBase node, TExprContext& ctx) const;

    template <typename TLMapType>
    TMaybeNode<TExprBase> LMap(TExprBase node, TExprContext& ctx) const;

    template<bool WithList>
    TCoLambda MakeJobLambda(TCoLambda lambda, bool useFlow, TExprContext& ctx) const;

    template <class TExpr>
    TMaybeNode<TExpr> CleanupWorld(TExpr node, TExprContext& ctx) const {
        return TMaybeNode<TExpr>(YtCleanupWorld(node.Ptr(), ctx, State_));
    }

    TMaybe<bool> CanFuseLambdas(const TCoLambda& innerLambda, const TCoLambda& outerLambda, TExprContext& ctx) const;


private:
    const TYtState::TPtr State_;
};

}  // namespace NYql 
