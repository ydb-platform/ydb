#pragma once

#include "dq_opt.h"

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {
enum class EInequalityPredicateType : ui8 { Less, LessOrEqual, Greater, GreaterOrEqual, Equal, NotEqual };

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, TExprContext& ctx, const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForExtendBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForCombiner(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqMerge(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TOptimizerHints hints = {}, TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr);
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqJoinBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqPhyCrossJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAsList(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAsStruct(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForTopBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSortBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForEquiJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForUnionAll(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);

template <typename TAggregationCallable>
void InferStatisticsForAggregationCallable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
extern template void InferStatisticsForAggregationCallable<NNodes::TCoShuffleByKeys>(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);


std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats);
std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);
std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats);
std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);

std::shared_ptr<TOptimizerStatistics> RemoveOrderings(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);

struct TOrderingInfo {
    std::int64_t OrderingIdx = -1;
    std::vector<TOrdering::TItem::EDirection> Directions{};
    TVector<TJoinColumn> Ordering{};
};

enum class ELogicalOperator : ui8 { And, Or, Leaf };

struct TPredicateRange {
    TMaybe<NNodes::TExprBase> Left;
    TMaybe<NNodes::TExprBase> Right;
    TMaybe<std::pair<TMaybe<TString>, TMaybe<TString>>> RangeBound;
    TMaybe<EInequalityPredicateType> CompareSign;
};

struct TTreeNode {
    ELogicalOperator Operator;

    // For And / Or
    TVector<std::shared_ptr<TTreeNode>> Children;

    // For Leaf
    TString Column;
    TString TableAlias;
    double Selectivity = 0.0;
    bool CollectMembers = false;
    TMaybe<TVector<TPredicateRange>> AllRanges;
};

TOrderingInfo GetTopBaseSortingOrderingInfo(const NNodes::TCoTopBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetSortBaseSortingOrderingInfo(const NNodes::TCoSortBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetAggregationBaseShuffleOrderingInfo(const NNodes::TCoAggregateBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& shufflingsFSM, TTableAliasMap*);
TVector<TJoinColumn> GetKeySelectorOrdering(const NNodes::TCoLambda& keySelector);

class TPredicateSelectivityComputer {
public:
    struct TColumnStatisticsUsedMembers {
        struct TColumnStatisticsUsedMember {
            enum _ : ui32 {
                EEquality,
                EInequality
            };

            TColumnStatisticsUsedMember(NNodes::TCoMember member, ui32 predicateType)
                : Member(std::move(member))
                , PredicateType(predicateType)
            {}

            NNodes::TCoMember Member;
            ui32 PredicateType;
        };

        void AddEquality(const NNodes::TCoMember& member) {
            Data.emplace_back(std::move(member), TColumnStatisticsUsedMember::EEquality);
        }

        void AddInequality(const NNodes::TCoMember& member) {
            Data.emplace_back(std::move(member), TColumnStatisticsUsedMember::EInequality);
        }

        TVector<TColumnStatisticsUsedMember> Data{};
    };

public:
    TPredicateSelectivityComputer(
        std::shared_ptr<TOptimizerStatistics> stats,
        bool collectColumnsStatUsedMembers = false,
        bool collectMemberEqualities = false,
        bool collectConstantMembers = false
    )
        : Stats(std::move(stats))
        , CollectColumnsStatUsedMembers(collectColumnsStatUsedMembers)
        , CollectMemberEqualities(collectMemberEqualities)
        , CollectConstantMembers(collectConstantMembers)
    {}

    double Compute(const NNodes::TExprBase& input);

    TColumnStatisticsUsedMembers GetColumnStatsUsedMembers() {
        Y_ENSURE(CollectColumnsStatUsedMembers);
        return ColumnStatsUsedMembers;
    }

    TVector<std::pair<NNodes::TCoMember, NNodes::TCoMember>> GetMemberEqualities() {
        return MemberEqualities;
    }

    TVector<NNodes::TCoMember> GetConstantMembers() {
        return ConstantMembers;
    }

protected:
    std::shared_ptr<TTreeNode> ComputeImpl(
        const NNodes::TExprBase& input,
        bool underNot,
        bool collectConstantMembers
    );

    double ComputeEqualitySelectivity(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool collectConstantMembers
    );

    double ComputeInequalitySelectivity(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool collectConstantMembers,
        EInequalityPredicateType predicate
    );

    double ComputeComparisonSelectivity(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool is_contain_str
    );

    std::shared_ptr<TTreeNode> ConvertEqualityToRange(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool underNot,
        bool collectMembers
    );

    std::shared_ptr<TTreeNode> ProcessSetPredicate(
        const NNodes::TExprBase& left,
        const TExprNode::TPtr list,
        bool underNot,bool collectMembers
    );

    std::shared_ptr<TTreeNode> ConvertInequalityToRange(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool underNot,
        bool collectMembers,
        EInequalityPredicateType inequalitySign
    );

    std::shared_ptr<TTreeNode> ProcessRegexPredicte(
        bool underNot,
        bool collectMembers
    );

    std::shared_ptr<TTreeNode> ProcessStringPredicate(
        const NNodes::TExprBase& left,
        const NNodes::TExprBase& right,
        bool underNot,
        bool collectMembers,
        bool is_contain_str
    );

private:
    std::shared_ptr<TOptimizerStatistics> Stats;

    bool CollectColumnsStatUsedMembers = false;
    TColumnStatisticsUsedMembers ColumnStatsUsedMembers{};

    bool CollectMemberEqualities = false;
    TVector<std::pair<NNodes::TCoMember, NNodes::TCoMember>> MemberEqualities{};

    bool CollectConstantMembers = false;
    TVector<NNodes::TCoMember> ConstantMembers{};
};

bool NeedCalc(NNodes::TExprBase node);
bool IsConstantExpr(const TExprNode::TPtr& input, bool foldUdfs = true);
bool IsConstantExprWithParams(const TExprNode::TPtr& input);

TCardinalityHints::TCardinalityHint* FindCardHint(TVector<TString>& labels, TCardinalityHints& hints);
TCardinalityHints::TCardinalityHint* FindBytesHint(TVector<TString>& labels, TCardinalityHints& hints);
std::shared_ptr<TOptimizerStatistics> ApplyBytesHints(std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    TCardinalityHints hints);
std::shared_ptr<TOptimizerStatistics> ApplyRowsHints(
    std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    TCardinalityHints hints);

} // namespace NYql::NDq {
