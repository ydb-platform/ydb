#pragma once

#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

using namespace NYql::NNodes;
using NYql::TExprNode;

enum class EInequalityPredicateType : ui8 { Less, LessOrEqual, Greater, GreaterOrEqual, Equal };

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForExtendBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAggregateBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForStage(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForDqMerge(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {}, TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr);
void InferStatisticsForBlockHashJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqJoinBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqPhyCrossJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAsList(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAsStruct(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForTopBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForSortBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForEquiJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForUnionAll(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);

template <typename TAggregationCallable>
void InferStatisticsForAggregationCallable(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
extern template void InferStatisticsForAggregationCallable<TCoShuffleByKeys>(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);


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

TOrderingInfo GetTopBaseSortingOrderingInfo(const TCoTopBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetSortBaseSortingOrderingInfo(const TCoSortBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetAggregationBaseShuffleOrderingInfo(const TCoAggregateBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& shufflingsFSM, TTableAliasMap*);
TVector<TJoinColumn> GetKeySelectorOrdering(const TCoLambda& keySelector);

class TPredicateSelectivityComputer {
public:
    struct TColumnStatisticsUsedMembers {
        struct TColumnStatisticsUsedMember {
            enum _ : ui32 {
                EEquality,
                EInequality
            };

            TColumnStatisticsUsedMember(TCoMember member, ui32 predicateType)
                : Member(std::move(member))
                , PredicateType(predicateType)
            {}

            TCoMember Member;
            ui32 PredicateType;
        };

        void AddEquality(const TCoMember& member) {
            Data.emplace_back(std::move(member), TColumnStatisticsUsedMember::EEquality);
        }

        void AddInequality(const TCoMember& member) {
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

    double Compute(const TExprBase& input);

    TColumnStatisticsUsedMembers GetColumnStatsUsedMembers() {
        Y_ENSURE(CollectColumnsStatUsedMembers);
        return ColumnStatsUsedMembers;
    }

    TVector<std::pair<TCoMember, TCoMember>> GetMemberEqualities() {
        return MemberEqualities;
    }

    TVector<TCoMember> GetConstantMembers() {
        return ConstantMembers;
    }

protected:
    double ComputeImpl(
        const TExprBase& input,
        bool underNot,
        bool collectConstantMembers
    );

    double ComputeEqualitySelectivity(
        const TExprBase& left,
        const TExprBase& right,
        bool collectConstantMembers
    );

    double ComputeInequalitySelectivity(
        const TExprBase& left,
        const TExprBase& right,
        EInequalityPredicateType predicate,
        bool collectConstantMembers
    );

    double ComputeComparisonSelectivity(
        const TExprBase& left,
        const TExprBase& right
    );

private:
    std::shared_ptr<TOptimizerStatistics> Stats;

    bool CollectColumnsStatUsedMembers = false;
    TColumnStatisticsUsedMembers ColumnStatsUsedMembers{};

    bool CollectMemberEqualities = false;
    TVector<std::pair<TCoMember, TCoMember>> MemberEqualities{};

    bool CollectConstantMembers = false;
    TVector<TCoMember> ConstantMembers{};
};

bool NeedCalc(TExprBase node);
// Returns true if the expression is already a fully-evaluated literal
// (TCoDataCtor, TCoNothing, or TCoJust wrapping a literal) and
// does not need further evaluation via EvaluateExpr.
bool IsLiteralDataExpr(TExprBase node);
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

} // namespace NKikimr::NKqp
