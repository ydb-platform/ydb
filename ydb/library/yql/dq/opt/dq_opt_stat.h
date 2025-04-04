#pragma once

#include "dq_opt.h"

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateCombine(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqMerge(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});
void InferStatisticsForDqJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});
void InferStatisticsForDqPhyCrossJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAsList(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
std::shared_ptr<TOptimizerStatistics> RemoveOrdering(const std::shared_ptr<TOptimizerStatistics>& stats);
std::shared_ptr<TOptimizerStatistics> RemoveOrdering(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);

class TPredicateSelectivityComputer {
public:
    struct TColumnStatisticsUsedMembers {
        struct TColumnStatisticsUsedMember {
            enum _ : ui32 {
                EEquality
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

        TVector<TColumnStatisticsUsedMember> Data{};
    };

    // this class exists to add functional dependencies later for Cost Based Optimizer
    struct TMemberEqualities {
        void Add(const NNodes::TCoMember& lhs, const NNodes::TCoMember& rhs) {
            Data.emplace_back(std::move(lhs), std::move(rhs));
        }

        TVector<std::pair<NNodes::TCoMember, NNodes::TCoMember>> Data{};
    };

public:
    TPredicateSelectivityComputer(
        const std::shared_ptr<TOptimizerStatistics>& stats,
        bool collectColumnsStatUsedMembers = false,
        bool collectMemberEqualities = false
    )
        : Stats(stats)
        , CollectColumnsStatUsedMembers(collectColumnsStatUsedMembers)
        , CollectMemberEqualities(collectMemberEqualities)
    {}

    double Compute(const NNodes::TExprBase& input);

    TColumnStatisticsUsedMembers GetColumnStatsUsedMembers() {
        Y_ENSURE(CollectColumnsStatUsedMembers);
        return ColumnStatsUsedMembers;
    }

    TMemberEqualities GetMemberEqualities() {
        return MemberEqualities;
    }

protected:
    double ComputeEqualitySelectivity(const NYql::NNodes::TExprBase& left, const NYql::NNodes::TExprBase& right);

    double ComputeComparisonSelectivity(const NYql::NNodes::TExprBase& left, const NYql::NNodes::TExprBase& right);

private:
    const std::shared_ptr<TOptimizerStatistics>& Stats;
    TColumnStatisticsUsedMembers ColumnStatsUsedMembers{};
    TMemberEqualities MemberEqualities{};

    bool CollectColumnsStatUsedMembers = false;
    bool CollectMemberEqualities = false;
};

bool NeedCalc(NNodes::TExprBase node);
bool IsConstantExpr(const TExprNode::TPtr& input);
bool IsConstantExprWithParams(const TExprNode::TPtr& input);

} // namespace NYql::NDq {
