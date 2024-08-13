#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForExtractMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateCombine(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});
void InferStatisticsForAsList(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForListParam(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);

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

        TVector<TColumnStatisticsUsedMember> Data;
    };
public:
    TPredicateSelectivityComputer(const std::shared_ptr<TOptimizerStatistics>& stats, bool collectColumnsStatUsedMembers = false)
        : Stats(stats)
        , CollectColumnsStatUsedMembers(collectColumnsStatUsedMembers)
    {}

    double Compute(const NNodes::TExprBase& input);

    TColumnStatisticsUsedMembers GetColumnStatsUsedMembers() {
        Y_ENSURE(CollectColumnsStatUsedMembers);
        return ColumnStatsUsedMembers;
    }

private:
    double ComputeEqualitySelectivity(const NYql::NNodes::TExprBase& left, const NYql::NNodes::TExprBase& right);

    double ComputeComparisonSelectivity(const NYql::NNodes::TExprBase& left, const NYql::NNodes::TExprBase& right);

private:
    const std::shared_ptr<TOptimizerStatistics>& Stats;
    TColumnStatisticsUsedMembers ColumnStatsUsedMembers;
    bool CollectColumnsStatUsedMembers = false;
};

bool NeedCalc(NNodes::TExprBase node);
bool IsConstantExpr(const TExprNode::TPtr& input);
bool IsConstantExprWithParams(const TExprNode::TPtr& input);

} // namespace NYql::NDq {
