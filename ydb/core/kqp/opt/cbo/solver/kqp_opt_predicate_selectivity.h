#pragma once

#include "kqp_opt_predicate_normalize.h"

namespace NKikimr::NKqp {

// Estimates predicate selectivity on the predicate tree built by TPredicateTreeBuilder.
class TPredicateSelectivityComputer {
public:
    using TColumnStatisticsUsedMembers = NKikimr::NKqp::TColumnStatisticsUsedMembers;

    TPredicateSelectivityComputer(
        std::shared_ptr<TOptimizerStatistics> stats,
        bool collectColumnsStatUsedMembers = false,
        bool collectMemberEqualities = false,
        bool collectConstantMembers = false
    )
        : Stats(std::move(stats))
        , Builder(Stats, collectColumnsStatUsedMembers, collectMemberEqualities, collectConstantMembers)
    {}

    // TPredicateTreeBuilder::Build, TPredicateTreeBuilder::Normalize, then ComputeSelectivity
    double Compute(const TExprBase& input);

    // Estimates the selectivity of a predicate tree with this computer's statistics
    double ComputeSelectivity(const std::shared_ptr<TTreeNode>& node);

    TColumnStatisticsUsedMembers GetColumnStatsUsedMembers() {
        return Builder.GetColumnStatsUsedMembers();
    }

    TVector<std::pair<TCoMember, TCoMember>> GetMemberEqualities() {
        return Builder.GetMemberEqualities();
    }

    TVector<TCoMember> GetConstantMembers() {
        return Builder.GetConstantMembers();
    }

protected:
    double ComputeEqualitySelectivity(
        const TExprBase& left,
        const TExprBase& right,
        bool collectConstantMembers
    );

    double ComputeInequalitySelectivity(
        const TExprBase& left,
        const TExprBase& right,
        bool collectConstantMembers,
        EInequalityPredicateType predicate
    );

    double ComputeComparisonSelectivity(
        const TExprBase& left,
        const TExprBase& right,
        bool containString
    );

    double ComputeLeafSelectivity(const TTreeNode& leaf);

    double ReComputeEstimation(
        const TString& attributeName,
        const TPredicateRange& mergedRange
    );

private:
    std::shared_ptr<TOptimizerStatistics> Stats;
    TPredicateTreeBuilder Builder;
};

} // namespace NKikimr::NKqp
