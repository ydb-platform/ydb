#pragma once

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

using namespace NYql::NNodes;
using NYql::TExprNode;

enum class EInequalityPredicateType : ui8 { Less, LessOrEqual, Greater, GreaterOrEqual, Equal };

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

} // namespace NKikimr::NKqp
