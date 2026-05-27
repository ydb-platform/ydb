#pragma once

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

using namespace NYql::NNodes;
using NYql::TExprNode;

enum class EInequalityPredicateType : ui8 { Less, LessOrEqual, Greater, GreaterOrEqual, Equal, NotEqual };

enum class ELogicalOperator : ui8 { And, Or, Leaf };

struct TPredicateRange {
    TMaybe<TExprBase> Left;
    TMaybe<TString> LeftBound;
    bool LeftInclusive = false;

    TMaybe<TExprBase> Right;
    TMaybe<TString> RightBound;
    bool RightInclusive = false;
};

struct TTreeNode {
    ELogicalOperator Operator;

    // For And / Or
    TVector<std::shared_ptr<TTreeNode>> Children;

    // For Leaf
    TString Column;
    TString ColumnType;
    TMaybe<TString> TableAlias;
    double Selectivity = 0.0;
    TPredicateRange Range;
};

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
    std::shared_ptr<TTreeNode> ComputeImpl(
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
        bool collectConstantMembers,
        EInequalityPredicateType predicate
    );

    double ComputeComparisonSelectivity(
        const TExprBase& left,
        const TExprBase& right,
        bool containString
    );

    std::shared_ptr<TTreeNode> ConvertEqualityToRange(
        const TExprBase& left,
        const TExprBase& right,
        bool underNot,
        bool collectConstantMembers
    );

    std::shared_ptr<TTreeNode> ProcessSetPredicate(
        const TExprBase& left,
        const TExprNode::TPtr list,
        bool underNot,
        bool collectConstantMembers
    );

    std::shared_ptr<TTreeNode> ConvertInequalityToRange(
        const TExprBase& left,
        const TExprBase& right,
        bool underNot,
        bool collectConstantMembers,
        EInequalityPredicateType inequalitySign
    );

    std::shared_ptr<TTreeNode> ProcessRegexPredicte(
        bool underNot,
        bool collectConstantMembers
    );

    std::shared_ptr<TTreeNode> ProcessStringPredicate(
        const TExprBase& left,
        const TExprBase& right,
        bool underNot,
        bool collectConstantMembers,
        bool containString
    );

    TMaybe<TString> GetAttributeType(const TString& attributeName);
    std::shared_ptr<TTreeNode> CreateLeafNode(TMaybe<TString> attribute);

    double ComputeSelectivity(
        const std::shared_ptr<TTreeNode>& node,
        TSet<TString>& tableAliases
    );

    double ReComputeEstimation(
        TString attributeName,
        TPredicateRange& mergedRange
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
