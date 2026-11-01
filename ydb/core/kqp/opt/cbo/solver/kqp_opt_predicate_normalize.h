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

enum class EPredicateKind : ui8 {
    Comparison,   // column vs constant (=, <, <=, >, >=);
    TwoColumns,   // column vs column
    StringMatch,  // StartsWith, EndsWith, StringContains, Find
    Regex,        // Re2 UDF
    BoolLiteral,  // TRUE / FALSE
    NonAttribute, // neither side is a column
    NonConstant,  // column vs a non-constant expression
    Parameter,    // column vs an expression with parameters
    MergedRanges, // comparisons on one attribute, the column is in the union of Ranges
};

// Predicate tree. ComputeImpl builds it with no selectivities, only what is needed to compute them later;
// Normalize returns a tree of the same type with the comparisons on one attribute merged into MergedRanges leaves.
struct TTreeNode {
    ELogicalOperator Operator;

    // For And / Or
    TVector<std::shared_ptr<TTreeNode>> Children;

    // For Leaf
    EPredicateKind Kind = EPredicateKind::NonAttribute;
    TMaybe<TExprBase> Left;  // attribute side (for Comparison after the operand swap)
    TMaybe<TExprBase> Right; // value side
    EInequalityPredicateType Sign = EInequalityPredicateType::Equal;
    bool UnderNot = false;      // StringMatch / Regex / TwoColumns: the predicate is negated
    bool ContainString = false; // StringMatch: StringContains / Find
    bool BoolValue = true;      // BoolLiteral: value with NOT applied
    TString Attribute;          // full attribute name, e.g. "alias.column"
    TString Column;
    TString ColumnType;
    TMaybe<TString> TableAlias;
    TPredicateRange Range;      // Comparison with a literal value

    // For MergedRanges
    TVector<TPredicateRange> Ranges;                   // disjoint, sorted; empty when an And intersection is empty
    TVector<std::shared_ptr<TTreeNode>> MergedLeaves;  // the merged Comparison leaves

    TString ToString(ui32 indent = 0) const;
};

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

namespace NPredicateTree {

TMaybe<TString> IsAttribute(const TExprBase& input);

EInequalityPredicateType GetOppositePredicateType(EInequalityPredicateType predicate);

EInequalityPredicateType GetOppositeRangeDirection(EInequalityPredicateType predicate);

TMaybe<TString> ExtractLiteral(TExprBase maybeLiteral);

void CollectTableAliases(const std::shared_ptr<TTreeNode>& node, TSet<TString>& tableAliases);

} // namespace NPredicateTree

// Builds the predicate tree from a predicate expression and normalizes it. Uses statistics only as a fallback for
// column types; computes no selectivity.
class TPredicateTreeBuilder {
public:
    TPredicateTreeBuilder(
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

    // Builds the predicate tree and collects members as configured.
    std::shared_ptr<TTreeNode> Build(const TExprBase& input);

    // Returns a tree where the comparisons of every And / Or node on one attribute are merged into a MergedRanges leaf.
    // The input tree is not modified and its leaves are shared.
    static std::shared_ptr<TTreeNode> Normalize(const std::shared_ptr<TTreeNode>& node);

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

    std::shared_ptr<TTreeNode> ProcessBoolLiteralPredicate(
        bool value,
        bool underNot,
        bool collectConstantMembers
    );

    std::shared_ptr<TTreeNode> ProcessRegexPredicate(
        const TExprBase* input,
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

    std::shared_ptr<TTreeNode> CreateLeafNode(
        EPredicateKind kind,
        const TExprBase* left = nullptr,
        const TExprBase* right = nullptr,
        EInequalityPredicateType sign = EInequalityPredicateType::Equal,
        bool underNot = false
    );

    void CollectColumnStatsUsedMember(const TExprBase& attribute, bool equality);

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
