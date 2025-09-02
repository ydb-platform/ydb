#pragma once

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

/*
 * This header contains an algorithm for resolving join conflicts with TConflictRulesCollector class
 * and ConvertConflictRulesIntoTES function, which are used to construct the hypergraph.
 */

namespace NYql::NDq {

bool OperatorIsCommutative(EJoinKind);

bool OperatorsAreAssociative(EJoinKind, EJoinKind);

/*
 * Asscom property is important for semi-joins:
 * enables pushing semi-join conditions down to reduce intermediate sizes
 */

/* (e1 o12 e2) o13 e3 == (e1 o13 e3) o12 e2 */
bool OperatorsAreLeftAsscom(EJoinKind, EJoinKind);

/* e1 o13 (e2 o23 e3) == e2 o23 (e1 o13 e3) */
bool OperatorsAreRightAsscom(EJoinKind, EJoinKind);

/*
 * Represents a constraint that restricts join reordering to maintain correctness.
 *
 * A conflict rule states: "If any table from RuleActivationNodes is present in the current
 * join scope, then ALL tables from RequiredNodes must also be present before this join
 * can be evaluated."
 *
 * This captures dependencies arising from join operator properties:
 * - When operators don't satisfy ASSOC/LASSCOM/RASSCOM properties
 * - When joins have special constraints (e.g., ANY joins, non-reorderable joins)
 *
 * Example: For LEFT JOIN where LASSCOM doesn't hold, if we have (A LEFT JOIN B) INNER JOIN C,
 * we cannot reorder to (A INNER JOIN C) LEFT JOIN B, so we create a conflict rule
 * requiring B to be present whenever A is being joined.
 */
template <typename TNodeSet>
struct TConflictRule {
    TConflictRule(const TNodeSet& ruleActivationNodes, const TNodeSet& requiredNodes)
        : RuleActivationNodes(ruleActivationNodes)
        , RequiredNodes(requiredNodes)
    {}

    TNodeSet RuleActivationNodes;  // Tables that trigger this constraint
    TNodeSet RequiredNodes;        // Tables that must be present when rule activates
};

/*
 * This class finds and collect conflicts between root of subtree and its nodes.
 * It traverses both sides of root and checks algebraic join properties (ASSOC, LASSCOM, RASSCOM).
 * The name of algorithm is "CD-C", and details are described in white papper -
 * - "On the Correct and Complete Enumeration of the Core Search Space" in section "5.4 Approach CD-C".
 */
template<typename TNodeSet>
class TConflictRulesCollector {
public:
    TConflictRulesCollector(
        std::shared_ptr<TJoinOptimizerNode> root,
        std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes
    )
        : Root_(root)
        , ConflictRules_({})
        , SubtreeNodes_(subtreeNodes)
    {}

    /*
     * Main entry point that collects all conflict rules for the join tree.
     *
     * Analyzes both left and right subtrees of the root join to identify
     * reordering constraints based on join operator algebraic properties.
     * Uses different visitor strategies for left vs right subtrees because
     * the conflict patterns differ depending on the position in the join tree.
     */
    TVector<TConflictRule<TNodeSet>> CollectConflicts() {
        VisitJoinTree(Root_->LeftArg, GetLeftConflictsVisitor());
        VisitJoinTree(Root_->RightArg, GetRightConflictsVisitor());
        return std::move(ConflictRules_);
    }

private:
    auto GetLeftConflictsVisitor() {
        auto visitor = [this](const std::shared_ptr<TJoinOptimizerNode>& child) {
            if (!OperatorsAreAssociative(child->JoinType, Root_->JoinType)) {
                ConflictRules_.emplace_back(
                    SubtreeNodes_[child->RightArg],
                    SubtreeNodes_[child->LeftArg]
                );
            }

            if (!OperatorsAreLeftAsscom(child->JoinType, Root_->JoinType)) {
                ConflictRules_.emplace_back(
                    SubtreeNodes_[child->LeftArg],
                    SubtreeNodes_[child->RightArg]
                );
            }
        };

        return visitor;
    }

    auto GetRightConflictsVisitor() {
        auto visitor = [this](const std::shared_ptr<TJoinOptimizerNode>& child) {
            if (!OperatorsAreAssociative(Root_->JoinType, child->JoinType)) {
                ConflictRules_.emplace_back(
                    SubtreeNodes_[child->LeftArg],
                    SubtreeNodes_[child->RightArg]
                );
            }

            if (!OperatorsAreRightAsscom(Root_->JoinType, child->JoinType)) {
                ConflictRules_.emplace_back(
                    SubtreeNodes_[child->RightArg],
                    SubtreeNodes_[child->LeftArg]
                );
            }
        };

        return visitor;
    }

private:
    /*
     * Recursively traverses the join tree and applies the visitor function to each join node.
     *
     * This method performs a post-order traversal, processing children before parents.
     * For each join node encountered, it:
     * 1. Recursively visits left and right subtrees
     * 2. Adds conflict rules for special join constraints (ANY joins, non-reorderable joins)
     * 3. Applies the provided visitor function to analyze algebraic properties
     */
    template <typename TFunction>
    void VisitJoinTree(const std::shared_ptr<IBaseOptimizerNode>& child, TFunction visitor) {
        if (child->Kind == EOptimizerNodeKind::RelNodeType) {
            return;
        }

        auto childJoinNode = std::static_pointer_cast<TJoinOptimizerNode>(child);
        VisitJoinTree(childJoinNode->LeftArg, visitor);
        VisitJoinTree(childJoinNode->RightArg, visitor);

        if (childJoinNode->LeftAny || !childJoinNode->IsReorderable) {
            ConflictRules_.emplace_back(
                SubtreeNodes_[childJoinNode->LeftArg],
                SubtreeNodes_[childJoinNode->RightArg]
            );
        }

        if (childJoinNode->RightAny || !childJoinNode->IsReorderable) {
            ConflictRules_.emplace_back(
                SubtreeNodes_[childJoinNode->RightArg],
                SubtreeNodes_[childJoinNode->LeftArg]
            );
        }

        visitor(childJoinNode);
    }

private:
    std::shared_ptr<TJoinOptimizerNode> Root_;
    TVector<TConflictRule<TNodeSet>> ConflictRules_;
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& SubtreeNodes_;
};

/*
 * This function converts conflict rules into TES.
 * TES (Total Eligibility Set) captures reordering constraints and represents
 * set of table, that must present, before join expresion can be evaluated.
 * It is initialized with SES (Syntatic Eligibility Set) - condition used tables.
 */
template <typename TNodeSet>
TNodeSet ConvertConflictRulesIntoTES(const TNodeSet& SES, TVector<TConflictRule<TNodeSet>>& conflictRules) {
    auto TES = SES;

    while (true) {
        auto prevTES = TES;

        for (const auto& conflictRule: conflictRules) {
            if (Overlaps(conflictRule.RuleActivationNodes, TES)) {
                TES |= conflictRule.RequiredNodes;
            }
        }

        EraseIf(
            conflictRules,
            [&](const TConflictRule<TNodeSet>& conflictRule){ return IsSubset(conflictRule.RequiredNodes, TES); }
        );

        if (TES == prevTES || conflictRules.empty()) {
            return TES;
        }
    }
}

} // namespace NYql::NDq
