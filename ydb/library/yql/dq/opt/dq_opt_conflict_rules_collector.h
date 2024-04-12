#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 

namespace NYql::NDq {

bool OperatorIsCommut(EJoinKind);

bool OperatorsAreAssoc(EJoinKind, EJoinKind);

bool OperatorsAreLeftAsscom(EJoinKind, EJoinKind);

bool OperatorsAreRightAsscom(EJoinKind, EJoinKind);

template <typename TNodeSet>
struct TConflictRule {
    TConflictRule(const TNodeSet& ruleActivationNodes, const TNodeSet& requiredNodes) 
        : RuleActivationNodes(ruleActivationNodes)
        , RequiredNodes(requiredNodes)
    {}

    TNodeSet RuleActivationNodes;
    TNodeSet RequiredNodes;
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

    TVector<TConflictRule<TNodeSet>> CollectConflicts() {
        VisitJoinTree(Root_->LeftArg, GetLeftConflictsVisitor());
        VisitJoinTree(Root_->RightArg, GetRightConflictsVisitor());
        return std::move(ConflictRules_);
    }

private:
    auto GetLeftConflictsVisitor() {
        auto visitor = [this](const std::shared_ptr<TJoinOptimizerNode>& child) {
            if (!OperatorsAreAssoc(child->JoinType, Root_->JoinType)) {
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
            if (!OperatorsAreAssoc(Root_->JoinType, child->JoinType)) {
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
    template <typename TFunction>
    void VisitJoinTree(const std::shared_ptr<IBaseOptimizerNode>& child, TFunction visitor) {
        if (child->Kind == EOptimizerNodeKind::RelNodeType) {
            return;
        }

        auto childJoinNode = std::static_pointer_cast<TJoinOptimizerNode>(child);
        VisitJoinTree(childJoinNode->LeftArg, visitor);
        VisitJoinTree(childJoinNode->RightArg, visitor);

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
TNodeSet ConvertConflictRulesIntoTES(const TNodeSet& SES, TVector<TConflictRule<TNodeSet>> conflictRules) {
    auto TES = SES;

    while (true) {
        auto prevTES = TES;

        for (const auto& conflictRule: conflictRules) {
            if (AreOverlaps(conflictRule.RuleActivationNodes, TES)) {
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
