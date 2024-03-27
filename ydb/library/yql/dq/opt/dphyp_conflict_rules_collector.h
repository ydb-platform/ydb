#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 

#include "dphyp_join_tree_node.h"

namespace NYql::NDq::NDphyp {

template <typename TNodeSet>
bool AreOverlaps(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) != 0;
}

template <typename TNodeSet>
bool IsSubset(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) == lhs;
}

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

template<typename TNodeSet>
class TConflictRulesCollector {
public:
    TConflictRulesCollector(
        std::shared_ptr<TJoinOptimizerNode> root,
        THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes
    )
        : Root_(root)
        , ConflictRules_({})
        , SubtreeNodes_(subtreeNodes)
    {}

    TVector<TConflictRule<TNodeSet>> CollectConflicts() {
        VisitJoinTree(Root_->LeftArg, GetLeftConflictsVisitor());
        VisitJoinTree(Root_->RightArg, GetRightConflictsVisitor());
    }

private:
    auto GetLeftConflictsVisitor() {
        auto visitor = [this](std::shared_ptr<TJoinOptimizerNode> child) {
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
        auto visitor = [this](std::shared_ptr<TJoinOptimizerNode> child) {
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
    void VisitJoinTree(std::shared_ptr<IBaseOptimizerNode> child, TFunction visitor) {
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
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& SubtreeNodes_;
};

template <typename TNodeSet>
void ConvertConflictRulesIntoTES(const TNodeSet& SES, TVector<TConflictRule<TNodeSet>> conflictRules) {
    auto TES = SES;

    while (true) {
        auto prevTES = TES;

        for (const auto& conflictRule: conflictRules) {
            if (AreOverlaps(conflictRule.RuleActivationNodes, TES)) {
                TES |= conflictRule.RequiredNodes;
            }
        }

        auto it = std::erase_if(std::begin(conflictRules), std::end(conflictRules), 
            [&](const TConflictRule<TNodeSet>& conflictRule){ return IsSubset(conflictRule.RequiredNodes, TES);}
        );
        conflictRules.erase(it, conflictRules.end());

        if (TES == prevTES || conflictRules.empty()) {
            return TES;
        }
    }
} 

} // namespace NYql::NDq
