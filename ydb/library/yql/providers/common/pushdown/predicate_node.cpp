#include "predicate_node.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql::NPushdown {

TPredicateNode::TPredicateNode() = default;

TPredicateNode::TPredicateNode(const TExprNode::TPtr& nodePtr)
    : ExprNode(nodePtr)
{}

TPredicateNode::TPredicateNode(const NNodes::TExprBase& node)
    : ExprNode(node)
{}

TPredicateNode::TPredicateNode(const TPredicateNode& predNode) = default;

TPredicateNode::~TPredicateNode() = default;

bool TPredicateNode::IsValid() const {
    bool res = true;
    if (Op != EBoolOp::Undefined) {
        res &= !Children.empty();
        for (auto& child : Children) {
            res &= child.IsValid();
        }
    }

    return res && ExprNode.IsValid();
}

void TPredicateNode::SetPredicates(const std::vector<TPredicateNode>& predicates, TExprContext& ctx, TPositionHandle pos) {
    auto predicatesSize = predicates.size();
    if (predicatesSize == 0) {
        return;
    } else if (predicatesSize == 1) {
        *this = predicates[0];
    } else {
        Op = EBoolOp::And;
        Children = predicates;
        CanBePushed = true;

        TVector<NNodes::TExprBase> exprNodes;
        exprNodes.reserve(predicatesSize);
        for (auto& pred : predicates) {
            exprNodes.emplace_back(pred.ExprNode.Cast());
            CanBePushed &= pred.CanBePushed;
        }
        ExprNode = NNodes::Build<NNodes::TCoAnd>(ctx, pos)
            .Add(exprNodes)
            .Done();
    }
}

} // namespace NYql::NPushdown
