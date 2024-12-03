#include "predicate_node.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

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

bool TPredicateNode::IsEmpty() const {
    if (!ExprNode || !IsValid()) {
        return true;
    }
    if (const auto maybeBool = ExprNode.Maybe<NNodes::TCoBool>()) {
        return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
    }
    return false;
}

void TPredicateNode::SetPredicates(const std::vector<TPredicateNode>& predicates, TExprContext& ctx, TPositionHandle pos, EBoolOp op) {
    auto predicatesSize = predicates.size();
    if (predicatesSize == 0) {
        return;
    }
    if (predicatesSize == 1) {
        *this = predicates[0];
        return;
    }

    Op = op;
    Children = predicates;
    CanBePushed = true;

    TVector<NNodes::TExprBase> exprNodes;
    exprNodes.reserve(predicatesSize);
    for (auto& pred : predicates) {
        exprNodes.emplace_back(pred.ExprNode.Cast());
        CanBePushed &= pred.CanBePushed;
    }

    switch (op) {
        case EBoolOp::And:
            ExprNode = NNodes::Build<NNodes::TCoAnd>(ctx, pos).Add(exprNodes).Done();
            break;

        case EBoolOp::Or:
            ExprNode = NNodes::Build<NNodes::TCoOr>(ctx, pos).Add(exprNodes).Done();
            break;

        default:
            throw yexception() << "Unsupported operator for predicate node creation: " << static_cast<int>(op);
    }
}

} // namespace NYql::NPushdown
