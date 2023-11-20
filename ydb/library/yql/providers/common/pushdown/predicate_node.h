#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_pos_handle.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NYql::NPushdown {

enum class EBoolOp {
    Undefined = 0,
    And,
    Or,
    Xor,
    Not
};

struct TPredicateNode {
    TPredicateNode();
    TPredicateNode(const TExprNode::TPtr& nodePtr);
    TPredicateNode(const NNodes::TExprBase& node);
    TPredicateNode(const TPredicateNode& predNode);

    ~TPredicateNode();

    bool IsValid() const;
    void SetPredicates(const std::vector<TPredicateNode>& predicates, TExprContext& ctx, TPositionHandle pos);

    NNodes::TMaybeNode<NNodes::TExprBase> ExprNode;
    std::vector<TPredicateNode> Children;
    EBoolOp Op = EBoolOp::Undefined;
    bool CanBePushed = false;
};

} // namespace NYql::NPushdown
