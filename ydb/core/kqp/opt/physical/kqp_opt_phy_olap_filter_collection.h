#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr::NKqp::NOpt {

enum class EBoolOp {
    Undefined = 0,
    And,
    Or,
    Xor,
    Not
};

struct TPredicateNode {
    TPredicateNode()
        : ExprNode(nullptr)
        , Op(EBoolOp::Undefined)
        , CanBePushed(false)
    {}

    TPredicateNode(NYql::TExprNode::TPtr nodePtr)
        : ExprNode(nodePtr)
        , Op(EBoolOp::Undefined)
        , CanBePushed(false)
    {}

    TPredicateNode(NYql::NNodes::TExprBase node)
        : ExprNode(node)
        , Op(EBoolOp::Undefined)
        , CanBePushed(false)
    {}

    TPredicateNode(const TPredicateNode& predNode)
        : ExprNode(predNode.ExprNode)
        , Children(predNode.Children)
        , Op(predNode.Op)
        , CanBePushed(predNode.CanBePushed)
    {}

    ~TPredicateNode() {}

    bool IsValid() const;
    void SetPredicates(const std::vector<TPredicateNode>& predicates, NYql::TExprContext& ctx, NYql::TPositionHandle pos);

    NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> ExprNode;
    std::vector<TPredicateNode> Children;
    EBoolOp Op;
    bool CanBePushed;
};

void CollectPredicates(const NYql::NNodes::TExprBase& predicate, TPredicateNode& predicateTree,
    const NYql::TExprNode* lambdaArg, const NYql::NNodes::TExprBase& lambdaBody);

} // namespace NKikimr::NKqp::NOpt