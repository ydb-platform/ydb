#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;

struct TPredicateNode {
    enum class EBoolOp {
        Undefined = 0,
        And,
        Or,
        Xor,
        Not
    };

    TPredicateNode() = default;

    TPredicateNode(TExprNode::TPtr nodePtr)
        : ExprNode(std::move(nodePtr))
    {}

    TPredicateNode(const NNodes::TExprBase& node)
        : ExprNode(node)
    {}

    bool IsValid() const {
        return ExprNode.IsValid() &&
            (Op == EBoolOp::Undefined || std::all_of(Children.cbegin(), Children.cend(), std::bind(&TPredicateNode::IsValid, std::placeholders::_1)));
    }

    void SetPredicates(const std::vector<TPredicateNode>& predicates, TExprContext& ctx, TPositionHandle pos);

    NNodes::TMaybeNode<NNodes::TExprBase> ExprNode;
    std::vector<TPredicateNode> Children;
    EBoolOp Op = EBoolOp::Undefined;
    bool CanBePushed = false;
};

void CollectPredicates(const NNodes::TExprBase& predicate, TPredicateNode& predicateTree, const TExprNode* lambdaArg, const NNodes::TExprBase& lambdaBody);

}
