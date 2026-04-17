#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/hash.h>

namespace NYql {

class TNodeTransform {
public:
    explicit TNodeTransform(TExprNode::TPtr lambda)
        : Func_(std::move(lambda))
    {
    }

    TExprNode::TPtr operator()(const TExprNode::TPtr& node, TExprContext& ctx) const {
        // clang-format off
        return ctx.Builder(node->Pos())
            .Apply(Func_)
                .With(0, node)
            .Seal()
            .Build();
        // clang-format on
    }

    TExprNode::TPtr Func() const {
        return Func_;
    }

private:
    TExprNode::TPtr Func_;
};

} // namespace NYql
