#pragma once

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.gen.h>

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql::NNodes {

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.decl.inl.h>

struct TTopSortSettings {
    static inline const TString AscendingSort = "Asc";
    static inline const TString DescendingSort = "Desc";
};

class TDqConnection : public NGenerated::TDqConnectionStub<TExprBase, TCallable, TDqOutput> {
public:
    explicit TDqConnection(const TExprNode* node)
        : TDqConnectionStub(node) {}

    explicit TDqConnection(const TExprNode::TPtr& node)
        : TDqConnectionStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!node) {
            return false;
        }

        if (!node->IsCallable()) {
            return false;
        }

        if (node->ChildrenSize() < 1) {
            return false;
        }

        return TDqOutput::Match(node->Child(0));
    }
};

class TDqOutputAnnotationBase : public NGenerated::TDqOutputAnnotationBaseStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TDqOutputAnnotationBase(const TExprNode* node)
        : TDqOutputAnnotationBaseStub(node) {}

    explicit TDqOutputAnnotationBase(const TExprNode::TPtr& node)
        : TDqOutputAnnotationBaseStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!node) {
            return false;
        }

        if (!node->IsCallable()) {
            return false;
        }

        if (node->ChildrenSize() < 2) {
            return false;
        }

        return TCoAtom::Match(node->Child(0))
            && TCallable::Match(node->Child(1));
    }
};

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.defs.inl.h>

} // namespace NYql::NDq
