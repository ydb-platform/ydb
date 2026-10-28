#pragma once

#include <ydb/library/yql/providers/qyt/expr_nodes/yql_qyt_expr_nodes.gen.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/qyt/expr_nodes/yql_qyt_expr_nodes.decl.inl.h>

class TQytDataSource: public NGenerated::TQytDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TQytDataSource(const TExprNode* node)
        : TQytDataSourceStub(node)
    {
    }

    explicit TQytDataSource(const TExprNode::TPtr& node)
        : TQytDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TQytDataSourceStub::Match(node)) {
            return false;
        }

        return node->ChildrenSize() == 2 && node->Child(0)->IsAtom(QytProviderName) && node->Child(1)->IsAtom();
    }
};

#include <ydb/library/yql/providers/qyt/expr_nodes/yql_qyt_expr_nodes.defs.inl.h>

}   // namespace NNodes
}   // namespace NYql
