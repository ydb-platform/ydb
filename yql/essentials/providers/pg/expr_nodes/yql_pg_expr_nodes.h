#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/pg/expr_nodes/yql_pg_expr_nodes.gen.h>

namespace NYql::NNodes {

#include <yql/essentials/providers/pg/expr_nodes/yql_pg_expr_nodes.decl.inl.h>

class TPgDataSource: public NGenerated::TPgDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TPgDataSource(const TExprNode* node)
        : TPgDataSourceStub(node)
    {
    }

    explicit TPgDataSource(const TExprNode::TPtr& node)
        : TPgDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TPgDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != PgProviderName) {
            return false;
        }

        return true;
    }
};

#include <yql/essentials/providers/pg/expr_nodes/yql_pg_expr_nodes.defs.inl.h>

} // namespace NYql::NNodes
