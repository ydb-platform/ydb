#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.decl.inl.h>

class TClDataSource: public NGenerated::TClDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TClDataSource(const TExprNode* node)
        : TClDataSourceStub(node)
    {
    }

    explicit TClDataSource(const TExprNode::TPtr& node)
        : TClDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TClDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != ClickHouseProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
