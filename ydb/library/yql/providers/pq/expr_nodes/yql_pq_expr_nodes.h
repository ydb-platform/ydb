#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.decl.inl.h>

class TPqDataSource: public NGenerated::TPqDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TPqDataSource(const TExprNode* node)
        : TPqDataSourceStub(node)
    {
    }

    explicit TPqDataSource(const TExprNode::TPtr& node)
        : TPqDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TPqDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != PqProviderName) {
            return false;
        }

        return true;
    }
};

class TPqDataSink : public NGenerated::TPqDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TPqDataSink(const TExprNode* node)
        : TPqDataSinkStub(node) {}

    explicit TPqDataSink(const TExprNode::TPtr& node)
        : TPqDataSinkStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!TPqDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != PqProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
