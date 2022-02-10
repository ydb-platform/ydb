#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.decl.inl.h>

class TSoDataSource: public NGenerated::TSoDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TSoDataSource(const TExprNode* node)
        : TSoDataSourceStub(node)
    {
    }

    explicit TSoDataSource(const TExprNode::TPtr& node)
        : TSoDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TSoDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != SolomonProviderName) {
            return false;
        }

        return true;
    }
};


class TSoDataSink: public NGenerated::TSoDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TSoDataSink(const TExprNode* node)
        : TSoDataSinkStub(node)
    {
    }

    explicit TSoDataSink(const TExprNode::TPtr& node)
        : TSoDataSinkStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TSoDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != SolomonProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
