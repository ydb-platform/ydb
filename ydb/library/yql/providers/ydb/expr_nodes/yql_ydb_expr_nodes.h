#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.decl.inl.h>

class TYdbDataSource: public NGenerated::TYdbDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYdbDataSource(const TExprNode* node)
        : TYdbDataSourceStub(node)
    {
    }

    explicit TYdbDataSource(const TExprNode::TPtr& node)
        : TYdbDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TYdbDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != YdbProviderName) {
            return false;
        }

        return true;
    }
};

class TYdbDataSink : public NGenerated::TYdbDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYdbDataSink(const TExprNode* node)
        : TYdbDataSinkStub(node) {}

    explicit TYdbDataSink(const TExprNode::TPtr& node)
        : TYdbDataSinkStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!TYdbDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != YdbProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
