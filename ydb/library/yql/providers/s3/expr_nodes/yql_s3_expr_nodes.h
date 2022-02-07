#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.decl.inl.h>

class TS3DataSource: public NGenerated::TS3DataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TS3DataSource(const TExprNode* node)
        : TS3DataSourceStub(node)
    {
    }

    explicit TS3DataSource(const TExprNode::TPtr& node)
        : TS3DataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TS3DataSourceStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != S3ProviderName) {
            return false;
        }

        return true;
    }
};

class TS3DataSink : public NGenerated::TS3DataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TS3DataSink(const TExprNode* node)
        : TS3DataSinkStub(node) {}

    explicit TS3DataSink(const TExprNode::TPtr& node)
        : TS3DataSinkStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!TS3DataSinkStub::Match(node)) {
            return false;
        }

        if (node->Head().Content() != S3ProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
