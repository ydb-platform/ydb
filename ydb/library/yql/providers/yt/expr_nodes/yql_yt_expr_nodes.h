#pragma once

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.gen.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.decl.inl.h>

class TYtDSource: public NGenerated::TYtDSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYtDSource(const TExprNode* node)
        : TYtDSourceStub(node)
    {
    }

    explicit TYtDSource(const TExprNode::TPtr& node)
        : TYtDSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TYtDSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != YtProviderName) {
            return false;
        }

        return true;
    }
};

class TYtDSink: public NGenerated::TYtDSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYtDSink(const TExprNode* node)
        : TYtDSinkStub(node)
    {
    }

    explicit TYtDSink(const TExprNode::TPtr& node)
        : TYtDSinkStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TYtDSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != YtProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
