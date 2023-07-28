#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.gen.h>


namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.decl.inl.h>

class TStatDSource: public NGenerated::TStatDSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TStatDSource(const TExprNode* node)
        : TStatDSourceStub {node}
    {
    }

    explicit TStatDSource(const TExprNode::TPtr& node)
        : TStatDSourceStub {node}
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TStatDSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != StatProviderName) {
            return false;
        }

        return true;
    }
};


class TStatDSink: public NGenerated::TStatDSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TStatDSink(const TExprNode* node)
        : TStatDSinkStub {node}
    {
    }

    explicit TStatDSink(const TExprNode::TPtr& node)
        : TStatDSinkStub {node}
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TStatDSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != StatProviderName) {
            return false;
        }

        return true;
    }
};

#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
