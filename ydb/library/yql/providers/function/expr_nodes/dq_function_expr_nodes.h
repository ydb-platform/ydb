#pragma once

#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.gen.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.decl.inl.h>

class TFunctionDataSource: public NGenerated::TFunctionDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TFunctionDataSource(const TExprNode* node)
        : TFunctionDataSourceStub(node)
    {
    }

    explicit TFunctionDataSource(const TExprNode::TPtr& node)
        : TFunctionDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TFunctionDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != FunctionProviderName) {
            return false;
        }

        return true;
    }
};

class TFunctionDataSink: public NGenerated::TFunctionDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TFunctionDataSink(const TExprNode* node)
        : TFunctionDataSinkStub(node)
    {
    }

    explicit TFunctionDataSink(const TExprNode::TPtr& node)
        : TFunctionDataSinkStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TFunctionDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != FunctionProviderName) {
            return false;
        }

        return true;
    }
};


#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
