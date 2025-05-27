#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.gen.h>


namespace NYql::NNodes {

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.decl.inl.h>

class TYtflowDSource: public NGenerated::TYtflowDSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYtflowDSource(const TExprNode* node);
    explicit TYtflowDSource(const TExprNode::TPtr& node);

    static bool Match(const TExprNode* node);
};


class TYtflowDSink: public NGenerated::TYtflowDSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYtflowDSink(const TExprNode* node);
    explicit TYtflowDSink(const TExprNode::TPtr& node);

    static bool Match(const TExprNode* node);
};

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.defs.inl.h>

} // namespace NYql::NNodes
