#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

TExprNode::TPtr FindTopLevelRead(const TExprNode::TPtr& queryGraph);

TExprNode::TPtr RewriteReadFromView(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    const TString& query,
    const TString& cluster
);

}