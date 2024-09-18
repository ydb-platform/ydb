#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/datetime/base.h>

namespace NYql::NHopping {

TExprNode::TPtr RewriteAsHoppingWindow(TExprNode::TPtr node, TExprContext& ctx);

} // namespace NYql::NHopping
