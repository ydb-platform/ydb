#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr &node, TExprContext &ctx);

} //namespace NYql