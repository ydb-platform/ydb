#pragma once


#include <ydb/library/yql/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

TVector<NYql::TExprNode::TPtr> RewriteExpression(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx);

}
}
