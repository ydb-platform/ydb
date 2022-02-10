#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
 
#include <ydb/library/yql/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

bool CanExecuteWithNewEngine(const NYql::NNodes::TKiProgram& program, NYql::TExprContext& ctx);

} // namespace NKqp
} // namespace NKikimr
