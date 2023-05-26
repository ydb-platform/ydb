#pragma once

#include "yql_dq_state.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {

bool ValidateDqExecution(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx, const TDqState::TPtr state);

} // namespace NYql
