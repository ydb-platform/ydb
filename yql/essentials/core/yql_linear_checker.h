#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

bool ValidateLinearTypes(const TExprNode& root, TExprContext& ctx);

}
