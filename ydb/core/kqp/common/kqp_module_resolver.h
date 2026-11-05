#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp {

bool GetKqpDefaultModuleResolver(NYql::TExprContext& ctx, NYql::IModuleResolver::TPtr& moduleResolver);

} // namespace NKikimr::NKqp
