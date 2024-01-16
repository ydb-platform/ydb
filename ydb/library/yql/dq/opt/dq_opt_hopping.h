#pragma once

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <util/generic/ptr.h>

namespace NYql::NDq {

NNodes::TMaybeNode<NNodes::TExprBase> RewriteAsHoppingWindow(const NNodes::TExprBase node, TExprContext& ctx, const NNodes::TDqConnection& input);

} // namespace NYql::NDqs
