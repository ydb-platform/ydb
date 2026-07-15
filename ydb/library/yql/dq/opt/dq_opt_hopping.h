#pragma once

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NYql::NDq::NHopping {

NNodes::TMaybeNode<NNodes::TExprBase> RewriteAsHoppingWindow(
    const NNodes::TExprBase node,
    TExprContext& ctx,
    const TOptimizeTransformerBase::TGetParents& getParents,
    const NNodes::TDqConnection& input,
    bool analyticsHopping,
    TDuration lateArrivalDelay,
    bool defaultWatermarksMode,
    TMaybe<NYql::NHoppingWindow::EPolicy> defaultLatePolicy = Nothing()
);

} // namespace NYql::NDq::NHopping
