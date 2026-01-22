#pragma once

#include <yql/essentials/core/sql_types/window_frames_collector_params.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NWindow {

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TStringCoreWinFramesCollectorParams& params,
    TPositionHandle pos,
    TStringBuf rangeCallableName,
    TExprContext& ctx);

} // namespace NYql::NWindow
