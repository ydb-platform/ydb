#pragma once

#include <yql/essentials/core/yql_window_frame_settings.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NWindow {

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TExprNodeCoreWinFrameCollectorParams& params,
    TPositionHandle pos,
    TExprContext& ctx);

} // namespace NYql::NWindow
