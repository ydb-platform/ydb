#pragma once

// Common type aliases for window frame processing in MiniKQL.
// This header provides a single place for all window-related type imports
// from NYql::NWindow namespace into NKikimr::NMiniKQL namespace.

#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>
#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>

namespace NKikimr::NMiniKQL {

using NYql::ESortOrder;
using NYql::NWindow::EDirection;
using NYql::NWindow::TCoreWinFrameCollectorBounds;
using NYql::NWindow::TCoreWinFramesCollectorParams;
using NYql::NWindow::TInputRange;
using NYql::NWindow::TInputRangeWindowFrame;
using NYql::NWindow::TInputRow;
using NYql::NWindow::TInputRowWindowFrame;
using NYql::NWindow::TNumberAndDirection;
using NYql::NWindow::TRow;
using NYql::NWindow::TRowWindowFrame;
using NYql::NWindow::TWindowFrame;

} // namespace NKikimr::NMiniKQL
