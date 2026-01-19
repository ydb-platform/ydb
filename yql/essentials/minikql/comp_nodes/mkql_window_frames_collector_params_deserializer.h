#pragma once

#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

NYql::ESortOrder DeserializeSortOrder(const TRuntimeNode& node);

TString DeserializeSortColumnName(const TRuntimeNode& node);

template <typename TRangeType>
NYql::NWindow::TCoreWinFrameCollectorBounds<TRangeType> DeserializeBounds(const TRuntimeNode& node);

TDataType* ExtractRangeDataTypeFromWindowAggregatorParams(const TRuntimeNode& node);

} // namespace NKikimr::NMiniKQL
