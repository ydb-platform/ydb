#pragma once

#include "window_frame_bounds.h"

#include <yql/essentials/core/sql_types/sort_order.h>

#include <utility>

namespace NYql::NWindow {

template <typename TRangeType, bool WithSortedColumnNames>
class TCoreWinFramesCollectorParams {
public:
    TCoreWinFramesCollectorParams(TCoreWinFrameCollectorBounds<TRangeType, WithSortedColumnNames> bounds, ESortOrder sortOrder)
        : Bounds_(std::move(bounds))
        , SortOrder_(sortOrder)
    {
    }

    const TCoreWinFrameCollectorBounds<TRangeType, WithSortedColumnNames>& GetBounds() const {
        return Bounds_;
    }

    ESortOrder GetSortOrder() const {
        return SortOrder_;
    }

private:
    TCoreWinFrameCollectorBounds<TRangeType, WithSortedColumnNames> Bounds_;
    ESortOrder SortOrder_;
};

} // namespace NYql::NWindow
