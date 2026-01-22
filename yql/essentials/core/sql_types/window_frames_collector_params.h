#pragma once

#include "window_frame_bounds.h"

#include <yql/essentials/core/sql_types/sort_order.h>

namespace NYql::NWindow {

template <typename TRangeType>
class TCoreWinFramesCollectorParams {
public:
    TCoreWinFramesCollectorParams(TCoreWinFrameCollectorBounds<TRangeType> bounds, ESortOrder sortOrder, const TString& sortColumnName)
        : Bounds_(std::move(bounds))
        , SortOrder_(sortOrder)
        , SortColumnName_(sortColumnName)
    {
    }

    const TCoreWinFrameCollectorBounds<TRangeType>& GetBounds() const {
        return Bounds_;
    }

    ESortOrder GetSortOrder() const {
        return SortOrder_;
    }

    TStringBuf GetSortColumnName() const {
        return SortColumnName_;
    }

private:
    TCoreWinFrameCollectorBounds<TRangeType> Bounds_;
    ESortOrder SortOrder_;
    TString SortColumnName_;
};

using TStringCoreWinFramesCollectorParams = TCoreWinFramesCollectorParams<TString>;

} // namespace NYql::NWindow
