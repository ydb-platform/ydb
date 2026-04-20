#pragma once

#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/core/yql_window_frame_setting_bound.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/overloaded.h>

#include <utility>

namespace NYql::NWindow {

class TRangeFrameCollectorBounds: private TCoreWinFrameCollectorBounds<TWindowFrameSettingWithOffset, /*WithSortedColumnNames=*/true> {
    using TBase = TCoreWinFrameCollectorBounds<TWindowFrameSettingWithOffset, /*WithSortedColumnNames=*/true>;

public:
    using THandle = typename TBase::THandle;
    using TRangeFrame = TInputRangeWindowFrame<TWindowFrameSettingWithOffset>;
    using TRangeBound = TInputRange<TWindowFrameSettingWithOffset>;

    explicit TRangeFrameCollectorBounds() = default;

    THandle AddRange(const TRangeFrame& range, TSortColumnNamesView sortColumnNames) {
        if (auto* ptr = RangeIntervalsCache_.FindPtr(range)) {
            return *ptr;
        }
        THandle handle = TBase::AddRange(range, sortColumnNames);
        RangeIntervalsCache_.emplace(range, handle);
        return handle;
    }

    THandle AddRow(const TInputRowWindowFrame& row) {
        if (auto* ptr = RowIntervalsCache_.FindPtr(row)) {
            return *ptr;
        }
        THandle handle = TBase::AddRow(row);
        RowIntervalsCache_.emplace(row, handle);
        return handle;
    }

    THandle AddRangeIncremental(const TRangeBound& delta, TSortColumnNameView sortColumnName) {
        if (auto* ptr = RangeIncrementalsCache_.FindPtr(delta)) {
            return *ptr;
        }
        THandle handle = TBase::AddRangeIncremental(delta, sortColumnName);
        RangeIncrementalsCache_.emplace(delta, handle);
        return handle;
    }

    THandle AddRowIncremental(const TInputRow& delta) {
        if (auto* ptr = RowIncrementalsCache_.FindPtr(delta)) {
            return *ptr;
        }
        THandle handle = TBase::AddRowIncremental(delta);
        RowIncrementalsCache_.emplace(delta, handle);
        return handle;
    }

    // Expose read-only accessors from base class.
    using TBase::Empty;
    using TBase::RangeIncrementals;
    using TBase::RangeIntervals;
    using TBase::RowIncrementals;
    using TBase::RowIntervals;

    // Convert to base class (returns const reference to underlying TCoreWinFrameCollectorBounds).
    const TBase& AsBase() const {
        return static_cast<const TBase&>(*this);
    }

private:
    using TNumberAndDirectionHash = TNumberAndDirectionHash<TWindowFrameSettingWithOffset, TWindowFrameSettingOriginalBoundHash<>>;
    using TNumberAndDirectionComparator = TNumberAndDirectionComparator<TWindowFrameSettingWithOffset, TWindowFrameSettingOriginalBoundComparator<>>;
    using TWindowFrameHash = TWindowFrameHash<TWindowFrameSettingBound, TNumberAndDirectionHash>;
    using TWindowFrameComparator = TWindowFrameComparator<TWindowFrameSettingBound, TNumberAndDirectionComparator>;

    THashMap<
        TRangeFrame,
        THandle,
        TWindowFrameHash,
        TWindowFrameComparator>
        RangeIntervalsCache_;

    THashMap<
        TRangeBound,
        THandle,
        TNumberAndDirectionHash,
        TNumberAndDirectionComparator>
        RangeIncrementalsCache_;

    THashMap<TInputRowWindowFrame, THandle> RowIntervalsCache_;
    THashMap<TInputRow, THandle> RowIncrementalsCache_;
};

} // namespace NYql::NWindow
