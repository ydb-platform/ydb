#pragma once

#include "window_number_and_direction.h"

#include <util/generic/overloaded.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NYql::NWindow {

template <typename T>
using TInputRange = TNumberAndDirection<T>;

using TInputRow = TNumberAndDirection<ui64>;

template <typename T>
class TWindowFrame {
public:
    using TBoundType = T;

    TWindowFrame(T min, T max)
        : Min_(std::move(min))
        , Max_(std::move(max))
    {
    }

    const T& Min() const {
        return Min_;
    }

    const T& Max() const {
        return Max_;
    }

    bool operator==(const TWindowFrame&) const = default;

private:
    T Min_;
    T Max_;
};

// Hash function for TWindowFrame.
template <typename T, typename THash = THash<typename TWindowFrame<T>::TBoundType>>
class TWindowFrameHash {
public:
    explicit TWindowFrameHash(THash boundHash = THash())
        : BoundHash_(std::move(boundHash))
    {
    }

    size_t operator()(const TWindowFrame<T>& frame) const {
        size_t hash = BoundHash_(frame.Min());
        hash = CombineHashes(hash, BoundHash_(frame.Max()));
        return hash;
    }

private:
    THash BoundHash_;
};

template <typename T, typename TComparator = TEqualTo<typename TWindowFrame<T>::TBoundType>>
class TWindowFrameComparator {
public:
    explicit TWindowFrameComparator(TComparator comparator = TComparator{})
        : Comparator_(comparator)
    {
    }

    bool operator()(const TWindowFrame<T>& left, const TWindowFrame<T>& right) const {
        return Comparator_(left.Min(), right.Min()) && Comparator_(left.Max(), right.Max());
    }

private:
    TComparator Comparator_;
};

template <typename T>
using TInputRangeWindowFrame = TWindowFrame<TInputRange<T>>;

using TInputRowWindowFrame = TWindowFrame<TInputRow>;

using TRow = i64;

class TRowWindowFrame: public TWindowFrame<TRow> {
    using Base = TWindowFrame<TRow>;

public:
    using Base::operator=;
    using Base::Base;

    TRow Size() const {
        if (Max() >= Min()) {
            return Max() - Min();
        } else {
            return 0;
        }
    }

    bool Empty() const {
        return Size() == 0;
    }
    static TRowWindowFrame CreateEmpty() {
        return TRowWindowFrame(0, 0);
    }
};

// Container for window frame bounds specifications.
// Stores multiple window frame intervals and delta specifications that define
// how window frames should be calculated for aggregation operations.
//
// Supports both range-based (value-based) and row-based (position-based) window frames.
// Each bound can be either a finite value or unbounded (infinity), allowing specifications like:
// - ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
// - RANGE BETWEEN 10 PRECEDING AND 5 FOLLOWING
// - ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING
//
// Unbounded values are represented using TNumberAndDirection with TUnbounded type,
// indicating infinite extent in the specified direction (Left for PRECEDING, Right for FOLLOWING).
template <typename TRangeElement, bool WithSortedColumnNames>
class TCoreWinFrameCollectorBounds {
public:
    // Handle that identifies a window frame specification.
    class THandle {
    public:
        THandle(size_t index, bool isRange, bool isIncremental)
            : Index_(index)
            , IsRange_(isRange)
            , IsIncremental_(isIncremental)
        {
        }

        size_t Index() const {
            return Index_;
        }

        bool IsRange() const {
            return IsRange_;
        }

        bool IsIncremental() const {
            return IsIncremental_;
        }

    private:
        const size_t Index_;
        const bool IsRange_;
        const bool IsIncremental_;
    };

    TCoreWinFrameCollectorBounds() = default;

    using TSortColumnName = TString;
    using TSortColumnNameView = TStringBuf;

    using TSortColumnNames = std::pair<TSortColumnName, TSortColumnName>;
    using TSortColumnNamesView = std::pair<TSortColumnNameView, TSortColumnNameView>;

    using TRangeInterval = std::conditional_t<WithSortedColumnNames, std::pair<TInputRangeWindowFrame<TRangeElement>, TSortColumnNames>, TInputRangeWindowFrame<TRangeElement>>;
    using TRangeIncremental = std::conditional_t<WithSortedColumnNames, std::pair<TInputRange<TRangeElement>, TSortColumnName>, TInputRange<TRangeElement>>;

    THandle AddRange(TInputRangeWindowFrame<TRangeElement> range, TSortColumnNamesView sortColumnNames)
        requires WithSortedColumnNames
    {
        RangeIntervals_.push_back({std::move(range), TSortColumnNames(sortColumnNames.first, sortColumnNames.second)});
        return THandle(RangeIntervals_.size() - 1, /*isRange=*/true, /*isIncremental=*/false);
    }

    THandle AddRange(TInputRangeWindowFrame<TRangeElement> range)
        requires(!WithSortedColumnNames)
    {
        RangeIntervals_.push_back(std::move(range));
        return THandle(RangeIntervals_.size() - 1, /*isRange=*/true, /*isIncremental=*/false);
    }

    THandle AddRow(const TInputRowWindowFrame& row) {
        RowIntervals_.push_back(row);
        return THandle(RowIntervals_.size() - 1, /*isRange=*/false, /*isIncremental=*/false);
    }

    THandle AddRangeIncremental(TInputRange<TRangeElement> delta, TSortColumnNameView sortColumnName)
        requires WithSortedColumnNames
    {
        RangeIncrementals_.push_back({std::move(delta), TSortColumnName(sortColumnName)});
        return THandle(RangeIncrementals_.size() - 1, /*isRange=*/true, /*isIncremental=*/true);
    }

    THandle AddRangeIncremental(TInputRange<TRangeElement> delta)
        requires(!WithSortedColumnNames)
    {
        RangeIncrementals_.push_back(std::move(delta));
        return THandle(RangeIncrementals_.size() - 1, /*isRange=*/true, /*isIncremental=*/true);
    }

    THandle AddRowIncremental(const TInputRow& delta) {
        RowIncrementals_.push_back(delta);
        return THandle(RowIncrementals_.size() - 1, /*isRange=*/false, /*isIncremental=*/true);
    }

    const auto& RangeIntervals() const {
        return RangeIntervals_;
    }

    const TVector<TInputRowWindowFrame>& RowIntervals() const {
        return RowIntervals_;
    }

    const auto& RangeIncrementals() const {
        return RangeIncrementals_;
    }

    const TVector<TInputRow>& RowIncrementals() const {
        return RowIncrementals_;
    }

    bool Empty() const {
        return RangeIntervals_.empty() && RowIntervals_.empty() && RangeIncrementals_.empty() && RowIncrementals_.empty();
    }

private:
    TVector<TRangeInterval> RangeIntervals_;
    TVector<TInputRowWindowFrame> RowIntervals_;
    TVector<TRangeIncremental> RangeIncrementals_;
    TVector<TInputRow> RowIncrementals_;
};

} // namespace NYql::NWindow

template <typename T>
struct THash<NYql::NWindow::TWindowFrame<T>>: NYql::NWindow::TWindowFrameHash<T> {};
