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

    T& Min() {
        return Min_;
    }

    T& Max() {
        return Max_;
    }

    bool operator==(const TWindowFrame&) const = default;

private:
    T Min_;
    T Max_;
};

// Hash function for TWindowFrame.
template <typename T>
struct TWindowFrameHash {
    size_t operator()(const TWindowFrame<T>& frame) const {
        TNumberAndDirectionHash<typename T::TNumberType> boundHash;
        size_t hash = boundHash(frame.Min());
        hash = CombineHashes(hash, boundHash(frame.Max()));
        return hash;
    }
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
template <typename TRangeElement>
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

    THandle AddRange(const TInputRangeWindowFrame<TRangeElement>& range) {
        RangeIntervals_.push_back(range);
        return THandle(RangeIntervals_.size() - 1, /*isRange=*/true, /*isIncremental=*/false);
    }

    THandle AddRow(const TInputRowWindowFrame& row) {
        RowIntervals_.push_back(row);
        return THandle(RowIntervals_.size() - 1, /*isRange=*/false, /*isIncremental=*/false);
    }

    THandle AddRangeIncremental(const TInputRange<TRangeElement>& delta) {
        RangeIncrementals_.push_back(delta);
        return THandle(RangeIncrementals_.size() - 1, /*isRange=*/true, /*isIncremental=*/true);
    }

    THandle AddRowIncremental(const TInputRow& delta) {
        RowIncrementals_.push_back(delta);
        return THandle(RowIncrementals_.size() - 1, /*isRange=*/false, /*isIncremental=*/true);
    }

    const TVector<TInputRangeWindowFrame<TRangeElement>>& RangeIntervals() const {
        return RangeIntervals_;
    }

    const TVector<TInputRowWindowFrame>& RowIntervals() const {
        return RowIntervals_;
    }

    const TVector<TInputRange<TRangeElement>>& RangeIncrementals() const {
        return RangeIncrementals_;
    }

    const TVector<TInputRow>& RowIncrementals() const {
        return RowIncrementals_;
    }

    bool Empty() const {
        return RangeIntervals_.empty() && RowIntervals_.empty() && RangeIncrementals_.empty() && RowIncrementals_.empty();
    }

private:
    TVector<TInputRangeWindowFrame<TRangeElement>> RangeIntervals_;
    TVector<TInputRowWindowFrame> RowIntervals_;
    TVector<TInputRange<TRangeElement>> RangeIncrementals_;
    TVector<TInputRow> RowIncrementals_;
};

} // namespace NYql::NWindow

template <typename T>
struct THash<NYql::NWindow::TWindowFrame<T>>: NYql::NWindow::TWindowFrameHash<T> {};
