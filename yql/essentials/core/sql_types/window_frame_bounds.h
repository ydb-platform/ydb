#pragma once

#include "window_number_and_direction.h"

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

// Hash function for TNumberAndDirection.
template <typename T>
struct TNumberAndDirectionHash {
    size_t operator()(const TNumberAndDirection<T>& value) const {
        size_t hash = 0;
        hash = CombineHashes(hash, THash<int>{}(static_cast<int>(value.GetDirection())));
        hash = CombineHashes(hash, THash<bool>{}(value.IsInf()));
        if (!value.IsInf()) {
            hash = CombineHashes(hash, THash<T>{}(value.GetUnderlyingValue()));
        }
        return hash;
    }
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

        size_t Index() {
            return Index_;
        }

        bool IsRange() {
            return IsRange_;
        }

        bool IsIncremental() {
            return IsIncremental_;
        }

    private:
        const size_t Index_;
        const bool IsRange_;
        const bool IsIncremental_;
    };

    explicit TCoreWinFrameCollectorBounds(bool dedup)
        : Dedup_(dedup)
    {
    }

    THandle AddRange(const TInputRangeWindowFrame<TRangeElement>& range) {
        auto it = RangeIntervalsCache_.find(range);
        if (Dedup_ && it != RangeIntervalsCache_.end()) {
            return it->second;
        }
        RangeIntervals_.push_back(range);
        THandle handle(RangeIntervals_.size() - 1, /*isRange=*/true, /*isIncremental=*/false);
        RangeIntervalsCache_.emplace(range, handle);
        return handle;
    }

    THandle AddRow(const TInputRowWindowFrame& row) {
        auto it = RowIntervalsCache_.find(row);
        if (Dedup_ && it != RowIntervalsCache_.end()) {
            return it->second;
        }
        RowIntervals_.push_back(row);
        THandle handle(RowIntervals_.size() - 1, /*isRange=*/false, /*isIncremental=*/false);
        RowIntervalsCache_.emplace(row, handle);
        return handle;
    }

    THandle AddRangeIncremental(const TInputRange<TRangeElement>& delta) {
        auto it = RangeIncrementalsCache_.find(delta);
        if (Dedup_ && it != RangeIncrementalsCache_.end()) {
            return it->second;
        }
        RangeIncrementals_.push_back(delta);
        THandle handle(RangeIncrementals_.size() - 1, /*isRange=*/true, /*isIncremental=*/true);
        RangeIncrementalsCache_.emplace(delta, handle);
        return handle;
    }

    THandle AddRowIncremental(const TInputRow& delta) {
        auto it = RowIncrementalsCache_.find(delta);
        if (Dedup_ && it != RowIncrementalsCache_.end()) {
            return it->second;
        }
        RowIncrementals_.push_back(delta);
        THandle handle(RowIncrementals_.size() - 1, /*isRange=*/false, /*isIncremental=*/true);
        RowIncrementalsCache_.emplace(delta, handle);
        return handle;
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

    // Caches for deduplication.
    bool Dedup_;
    THashMap<TInputRangeWindowFrame<TRangeElement>, THandle, TWindowFrameHash<TInputRange<TRangeElement>>> RangeIntervalsCache_;
    THashMap<TInputRowWindowFrame, THandle, TWindowFrameHash<TInputRow>> RowIntervalsCache_;
    THashMap<TInputRange<TRangeElement>, THandle, TNumberAndDirectionHash<TRangeElement>> RangeIncrementalsCache_;
    THashMap<TInputRow, THandle, TNumberAndDirectionHash<TInputRow::TNumberType>> RowIncrementalsCache_;
};

} // namespace NYql::NWindow
