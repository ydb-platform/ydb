#pragma once

#include <yql/essentials/minikql/comp_nodes/mkql_safe_circular_buffer.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_saturated_math.h>
#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>

#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/generic/scope.h>

#include <utility>
#include <algorithm>

namespace NKikimr::NMiniKQL {

using NYql::ESortOrder;
using NYql::NWindow::EDirection;
using NYql::NWindow::TCoreWinFrameCollectorBounds;
using NYql::NWindow::TCoreWinFramesCollectorParams;
using NYql::NWindow::TInputRange;
using NYql::NWindow::TInputRangeWindowFrame;
using NYql::NWindow::TInputRow;
using NYql::NWindow::TInputRowWindowFrame;
using NYql::NWindow::TRow;
using NYql::NWindow::TRowWindowFrame;

enum class EConsumeStatus {
    Ok,
    Wait,
    End,
};

inline IOutputStream& operator<<(IOutputStream& out, EConsumeStatus status) {
    switch (status) {
        case EConsumeStatus::Ok:
            return out << "Ok";
        case EConsumeStatus::Wait:
            return out << "Wait";
        case EConsumeStatus::End:
            return out << "End";
    }
}

template <typename TStream, typename TStreamElement>
class TStreamConsumer {
public:
    explicit TStreamConsumer(TStream stream)
        : Stream_(std::move(stream))
    {
    }

    void Consume(TStreamElement& elem) {
        LastConsumeStatus_ = Stream_(elem);
        switch (LastConsumedStatus()) {
            case EConsumeStatus::Ok:
                ConsumedElements_++;
                break;
            case EConsumeStatus::Wait:
            case EConsumeStatus::End:
                break;
        };
        return;
    }

    EConsumeStatus LastConsumedStatus() const {
        return LastConsumeStatus_;
    }

    bool AllElementsAreConsumed() const {
        return LastConsumedStatus() == EConsumeStatus::End;
    }

    TRow ConsumedElements() const {
        return ConsumedElements_;
    }

private:
    TStream Stream_;
    TRow ConsumedElements_ = 0;
    EConsumeStatus LastConsumeStatus_ = EConsumeStatus::Ok;
};

// Structure to hold current window states for all window types.
class TFrameBoundsIndices {
public:
    bool IsEmpty() const {
        return RangeIntervals_.empty() && RowIntervals_.empty() &&
               RangeIncrementals_.empty() && RowIncrementals_.empty();
    }

    TRowWindowFrame GetIntervalInQueueByRange(size_t idx) const {
        return RangeIntervals_.at(idx);
    }

    TRowWindowFrame GetIntervalInQueueByRow(size_t idx) const {
        return RowIntervals_.at(idx);
    }

    TRowWindowFrame GetIntervalInQueueByRangeIncremental(size_t idx) const {
        return RangeIncrementals_.at(idx);
    }

    TRowWindowFrame GetIntervalInQueueByRowIncremental(size_t idx) const {
        return RowIncrementals_.at(idx);
    }

private:
    template <typename, typename, ESortOrder>
    friend class TCoreWinFramesCollector;

    TVector<TRowWindowFrame> RangeIntervals_;
    TVector<TRowWindowFrame> RowIntervals_;
    TVector<TRowWindowFrame> RangeIncrementals_;
    TVector<TRowWindowFrame> RowIncrementals_;
};

// Manages window frames bounds collection over a stream of elements with queue-based buffering.
// Processes elements from a stream, maintains a circular buffer queue, and tracks multiple
// window frame intervals for operations.
//
// Capabilities:
// - Range-based windows: Define window bounds based on actual element values.
// - Row-based windows: Define window bounds based on row positions relative to current row.
// - Incremental mode: Track only the right boundary for incremental collection, initially empty until first element is tracked.
//                     When at least one element appears inside window bounds frame current state cannot become empty again.
// - Multiple simultaneous windows: Support multiple window specifications with different bounds in a single pass.
// - Automatic queue management: Efficiently removes elements no longer needed by any window frame.
// - Sort order awareness: Handles both ascending and descending sort orders.
//
// Example with 4 elements [10, 20, 30, 40]:
//
// Row-based window (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING):
//   Position 0 (value=10): window [0, 2) includes [10, 20]
//   Position 1 (value=20): window [0, 3) includes [10, 20, 30]
//   Position 2 (value=30): window [1, 4) includes [20, 30, 40]
//   Position 3 (value=40): window [2, 4) includes [30, 40]
//
// Range-based window (RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING):
//   Position 0 (value=10): window [0, 2) includes [10, 20]      // values in [-5, 25]
//   Position 1 (value=20): window [0, 3) includes [10, 20, 30]  // values in [5, 35]
//   Position 2 (value=30): window [1, 4) includes [20, 30, 40]  // values in [15, 45]
//   Position 3 (value=40): window [2, 4) includes [30, 40]      // values in [25, 55]
//
// Row incremental (1 FOLLOWING) - tracks only right boundary:
//   At position 0: incremental [1, 2) includes [20]
//   At position 1: incremental [2, 3) includes [30]
//   At position 2: incremental [3, 4) includes [40]
//   At position 3: incremental [3, 4) includes [40] // Since this is last available state for that boundary.
//
// Range incremental (15 FOLLOWING) - tracks only right boundary:
//   At position 0 (value=10): incremental [1, 2) includes [20]        // new values in (10, 25)
//   At position 1 (value=20): incremental [2, 3) includes [30]        // new values in (20, 35)
//   At position 2 (value=30): incremental [3, 4) includes [40]        // new values in (30, 45)
//   At position 3 (value=40): incremental [3, 4) includes [40]       // Since this is last available state for that boundary.
//
// Important: All indices in the examples above are relative to the original stream positions,
// not positions inside the currently maintained queue. TCoreWinFramesCollector automatically removes
// elements that are no longer needed by any window frame, causing indices to be rebased after each cleanup.
// So actual indices that methods return will be relative to the current queue state.
template <typename TStreamElement, typename TElementGetter, ESortOrder SortOrder>
class TCoreWinFramesCollector {
public:
    using TElementGetterResult = std::invoke_result_t<TElementGetter, const TStreamElement&>;
    using TRangeElement = typename TElementGetterResult::value_type;
    using TQueue = TSafeCircularBuffer<TStreamElement>;
    using TStream = std::function<EConsumeStatus(TStreamElement&)>;

    static auto CreateFactory(TCoreWinFrameCollectorBounds<TRangeElement> inputBounds,
                              TElementGetter elementGetter) {
        // Validate intervals
        for (const auto& interval : inputBounds.RangeIntervals()) {
            ValidateInterval(interval, "Range");
        }
        for (const auto& interval : inputBounds.RowIntervals()) {
            ValidateInterval(interval, "Row");
        }

        TPrecomputedBoundsData precomputed;
        precomputed.RangeWindowFrames = inputBounds.RangeIntervals();
        precomputed.RangeIncrementals = inputBounds.RangeIncrementals();
        precomputed.RowWindowFrames = ConvertRowIntervals(inputBounds.RowIntervals());
        precomputed.RowIncrementals = ConvertRowIntervals(inputBounds.RowIncrementals());

        if (!precomputed.RangeIncrementals.empty()) {
            auto maxIt = std::ranges::max_element(precomputed.RangeIncrementals);
            precomputed.MaxIncrementalRangeIntervals = std::distance(precomputed.RangeIncrementals.begin(), maxIt);
        }

        if (!precomputed.RowIncrementals.empty()) {
            auto maxIt = std::ranges::max_element(precomputed.RowIncrementals);
            precomputed.MaxIncrementalRowIntervals = std::distance(precomputed.RowIncrementals.begin(), maxIt);
        }

        if (!precomputed.RowWindowFrames.empty()) {
            auto minBegin = std::ranges::min_element(precomputed.RowWindowFrames,
                                                     [](const auto& a, const auto& b) {
                                                         return a.Min() < b.Min();
                                                     })
                                ->Min();
            auto maxEnd = std::ranges::max_element(precomputed.RowWindowFrames,
                                                   [](const auto& a, const auto& b) {
                                                       return a.Max() < b.Max();
                                                   })
                              ->Max();
            precomputed.MaxRowInterval = TRowWindowFrame(minBegin, maxEnd);
        }

        if (!precomputed.RangeWindowFrames.empty()) {
            auto minElement = std::ranges::min_element(precomputed.RangeWindowFrames,
                                                       [](const auto& a, const auto& b) {
                                                           return a.Min() < b.Min();
                                                       });
            auto maxElement = std::ranges::max_element(precomputed.RangeWindowFrames,
                                                       [](const auto& a, const auto& b) {
                                                           return a.Max() < b.Max();
                                                       });
            precomputed.MaxRangeInterval = TInputRangeWindowFrame<TRangeElement>{
                minElement->Min(),
                maxElement->Max()};
        }

        return [precomputed, elementGetter](TQueue& outputQueue, TStream stream, TFrameBoundsIndices& currentFrameBoundsIndices) {
            return TCoreWinFramesCollector(
                outputQueue,
                stream,
                elementGetter,
                currentFrameBoundsIndices,
                precomputed);
        };
    }

    EConsumeStatus Next()
    {
        MKQL_ENSURE(QueueGeneration_ == OutputQueue_.Generation(), "Unexpected change.");
        Y_DEFER {
            QueueGeneration_ = OutputQueue_.Generation();
        };
        if (!ConsumeStreamUntilAllIntervalsAreSatisfied(CurrentPositionInQueue_ + 1)) {
            return EConsumeStatus::Wait;
        };

        CurrentPositionInQueue_++;
        CurrentPositionInStream_++;
        if (StreamConsumer_.LastConsumedStatus() == EConsumeStatus::End && CurrentPositionInStream_ >= StreamConsumer_.ConsumedElements()) {
            return EConsumeStatus::End;
        }

        UpdateAllRowsIntervals();
        UpdateAllRowIncrementalsIntervals();
        if constexpr (IsRangeSupported()) {
            UpdateAllRangesIntervals();
            UpdateAllRangeIncrementalsIntervals();
        }

        RemoveAllReduntantElementsFromQueue();

        return EConsumeStatus::Ok;
    }

    void Clean() {
        MKQL_ENSURE(QueueGeneration_ == OutputQueue_.Generation(), "Unexpected change.");
        Y_DEFER {
            QueueGeneration_ = OutputQueue_.Generation();
        };
        OutputQueue_.Clean();
    }

    TStreamElement GetCurrentElement() const {
        return OutputQueue_.Get(CurrentPositionInQueue_);
    }

private:
    struct TPrecomputedBoundsData {
        TVector<TInputRangeWindowFrame<TRangeElement>> RangeWindowFrames;
        TVector<TRowWindowFrame> RowWindowFrames;
        TVector<TInputRange<TRangeElement>> RangeIncrementals;
        TVector<TRow> RowIncrementals;
        TMaybe<TRowWindowFrame> MaxRowInterval;
        TMaybe<TInputRangeWindowFrame<TRangeElement>> MaxRangeInterval;
        TMaybe<TRow> MaxIncrementalRangeIntervals;
        TMaybe<TRow> MaxIncrementalRowIntervals;
    };

    TCoreWinFramesCollector(TQueue& outputQueue,
                            TStream stream,
                            TElementGetter elementGetter,
                            TFrameBoundsIndices& currentFrameBoundsIndices,
                            const TPrecomputedBoundsData& precomputed)
        : OutputQueue_(outputQueue)
        , MaxRowInterval_(precomputed.MaxRowInterval)
        , MaxRangeInterval_(precomputed.MaxRangeInterval)
        , RangeWindowFrames_(precomputed.RangeWindowFrames)
        , RowWindowFrames_(precomputed.RowWindowFrames)
        , RangeIncrementals_(precomputed.RangeIncrementals)
        , RowIncrementals_(precomputed.RowIncrementals)
        , MaxIncrementalRangeIntervals_(precomputed.MaxIncrementalRangeIntervals)
        , MaxIncrementalRowIntervals_(precomputed.MaxIncrementalRowIntervals)
        , StreamConsumer_(std::move(stream))
        , ElementGetter_(std::move(elementGetter))
        , CurrentFrameBoundsIndices_(currentFrameBoundsIndices)
    {
        MKQL_ENSURE(CurrentFrameBoundsIndices_.IsEmpty(), "FrameBoundsIndices must be empty on construction.");
        MKQL_ENSURE(OutputQueue_.Size() == 0, "Queue must be empty.");
        QueueGeneration_ = OutputQueue_.Generation();

        CurrentFrameBoundsIndices_.RangeIntervals_.reserve(RangeWindowFrames_.size());
        CurrentFrameBoundsIndices_.RowIntervals_.reserve(RowWindowFrames_.size());
        CurrentFrameBoundsIndices_.RangeIncrementals_.reserve(RangeIncrementals_.size());
        CurrentFrameBoundsIndices_.RowIncrementals_.reserve(RowIncrementals_.size());

        for (size_t i = 0; i < RangeWindowFrames_.size(); ++i) {
            CurrentFrameBoundsIndices_.RangeIntervals_.emplace_back(TRowWindowFrame::CreateEmpty());
        }
        for (size_t i = 0; i < RowWindowFrames_.size(); ++i) {
            CurrentFrameBoundsIndices_.RowIntervals_.emplace_back(TRowWindowFrame::CreateEmpty());
        }
        for (size_t i = 0; i < RangeIncrementals_.size(); ++i) {
            CurrentFrameBoundsIndices_.RangeIncrementals_.emplace_back(TRowWindowFrame::CreateEmpty());
        }
        for (size_t i = 0; i < RowIncrementals_.size(); ++i) {
            CurrentFrameBoundsIndices_.RowIncrementals_.emplace_back(TRowWindowFrame::CreateEmpty());
        }
    }

    using TStreamConsumer = TStreamConsumer<TStream, TStreamElement>;

    TMaybe<TInputRange<TRangeElement>> GetMaxRangeIncrementalElement() const {
        if (MaxIncrementalRangeIntervals_.Defined()) {
            return RangeIncrementals_[*MaxIncrementalRangeIntervals_];
        }
        return {};
    }

    TMaybe<TRow> GetMaxRowIncrementalElement() const {
        if (MaxIncrementalRowIntervals_.Defined()) {
            return RowIncrementals_[*MaxIncrementalRowIntervals_];
        }
        return {};
    }

    template <typename TInterval>
    static void ValidateInterval(const TInterval& interval, const char* intervalType) {
        const auto& minBound = interval.Min();
        const auto& maxBound = interval.Max();

        if (!minBound.IsInf()) {
            MKQL_ENSURE(minBound.GetUnderlyingValue() >= 0,
                        TStringBuilder() << intervalType << " interval Min value must be positive");
        }
        if (minBound.IsInf() && minBound.GetDirection() != EDirection::Preceding) {
            MKQL_ENSURE(false, TStringBuilder() << intervalType << " interval Min cannot be Unbounded Right");
        }

        if (!maxBound.IsInf()) {
            MKQL_ENSURE(maxBound.GetUnderlyingValue() >= 0,
                        TStringBuilder() << intervalType << " interval Max value must be positive");
        }
        if (maxBound.IsInf() && maxBound.GetDirection() != EDirection::Following) {
            MKQL_ENSURE(false, TStringBuilder() << intervalType << " interval Max cannot be Unbounded Left");
        }
    }

    TMaybe<TRangeElement> GetQueueValue(TRow idx) const {
        return ElementGetter_(OutputQueue_.Get(idx));
    }

    consteval static EInfBoundary InfBoundaryAddElements() {
        static_assert(IsRangeSupported());
        if constexpr (SortOrder == ESortOrder::Asc) {
            return EInfBoundary::Left;
        } else {
            return EInfBoundary::Right;
        }
    }

    consteval static EInfBoundary InfBoundaryRemoveElements() {
        static_assert(IsRangeSupported());
        if constexpr (SortOrder == ESortOrder::Asc) {
            return EInfBoundary::Right;
        } else {
            return EInfBoundary::Left;
        }
    }

    template <typename T>
    constexpr static bool IsNan(T value) {
        if constexpr (std::is_floating_point_v<T>) {
            return std::isnan(value);
        } else {
            return false;
        }
    }

    bool ShouldAddElement(TInputRange<TRangeElement> range,
                          TRow fromIdx,
                          TRow elemToTestIdx) const {
        // Unbounded preceding or following case. Should be added no matter what.
        if (range.IsInf()) {
            return true;
        }

        auto from = GetQueueValue(fromIdx);
        auto elemToTest = GetQueueValue(elemToTestIdx);

        if (from.Defined() != elemToTest.Defined()) {
            return fromIdx > elemToTestIdx;
        }
        // Two empty optionals are inside same interval always. Since they are equal.
        if (!from.Defined() && !elemToTest.Defined()) {
            return true;
        }

        if (IsNan(*from) != IsNan(*elemToTest)) {
            return fromIdx > elemToTestIdx;
        }

        if (IsNan(*from) && IsNan(*elemToTest)) {
            return true;
        }

        // Just compare elements to match intervals.
        return IsBelongToInterval<InfBoundaryAddElements()>(GetComparationDirection(range.GetDirection()), *from, range.GetUnderlyingValue(), *elemToTest);
    }

    bool ShouldRemoveElement(TInputRange<TRangeElement> range,
                             TRow fromIdx,
                             TRow elemToTestIdx) const {
        // Unbounded preceding or following case. Should be added no matter what.
        if (range.IsInf()) {
            return false;
        }

        auto from = GetQueueValue(fromIdx);
        auto elemToTest = GetQueueValue(elemToTestIdx);
        if (from.Defined() != elemToTest.Defined()) {
            return fromIdx > elemToTestIdx;
        }

        // Two empty optionals are inside same interval always. Since they are equal.
        if (!from.Defined() && !elemToTest.Defined()) {
            return false;
        }

        if (IsNan(*from) != IsNan(*elemToTest)) {
            return fromIdx > elemToTestIdx;
        }

        if (IsNan(*from) && IsNan(*elemToTest)) {
            return false;
        }

        // Just compare elements to match intervals.
        return !IsBelongToInterval<InfBoundaryRemoveElements()>(GetComparationDirection(range.GetDirection()), *from, range.GetUnderlyingValue(), *elemToTest);
    }

    consteval static bool IsRangeSupported() {
        return SortOrder != ESortOrder::Unimportant;
    }

    static constexpr EDirection GetComparationDirection(EDirection dir) {
        if constexpr (SortOrder == ESortOrder::Asc) {
            return dir;
        } else {
            return InvertDirection(dir);
        }
    }

    // Consumes elements from the stream until all window frame intervals can be satisfied.
    // Checks if the queue has enough elements to cover all window specifications (both range and row based),
    // and fetches more elements from the stream if needed. Returns false if stream returns Wait status.
    bool ConsumeStreamUntilAllIntervalsAreSatisfied(TRow currentPositionInQueue) {
        while (ShouldAddNewElement(currentPositionInQueue)) {
            TStreamElement element;
            StreamConsumer_.Consume(element);
            if (StreamConsumer_.LastConsumedStatus() == EConsumeStatus::Wait) {
                return false;
            }

            if (StreamConsumer_.LastConsumedStatus() == EConsumeStatus::End) {
                break;
            }
            OutputQueue_.PushBack(std::move(element));
        }
        return true;
    };

    bool ShouldAddNewElement(TRow currentPositionInQueue) {
        if (StreamConsumer_.LastConsumedStatus() == EConsumeStatus::End) {
            return false;
        }

        if (currentPositionInQueue >= static_cast<TRow>(OutputQueue_.Size())) {
            return true;
        }

        auto maxIncrementalRow = GetMaxRowIncrementalElement();

        if (maxIncrementalRow && currentPositionInQueue + *maxIncrementalRow >= static_cast<TRow>(OutputQueue_.Size())) {
            return true;
        }

        if (MaxRowInterval_ && currentPositionInQueue + MaxRowInterval_->Max() >= static_cast<TRow>(OutputQueue_.Size())) {
            return true;
        }

        if constexpr (IsRangeSupported()) {
            if (MaxRangeInterval_ && ShouldAddElement(MaxRangeInterval_->Max(),
                                                      currentPositionInQueue,
                                                      OutputQueue_.Size() - 1)) {
                return true;
            }

            auto maxIncrementalRange = GetMaxRangeIncrementalElement();

            if (maxIncrementalRange && ShouldAddElement(
                                           *maxIncrementalRange,
                                           currentPositionInQueue,
                                           OutputQueue_.Size() - 1)) {
                return true;
            }
        }

        return false;
    };

    // Removes elements from the queue that are no longer needed by any window frame.
    // Finds the minimum index still required across all active windows and removes all elements
    // before that index. Updates all interval indices to reflect the new queue state after removal.
    void RemoveAllReduntantElementsFromQueue() {
        TRow removedElements = std::numeric_limits<TRow>::max();
        for (const auto* vector : GetAllFrameBoundsIndices()) {
            for (const auto& interval : *vector) {
                removedElements = std::min(removedElements, interval.Min());
            }
        }

        removedElements = std::min(removedElements, CurrentPositionInQueue_);

        Y_ENSURE(removedElements != std::numeric_limits<TRow>::max());

        for (TRow i = 0; i < removedElements; i++) {
            OutputQueue_.PopFront();
        }

        CurrentPositionInQueue_ -= removedElements;

        for (auto& vector : GetAllFrameBoundsIndices()) {
            for (auto& interval : *vector) {
                interval.Min() = interval.Min() - removedElements;
                interval.Max() = std::clamp(interval.Max() - removedElements, TRowWindowFrame::TBoundType(0), std::numeric_limits<TRowWindowFrame::TBoundType>::max());
            }
        }
    };

    // Updates all range-based window frame intervals for the current position.
    // Expands or contracts interval boundaries based on element values to match the specified
    // RANGE BETWEEN conditions.
    void UpdateAllRangesIntervals()
        requires(IsRangeSupported())
    {
        for (size_t idx = 0; idx < RangeWindowFrames_.size(); idx++) {
            const auto& interval = RangeWindowFrames_[idx];
            auto& currentFrame = CurrentFrameBoundsIndices_.RangeIntervals_[idx];

            while (currentFrame.Max() < static_cast<TRow>(OutputQueue_.Size()) &&
                   ShouldAddElement(interval.Max(),
                                    CurrentPositionInQueue_,
                                    currentFrame.Max())) {
                currentFrame = TRowWindowFrame(currentFrame.Min(), currentFrame.Max() + 1);
            }

            while (currentFrame.Min() < static_cast<TRow>(OutputQueue_.Size()) &&
                   ShouldRemoveElement(interval.Min(),
                                       CurrentPositionInQueue_,
                                       currentFrame.Min())) {
                currentFrame = TRowWindowFrame(currentFrame.Min() + 1, currentFrame.Max());
            }
        }
    }

    // Updates all range-based incremental intervals for the current position.
    // Incremental mode tracks only the right boundary.
    void UpdateAllRangeIncrementalsIntervals()
        requires(IsRangeSupported())
    {
        for (size_t idx = 0; idx < RangeIncrementals_.size(); idx++) {
            const auto& incremental = RangeIncrementals_[idx];
            auto currentFrameCopy = CurrentFrameBoundsIndices_.RangeIncrementals_[idx];

            while (currentFrameCopy.Max() < static_cast<TRow>(OutputQueue_.Size()) &&
                   ShouldAddElement(incremental,
                                    CurrentPositionInQueue_,
                                    currentFrameCopy.Max())) {
                currentFrameCopy.Max()++;
            }
            if (!currentFrameCopy.Empty()) {
                // Store only last element.
                CurrentFrameBoundsIndices_.RangeIncrementals_[idx] = TRowWindowFrame(currentFrameCopy.Max() - 1, currentFrameCopy.Max());
            } else {
                CurrentFrameBoundsIndices_.RangeIncrementals_[idx] = TRowWindowFrame(currentFrameCopy.Max(), currentFrameCopy.Max());
            }
        }
    }

    // Updates all row-based incremental intervals for the current position.
    // Incremental mode tracks only the right boundary.
    void UpdateAllRowIncrementalsIntervals() {
        for (size_t idx = 0; idx < RowIncrementals_.size(); ++idx) {
            auto& rowInterval = CurrentFrameBoundsIndices_.RowIncrementals_[idx];
            const auto& rowIncremental = RowIncrementals_[idx];
            TRow maxPos = CurrentPositionInQueue_ + rowIncremental + 1;
            maxPos = ClampToQueue(maxPos);
            if (maxPos > 0) {
                rowInterval = TRowWindowFrame(maxPos - 1, maxPos);
            } else {
                rowInterval = TRowWindowFrame::CreateEmpty();
            }
        }
    }

    // Updates all row-based window frame intervals for the current position.
    // Calculates interval boundaries based on row positions relative to the current row,
    // implementing ROWS BETWEEN conditions. Simply adds/subtracts row offsets from current position.
    void UpdateAllRowsIntervals() {
        for (size_t idx = 0; idx < RowWindowFrames_.size(); ++idx) {
            const auto& rowInterval = RowWindowFrames_[idx];
            auto& rowWindow = CurrentFrameBoundsIndices_.RowIntervals_[idx];

            TRow minPos = CurrentPositionInQueue_ + rowInterval.Min();
            TRow maxPos = CurrentPositionInQueue_ + rowInterval.Max() + 1;

            rowWindow = TRowWindowFrame(ClampToQueue(minPos), ClampToQueue(maxPos));
        }
    }

    TRow ClampToQueue(TRow value) {
        return std::clamp(value, TRow(0), static_cast<TRow>(OutputQueue_.Size()));
    }

    // Converts TInputRow (with direction and optional unbounded) to signed |TRow| offset.
    // UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING could be represented with max integer value,
    // but we use a large enough bound (max/2) to avoid overflows when adding with other values.
    // Left direction (PRECEDING) produces negative offsets, Right direction (FOLLOWING) produces positive offsets.
    static TRow ConvertRowValue(const TInputRow& input) {
        auto clampToValid = [](auto input) {
            using T = decltype(input);
            return std::clamp(input, T(0), T(std::numeric_limits<TRowWindowFrame::TBoundType>::max()) / 2);
        };

        TRowWindowFrame::TBoundType value = input.IsInf() ? std::numeric_limits<TRowWindowFrame::TBoundType>::max() : clampToValid(input.GetUnderlyingValue());
        value = clampToValid(value);
        return (input.GetDirection() == EDirection::Preceding) ? -value : value;
    }

    static TVector<TRowWindowFrame> ConvertRowIntervals(const TVector<TInputRowWindowFrame>& inputIntervals) {
        TVector<TRowWindowFrame> result;
        result.reserve(inputIntervals.size());

        for (const auto& interval : inputIntervals) {
            result.emplace_back(ConvertRowValue(interval.Min()),
                                ConvertRowValue(interval.Max()));
        }

        return result;
    }

    static TVector<TRow> ConvertRowIntervals(const TVector<TInputRow>& inputIntervals) {
        TVector<TRow> result;
        result.reserve(inputIntervals.size());

        for (const auto& interval : inputIntervals) {
            result.emplace_back(ConvertRowValue(interval));
        }

        return result;
    }

    std::array<TVector<TRowWindowFrame>*, 4> GetAllFrameBoundsIndices() {
        return {&CurrentFrameBoundsIndices_.RangeIntervals_, &CurrentFrameBoundsIndices_.RowIntervals_, &CurrentFrameBoundsIndices_.RangeIncrementals_, &CurrentFrameBoundsIndices_.RowIncrementals_};
    }

    TQueue& OutputQueue_;

    TRow CurrentPositionInQueue_ = -1;
    TRow CurrentPositionInStream_ = -1;

    TMaybe<TRowWindowFrame> MaxRowInterval_;
    TMaybe<TInputRangeWindowFrame<TRangeElement>> MaxRangeInterval_;

    TVector<TInputRangeWindowFrame<TRangeElement>> RangeWindowFrames_;
    TVector<TRowWindowFrame> RowWindowFrames_;

    TVector<TInputRange<TRangeElement>> RangeIncrementals_;
    TVector<TRow> RowIncrementals_;

    TMaybe<TRow> MaxIncrementalRangeIntervals_;
    TMaybe<TRow> MaxIncrementalRowIntervals_;

    TStreamConsumer StreamConsumer_;
    TElementGetter ElementGetter_;

    ui64 QueueGeneration_ = 0;
    TFrameBoundsIndices& CurrentFrameBoundsIndices_;
};

} // namespace NKikimr::NMiniKQL
