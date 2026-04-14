#pragma once

#include <yql/essentials/minikql/mkql_core_win_frames_collector.h>
#include <yql/essentials/minikql/mkql_core_window_frames_collector_params_deserializer.h>
#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/fwd.h>

namespace NKikimr::NMiniKQL::NTest::NWindow {

struct TYield {};

struct TEmptyInterval {};

struct TIntervalCheck {
    i64 ExpectedMin;
    i64 ExpectedMax;
};

struct TIntervalCheckOrEmpty: std::variant<TIntervalCheck, TEmptyInterval> {
    using TBase = std::variant<TIntervalCheck, TEmptyInterval>;
    using TBase::TBase;
    using TBase::operator=;

    // Allow brace initialization like {{0, 3}}.
    TIntervalCheckOrEmpty(std::initializer_list<i64> init) {
        Y_ENSURE(init.size() == 2);
        auto it = init.begin();
        i64 min = *it++;
        i64 max = *it;
        *this = TIntervalCheck{.ExpectedMin = min, .ExpectedMax = max};
    }
};

struct TFakeContext {};

template <typename TElement>
using TTypeTestWithScale = TRangeTypeWithScale<TFakeContext, TElement>;

template <typename TElement>
using TColumnTypeTestWithScale = TColumnTypeWithScale<TFakeContext, TElement>;

template <typename TElement, ESortOrder SortOrder>
struct TTestCase {
    static_assert(false, "TMaybe must be specialized as TElement");
};

template <typename TInputType, ESortOrder SortOrder>
struct TTestCase<TMaybe<TInputType>, SortOrder> {
    using TElement = TMaybe<TInputType>;
    using TColumnTypeWithScale = TColumnTypeTestWithScale<TElement>;
    using TRangeTypeWithScale = TTypeTestWithScale<TElement>;
    using TVariantBounds = TVariantBounds<TFakeContext, TElement>;
    using TVariantBound = TVariantBound<TFakeContext, TElement>;
    using TRangeBound = TRangeBound<TFakeContext, TElement>;
    using TRangeElement = TInputType;

    // Input interval bounds. Requires two: left and right.
    TVector<TInputRowWindowFrame> RowIntervals;
    TVector<TInputRangeWindowFrame<TRangeTypeWithScale>> RangeIntervals;

    // Incremental bounds - for tracking only right boundary.
    TVector<TInputRange<TRangeTypeWithScale>> RangeIncrementals;
    TVector<TInputRow> RowIncrementals;

    TVector<std::variant<TYield, TElement>> InputElements;

    // Expected results after each Next() call.
    struct TExpectedState {
        TElement CurrentElement;
        TVector<TElement> QueueContent;

        TVector<TIntervalCheckOrEmpty> RowIntervalChecks;
        TVector<TIntervalCheckOrEmpty> RangeIntervalChecks;
        TVector<TIntervalCheckOrEmpty> RangeIncrementalChecks;
        TVector<TIntervalCheckOrEmpty> RowIncrementalChecks;
    };

    TVector<TExpectedState> ExpectedStates;
};

template <typename TElement>
bool ElementsEqual(const TMaybe<TElement>& a, const TMaybe<TElement>& b) {
    if constexpr (std::is_floating_point_v<TElement>) {
        if (a.Defined() && b.Defined() && std::isnan(*a) && std::isnan(*b)) {
            return true;
        }
    }
    return a == b;
}

template <typename TElement>
TString FormatQueueContent(const TSafeCircularBuffer<TElement>& queue) {
    TStringBuilder result;
    result << "[";
    for (size_t i = 0; i < queue.Size(); ++i) {
        if (i > 0) {
            result << ", ";
        }
        result << queue.Get(i);
    }
    result << "]";
    return result;
}

// Helper function to run a single test case.
template <typename TElement, ESortOrder SortOrder>
void RunTestCase(const TTestCase<TElement, SortOrder>& testCase) {
    using TVariantBounds = TTestCase<TElement, SortOrder>::TVariantBounds;
    using TVariantBound = TTestCase<TElement, SortOrder>::TVariantBound;
    using TRangeBound = TTestCase<TElement, SortOrder>::TRangeBound;
    using TColumnTypeWithScale = TTestCase<TElement, SortOrder>::TColumnTypeWithScale;
    using TRangeTypeWithScale = TTestCase<TElement, SortOrder>::TRangeTypeWithScale;
    using TRangeElement = TTestCase<TElement, SortOrder>::TRangeElement;

    // Setup variant bounds first.
    TVariantBounds variantBounds;
    auto makeVariantBound = [](const TInputRange<TRangeTypeWithScale>& bound) -> TVariantBound {
        if (bound.IsInf()) {
            return TVariantBound::Inf(bound.GetDirection());
        }
        const auto zeroColumn = TColumnTypeWithScale(TNoScaledType<TRangeElement>{0});
        auto boundValue = TRangeTypeWithScale(bound.GetUnderlyingValue());
        return TVariantBound(TRangeBound(std::move(boundValue), std::move(zeroColumn), 0), bound.GetDirection());
    };
    for (const auto& interval : testCase.RowIntervals) {
        variantBounds.AddRow(interval);
    }
    for (const auto& interval : testCase.RangeIntervals) {
        TInputRangeWindowFrame<TRangeBound> frame(makeVariantBound(interval.Min()), makeVariantBound(interval.Max()));
        variantBounds.AddRange(std::move(frame));
    }
    for (const auto& incremental : testCase.RangeIncrementals) {
        variantBounds.AddRangeIncremental(makeVariantBound(incremental));
    }
    for (const auto& incremental : testCase.RowIncrementals) {
        variantBounds.AddRowIncremental(incremental);
    }

    auto memberExtractor = [](const TElement& value, ui32 memberIndex) {
        Y_UNUSED(memberIndex);
        return value;
    };

    auto nullChecker = [](const TElement& value) -> bool {
        return !value.Defined();
    };

    auto elementExtractor =
        []<typename T>(const TElement& value) -> T {
        return *value;
    };

    TFakeContext fakeContext;
    TDeserializerContext deserializerContext(memberExtractor, nullChecker, elementExtractor);
    // Convert variant bounds to comparator bounds.
    auto comparatorBounds = ConvertBoundsToComparators<TElement, TElement, TFakeContext>(variantBounds, SortOrder, deserializerContext);

    // Create queue and stream (unbounded buffer).
    TSafeCircularBuffer<TElement> outputQueue(TMaybe<size_t>(), TElement{});

    size_t inputIdx = 0;
    auto stream = [&](TElement& elem) -> EConsumeStatus {
        if (inputIdx >= testCase.InputElements.size()) {
            return EConsumeStatus::End;
        }
        const auto& input = testCase.InputElements[inputIdx++];
        if (std::holds_alternative<TYield>(input)) {
            return EConsumeStatus::Wait;
        }
        elem = std::get<TElement>(input);
        return EConsumeStatus::Ok;
    };

    // Create current windows structure.
    TFrameBoundsIndices currentWindows;

    // Create aggregator using factory method.
    // clang-format off
    auto factory = TCoreWinFramesCollector<TElement, TFakeContext, SortOrder != ESortOrder::Unimportant>::CreateFactory(comparatorBounds);
    // clang-format on
    // TQueue& outputQueue, TStream stream, TFrameBoundsIndices& currentFrameBoundsIndices, TContext& context
    auto aggregator = factory(outputQueue, stream, currentWindows, fakeContext);

    // Process elements and check states
    for (size_t i = 0; i < testCase.ExpectedStates.size(); ++i) {
        EConsumeStatus status;
        // Keep calling Next() until we get Ok (skipping Wait statuses)
        do {
            status = aggregator.Next();
        } while (status == EConsumeStatus::Wait);

        UNIT_ASSERT_VALUES_EQUAL_C(
            status,
            EConsumeStatus::Ok,
            TStringBuilder() << "Step: " << (i + 1) << ", Status should be Ok");

        const auto& expectedState = testCase.ExpectedStates[i];

        // Check current element
        auto currentElement = aggregator.GetCurrentElement();
        UNIT_ASSERT_C(
            ElementsEqual(currentElement, expectedState.CurrentElement),
            TStringBuilder() << "Step: " << (i + 1)
                             << ", Current element mismatch. Expected: " << expectedState.CurrentElement
                             << ", Got: " << currentElement);

        // Check queue content
        if (outputQueue.Size() != expectedState.QueueContent.size()) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                outputQueue.Size(),
                expectedState.QueueContent.size(),
                TStringBuilder() << "Step: " << (i + 1)
                                 << ", Queue size mismatch. Actual queue: " << FormatQueueContent(outputQueue));
        }

        for (size_t j = 0; j < expectedState.QueueContent.size(); ++j) {
            UNIT_ASSERT_C(
                ElementsEqual(outputQueue.Get(j), expectedState.QueueContent[j]),
                TStringBuilder() << "Step: " << (i + 1)
                                 << ", Queue[" << j << "] mismatch. Expected: " << expectedState.QueueContent[j]
                                 << ", Got: " << outputQueue.Get(j));
        }

        // Helper lambda to check interval
        auto checkInterval = [&](const TIntervalCheckOrEmpty& checkVariant, auto getIntervalFunc, size_t idx, const char* intervalType) {
            if (std::holds_alternative<TEmptyInterval>(checkVariant)) {
                // Empty interval - verify it's actually empty (Min > Max)
                auto interval = getIntervalFunc(idx);
                UNIT_ASSERT_C(
                    interval.Min() >= interval.Max(),
                    TStringBuilder() << "Step: " << (i + 1)
                                     << ", " << intervalType << " " << idx
                                     << " should be empty (Min >= Max), but Min=" << interval.Min()
                                     << ", Max=" << interval.Max());
                return;
            }
            const auto& check = std::get<TIntervalCheck>(checkVariant);
            auto interval = getIntervalFunc(idx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                interval.Min(),
                check.ExpectedMin,
                TStringBuilder() << "Step: " << (i + 1)
                                 << ", " << intervalType << " " << idx << " Min");
            UNIT_ASSERT_VALUES_EQUAL_C(
                interval.Max(),
                check.ExpectedMax,
                TStringBuilder() << "Step: " << (i + 1)
                                 << ", " << intervalType << " " << idx << " Max");
        };

        // Check row intervals
        for (size_t idx = 0; idx < expectedState.RowIntervalChecks.size(); ++idx) {
            checkInterval(expectedState.RowIntervalChecks[idx],
                          [&](size_t i) { return currentWindows.GetIntervalInQueueByRow(i); },
                          idx, "Row interval");
        }

        // Check range intervals
        for (size_t idx = 0; idx < expectedState.RangeIntervalChecks.size(); ++idx) {
            checkInterval(expectedState.RangeIntervalChecks[idx],
                          [&](size_t i) { return currentWindows.GetIntervalInQueueByRange(i); },
                          idx, "Range interval");
        }

        // Check range incremental intervals
        for (size_t idx = 0; idx < expectedState.RangeIncrementalChecks.size(); ++idx) {
            checkInterval(expectedState.RangeIncrementalChecks[idx],
                          [&](size_t i) { return currentWindows.GetIntervalInQueueByRangeIncremental(i); },
                          idx, "Range incremental");
        }

        // Check row incremental intervals
        for (size_t idx = 0; idx < expectedState.RowIncrementalChecks.size(); ++idx) {
            checkInterval(expectedState.RowIncrementalChecks[idx],
                          [&](size_t i) { return currentWindows.GetIntervalInQueueByRowIncremental(i); },
                          idx, "Row incremental");
        }
    }

    // Check that after processing all elements, aggregator returns End
    // Skip any remaining Wait statuses and verify we get End
    constexpr size_t timesToCheckEnd = 10;
    for (size_t j = 0; j < timesToCheckEnd; j++) {
        EConsumeStatus finalStatus;
        do {
            finalStatus = aggregator.Next();
        } while (finalStatus == EConsumeStatus::Wait);

        UNIT_ASSERT_VALUES_EQUAL_C(
            finalStatus,
            EConsumeStatus::End,
            TStringBuilder() << ", After all elements processed, status should be End");
    }
}

} // namespace NKikimr::NMiniKQL::NTest::NWindow
