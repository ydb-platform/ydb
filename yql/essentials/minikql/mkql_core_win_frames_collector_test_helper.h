#pragma once

#include <yql/essentials/minikql/mkql_core_win_frames_collector.h>

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
        *this = TIntervalCheck{min, max};
    }
};

template <typename TElement, ESortOrder SortOrder, typename TRangeElement = TElement>
struct TTestCase {
    // Input interval bounds. Requires two: left and right.
    TVector<TInputRowWindowFrame> RowIntervals;
    TVector<TInputRangeWindowFrame<TRangeElement>> RangeIntervals;

    // Incremental bounds - for tracking only right boundary.
    TVector<TInputRange<TRangeElement>> RangeIncrementals;
    TVector<TInputRow> RowIncrementals;

    // Element getter function - by default returns element as-is (identity) wrapped in TMaybe.
    std::function<TMaybe<TRangeElement>(const TElement&)> ElementGetter = [](const TElement& elem) -> TMaybe<TRangeElement> {
        Y_ABORT_UNLESS((std::is_same_v<TElement, TRangeElement>), "ElementGetter must be provided when TRangeElement != TElement");
        return TMaybe<TRangeElement>(elem);
    };

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
bool ElementsEqual(const TElement& a, const TElement& b) {
    if constexpr (std::is_floating_point_v<TElement>) {
        if (std::isnan(a) && std::isnan(b)) {
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
template <typename TElement, ESortOrder SortOrder, typename TRangeElement = TElement>
void RunTestCase(const TTestCase<TElement, SortOrder, TRangeElement>& testCase) {
    // Setup bounds.
    TCoreWinFrameCollectorBounds<TRangeElement> bounds(/*dedup=*/false);
    for (const auto& interval : testCase.RowIntervals) {
        bounds.AddRow(interval);
    }
    for (const auto& interval : testCase.RangeIntervals) {
        bounds.AddRange(interval);
    }
    for (const auto& incremental : testCase.RangeIncrementals) {
        bounds.AddRangeIncremental(incremental);
    }
    for (const auto& incremental : testCase.RowIncrementals) {
        bounds.AddRowIncremental(incremental);
    }

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
    auto factory = TCoreWinFramesCollector<TElement, decltype(testCase.ElementGetter), SortOrder>::CreateFactory(
        bounds, testCase.ElementGetter);
    auto aggregator = factory(outputQueue, stream, currentWindows);

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
