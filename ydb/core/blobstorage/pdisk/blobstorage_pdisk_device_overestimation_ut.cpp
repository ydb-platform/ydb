#include "blobstorage_pdisk_device_overestimation.h"

#include <ydb/core/util/hp_timer_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NPDisk {

namespace {

TDeviceIoSample MakeSample(ui64 submitCycles, ui64 completeCycles, ui64 offset, ui64 size,
        bool isWrite, ui64 baseCostNs) {
    TDeviceIoSample sample;
    sample.SubmitCycles = submitCycles;
    sample.CompleteCycles = completeCycles;
    sample.Offset = offset;
    sample.Size = size;
    sample.IsWrite = isWrite;
    sample.BaseCostNs = baseCostNs;
    return sample;
}

} // namespace

Y_UNIT_TEST_SUITE(TDeviceOverestimationAggregatorTest) {

    Y_UNIT_TEST(EmptyBufferProducesEmptyResult) {
        TDeviceOverestimationAggregator agg;
        auto result = agg.ComputeAndReset(/*seekCostNs*/ 1000000);
        UNIT_ASSERT_VALUES_EQUAL(result.SampleCount, 0u);
        UNIT_ASSERT_VALUES_EQUAL(result.EstimatedNs, 0u);
        UNIT_ASSERT_VALUES_EQUAL(result.ActualNs, 0u);
    }

    // A single sample from one source: actual duration should be
    // min(complete-submit, complete-prevComplete) = complete-submit (since
    // there's no previous completion, PrevCompleteCycles starts at 0).
    Y_UNIT_TEST(SingleSampleUsesSubmitToCompleteDuration) {
        TDeviceOverestimationAggregator agg;
        const ui64 submit = 1000;
        const ui64 complete = 2000;
        agg.Push(MakeSample(submit, complete, /*offset*/ 0, /*size*/ 4096, false, /*baseCostNs*/ 500));

        auto result = agg.ComputeAndReset(/*seekCostNs*/ 100);
        UNIT_ASSERT_VALUES_EQUAL(result.SampleCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.ActualNs, HPNanoSeconds((i64)(complete - submit)));
        // First sample: EndOffset starts at 0, offset is 0, so no seek from
        // offset mismatch; but the submit-vs-prev-complete check
        // (isSeekExpected) may still trigger since PrevCompleteCycles==0 and
        // submit + seekCostNs/25 >= 0 is trivially true. Just check the
        // magnitude is either BaseCostNs or BaseCostNs+seekCostNs.
        UNIT_ASSERT(result.EstimatedNs == 500 || result.EstimatedNs == 600);
    }

    // Two contiguous samples from the SAME source (simulating one device
    // stream): second sample's actual duration should be clamped by
    // min(complete2-submit2, complete2-complete1).
    Y_UNIT_TEST(TwoContiguousSamplesUseParallelismOneClamp) {
        TDeviceOverestimationAggregator agg;
        // Sample 1: submit=0, complete=1000
        agg.Push(MakeSample(0, 1000, 0, 4096, false, 100));
        // Sample 2: submits early (overlapping with sample 1's execution),
        // completes at 1500. Actual duration should be clamped to
        // complete2 - complete1 = 500 (since submit2=200 < complete1=1000,
        // startCycle = max(200, 1000) = 1000).
        agg.Push(MakeSample(200, 1500, 4096, 4096, false, 100));

        auto result = agg.ComputeAndReset(/*seekCostNs*/ 0);
        UNIT_ASSERT_VALUES_EQUAL(result.SampleCount, 2u);
        // Total actual = (1000-0) + (1500-1000) = 1000 + 500 = 1500 cycles worth.
        ui64 expectedActualNs = HPNanoSeconds(1000) + HPNanoSeconds(500);
        UNIT_ASSERT_VALUES_EQUAL(result.ActualNs, expectedActualNs);
    }

    // Merging samples from two different sources (interleaved by completion
    // time) should process them in completion order regardless of push order.
    Y_UNIT_TEST(MergesMultipleSourcesByCompletionOrder) {
        TDeviceOverestimationAggregator agg;

        // Source A pushes a sample completing at 3000.
        agg.Push(MakeSample(2000, 3000, 0, 4096, false, 100));
        // Source B pushes a sample completing earlier, at 1000 (out of push order).
        agg.Push(MakeSample(500, 1000, 8192, 4096, true, 100));

        auto result = agg.ComputeAndReset(/*seekCostNs*/ 0);
        UNIT_ASSERT_VALUES_EQUAL(result.SampleCount, 2u);
        // Processing order should be: sample completing at 1000 first, then
        // the one completing at 3000. For the second (offset jumps from 8192
        // region to 0, and it's a different source), seek is expected -- but
        // seekCostNs=0 here so it doesn't affect EstimatedNs. Actual for
        // first: startCycle = max(500, 0) = 500, duration = 1000-500=500.
        // Actual for second: startCycle = max(2000, 1000) = 2000, duration = 3000-2000=1000.
        ui64 expectedActualNs = HPNanoSeconds(500) /*first sample: 1000-500*/
            + HPNanoSeconds(1000) /*second sample clamped*/;
        UNIT_ASSERT_VALUES_EQUAL(result.ActualNs, expectedActualNs);
        UNIT_ASSERT_VALUES_EQUAL(result.EstimatedNs, 200u); // 100 + 100, seekCostNs=0
    }

    Y_UNIT_TEST(ZeroSizeSamplesAreSkipped) {
        TDeviceOverestimationAggregator agg;
        agg.Push(MakeSample(0, 1000, 0, 0, false, 100));
        auto result = agg.ComputeAndReset(0);
        UNIT_ASSERT_VALUES_EQUAL(result.SampleCount, 0u);
    }

    Y_UNIT_TEST(OverflowDropsOldestAndCountsDropped) {
        TDeviceOverestimationAggregator agg(/*maxBufferedSamples*/ 4);
        for (ui64 i = 0; i < 10; ++i) {
            agg.Push(MakeSample(i * 100, i * 100 + 50, i * 4096, 4096, false, 10));
        }
        UNIT_ASSERT(agg.GetDroppedSamples() > 0);
        auto result = agg.ComputeAndReset(0);
        // Some samples should remain (at least the last few pushed).
        UNIT_ASSERT(result.SampleCount > 0);
        UNIT_ASSERT(result.SampleCount <= 4);
    }

    Y_UNIT_TEST(ComputeAndResetDrainsBuffer) {
        TDeviceOverestimationAggregator agg;
        agg.Push(MakeSample(0, 1000, 0, 4096, false, 100));
        auto firstResult = agg.ComputeAndReset(0);
        UNIT_ASSERT_VALUES_EQUAL(firstResult.SampleCount, 1u);

        // Buffer should be empty now; a second call with no new samples
        // pushed should produce an empty result.
        auto secondResult = agg.ComputeAndReset(0);
        UNIT_ASSERT_VALUES_EQUAL(secondResult.SampleCount, 0u);
    }
}

// Tests for the UseDeviceOverestimationRatioMerged option's core decision
// logic: SelectPublishedOverestimationResult() picks which of the legacy
// (PDisk-only) or merged (cross-source) computed results gets published via
// the user-facing DeviceOverestimationRatio/DeviceNonperformanceMs sensors.
// This is the pure, unit-testable piece of the ICB-controlled option
// described in blobstorage_pdisk_blockdevice_async.cpp's TSharedCallback::Exec.
Y_UNIT_TEST_SUITE(TSelectPublishedOverestimationResultTest) {

    Y_UNIT_TEST(WhenEnabledPublishesMergedResult) {
        TOverestimationRatioResult legacy;
        legacy.OverestimationRatio = 111;
        legacy.NonperformanceMs = 11;

        TOverestimationRatioResult merged;
        merged.OverestimationRatio = 222;
        merged.NonperformanceMs = 22;

        const auto& selected = SelectPublishedOverestimationResult(/*useMerged*/ true, legacy, merged);
        UNIT_ASSERT_VALUES_EQUAL(selected.OverestimationRatio, merged.OverestimationRatio);
        UNIT_ASSERT_VALUES_EQUAL(selected.NonperformanceMs, merged.NonperformanceMs);
    }

    Y_UNIT_TEST(WhenDisabledPublishesLegacyResult) {
        TOverestimationRatioResult legacy;
        legacy.OverestimationRatio = 111;
        legacy.NonperformanceMs = 11;

        TOverestimationRatioResult merged;
        merged.OverestimationRatio = 222;
        merged.NonperformanceMs = 22;

        const auto& selected = SelectPublishedOverestimationResult(/*useMerged*/ false, legacy, merged);
        UNIT_ASSERT_VALUES_EQUAL(selected.OverestimationRatio, legacy.OverestimationRatio);
        UNIT_ASSERT_VALUES_EQUAL(selected.NonperformanceMs, legacy.NonperformanceMs);
    }

    // Simulates the option being toggled at runtime (as it would be via the
    // ICB console command, without a cluster restart): flipping the flag
    // must immediately flip which result is selected, with no memory of the
    // previous choice.
    Y_UNIT_TEST(TogglingFlagSwitchesSelectionImmediately) {
        TOverestimationRatioResult legacy;
        legacy.OverestimationRatio = 500;
        legacy.NonperformanceMs = 5;

        TOverestimationRatioResult merged;
        merged.OverestimationRatio = 900;
        merged.NonperformanceMs = 9;

        bool useMerged = true;
        UNIT_ASSERT_VALUES_EQUAL(
            SelectPublishedOverestimationResult(useMerged, legacy, merged).OverestimationRatio,
            merged.OverestimationRatio);

        useMerged = false;
        UNIT_ASSERT_VALUES_EQUAL(
            SelectPublishedOverestimationResult(useMerged, legacy, merged).OverestimationRatio,
            legacy.OverestimationRatio);

        useMerged = true;
        UNIT_ASSERT_VALUES_EQUAL(
            SelectPublishedOverestimationResult(useMerged, legacy, merged).OverestimationRatio,
            merged.OverestimationRatio);
    }
}

} // namespace NPDisk
} // namespace NKikimr
