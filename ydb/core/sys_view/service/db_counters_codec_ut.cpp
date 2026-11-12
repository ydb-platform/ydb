#include "db_counters_codec.h"

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {

namespace {

// Full (dense) histogram as held by a node before encoding
NKikimrSysView::TDbCounters::THistogram* AddDenseHistogram(NKikimrSysView::TDbCounters& counters,
    std::initializer_list<ui64> values, bool nonDerivative = false)
{
    auto* histogram = counters.AddHistogram();
    for (ui64 value : values) {
        histogram->AddBuckets(value);
    }
    if (nonDerivative) {
        histogram->SetNonDerivative(true);
    }
    return histogram;
}

// Encoded histogram: sparse (bucket, value) pairs
NKikimrSysView::TDbCounters::THistogram* AddEncodedHistogram(NKikimrSysView::TDbCounters& counters,
    ui64 bucketsCount, std::initializer_list<ui64> pairs, bool nonDerivative = false)
{
    auto* histogram = counters.AddHistogram();
    histogram->SetBucketsCount(bucketsCount);
    for (ui64 value : pairs) {
        histogram->AddBuckets(value);
    }
    if (nonDerivative) {
        histogram->SetNonDerivative(true);
    }
    return histogram;
}

void AssertEncodedHistogram(const NKikimrSysView::TDbCounters::THistogram& histogram,
    ui64 bucketsCount, std::initializer_list<ui64> pairs, bool nonDerivative)
{
    UNIT_ASSERT_VALUES_EQUAL(histogram.GetBucketsCount(), bucketsCount);
    UNIT_ASSERT_VALUES_EQUAL(histogram.GetNonDerivative(), nonDerivative);
    UNIT_ASSERT_VALUES_EQUAL(size_t(histogram.BucketsSize()), pairs.size());
    int i = 0;
    for (ui64 value : pairs) {
        UNIT_ASSERT_VALUES_EQUAL(histogram.GetBuckets(i++), value);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TDbCountersCodecTest) {
    Y_UNIT_TEST(AbsoluteAndDeltaReportsRestoreCountersIncludingZeroGauges) {
        NKikimrSysView::TDbCounters previous;
        previous.AddSimple(10);
        for (ui64 value : {0, 20, 0}) {
            previous.AddCumulative(value);
        }
        auto* histogram = previous.AddHistogram();
        histogram->AddBuckets(0);
        histogram->AddBuckets(5);

        NKikimrSysView::TDbCounters wire, restored;
        CalculateCountersDiff(&wire, previous);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetCumulativeCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(wire.CumulativeSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetCumulative(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetCumulative(1), 20);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetHistogram(0).GetBucketsCount(), 2);
        TAggregateSimple<false>::Apply(&restored, wire);
        TAggregateCumulative<false>::Apply(&restored, wire);

        auto current = previous;
        current.SetSimple(0, 0);
        current.SetCumulative(1, 26);
        current.SetCumulative(2, 7);
        current.MutableHistogram(0)->SetBuckets(1, 8);
        CalculateCountersDiff(&wire, current, &previous);
        ResetSimpleCounters(&restored);
        TAggregateSimple<false>::Apply(&restored, wire);
        TAggregateCumulative<false>::Apply(&restored, wire);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSimple(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(1), 26);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(2), 7);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(1), 8);

        previous = current;
        current.SetCumulative(1, 3);
        current.MutableHistogram(0)->SetBuckets(1, 2);
        CalculateCountersDiff(&wire, current, &previous);
        UNIT_ASSERT_VALUES_EQUAL(wire.CumulativeSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetCumulative(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetCumulative(1), ui64(3) - ui64(26));
        UNIT_ASSERT_VALUES_EQUAL(wire.GetHistogram(0).BucketsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetHistogram(0).GetBuckets(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(wire.GetHistogram(0).GetBuckets(1), ui64(2) - ui64(8));
        TAggregateCumulative<false>::Apply(&restored, wire);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(1), 3);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(2), 7);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(1), 2);
    }

    Y_UNIT_TEST(TabletMaximaRemainAbsoluteAcrossReports) {
        NKikimrSysView::TDbTabletCounters previous, current, wire;
        previous.SetType(TTabletTypes::DataShard);
        previous.MutableMaxExecutorCounters()->AddCumulative(7);
        current = previous;
        current.MutableMaxExecutorCounters()->SetCumulative(0, 9);
        CalculateCountersDiff(&wire, current, &previous);
        NKikimrSysView::TDbCounters restored;
        TAggregateCumulative<true>::Apply(&restored, wire.GetMaxExecutorCounters());
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(0), 9);
        ResetMaxCounters(&restored);
        current.MutableMaxExecutorCounters()->SetCumulative(0, 2);
        CalculateCountersDiff(&wire, current, &previous);
        TAggregateCumulative<true>::Apply(&restored, wire.GetMaxExecutorCounters());
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(0), 2);
    }

    Y_UNIT_TEST(MergedSparseDeltasRestoreMultiplePendingReports) {
        NKikimrSysView::TDbCounters pending, additional, current;
        pending.AddSimple(9);
        pending.AddSimple(99);
        pending.SetCumulativeCount(4);
        for (ui64 value : {0, 10, 2, 20}) {
            pending.AddCumulative(value);
        }
        additional.SetCumulativeCount(4);
        for (ui64 value : {0, 2, 3, 5}) {
            additional.AddCumulative(value);
        }
        current.AddSimple(0);
        current.AddSimple(5);
        current.SetCumulativeCount(4);
        for (ui64 value : {0, 3, 1, 7}) {
            current.AddCumulative(value);
        }
        const auto pendingBefore = pending.SerializeAsString();
        const auto additionalBefore = additional.SerializeAsString();

        MergeCounterDeltas(current, pending);
        MergeCounterDeltas(current, additional);

        NKikimrSysView::TDbCounters restored;
        for (ui64 value : {100, 200, 300, 400}) {
            restored.AddCumulative(value);
        }
        TAggregateSimple<false>::Apply(&restored, current);
        TAggregateCumulative<false>::Apply(&restored, current);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSimple(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSimple(1), 5);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(0), 115);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(1), 207);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(2), 320);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(3), 405);
        UNIT_ASSERT_VALUES_EQUAL(pending.SerializeAsString(), pendingBefore);
        UNIT_ASSERT_VALUES_EQUAL(additional.SerializeAsString(), additionalBefore);
    }

    Y_UNIT_TEST(MergedRetirementAndRecreationRestoreHistogram) {
        NKikimrSysView::TDbCounters pending, current, restored;
        auto* previousHistogram = restored.AddHistogram();
        for (ui64 value : {2, 5, 7}) {
            previousHistogram->AddBuckets(value);
        }

        // Retirement cancels all observations from the receiver's previous snapshot.
        auto* retiredHistogram = pending.AddHistogram();
        retiredHistogram->SetBucketsCount(3);
        for (ui64 index = 0; index < 3; ++index) {
            retiredHistogram->AddBuckets(index);
            retiredHistogram->AddBuckets(ui64(0) - previousHistogram->GetBuckets(index));
        }
        auto* recreatedHistogram = current.AddHistogram();
        recreatedHistogram->SetBucketsCount(3);
        for (ui64 value : {0, 3, 1, 5}) {
            recreatedHistogram->AddBuckets(value);
        }
        const auto pendingBefore = pending.SerializeAsString();

        MergeCounterDeltas(current, pending);
        TAggregateCumulative<false>::Apply(&restored, current);

        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(0), 3);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(1), 5);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(current.GetHistogram(0).GetBucketsCount(), 3);
        // The unchanged middle bucket has no entry in the merged delta.
        UNIT_ASSERT_VALUES_EQUAL(current.GetHistogram(0).BucketsSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(pending.SerializeAsString(), pendingBefore);
    }

    Y_UNIT_TEST(MergedTabletDeltasKeepLatestStatefulCounters) {
        const auto setCounters = [](NKikimrSysView::TDbCounters& counters, ui64 simple, ui64 cumulative) {
            counters.AddSimple(simple);
            counters.AddCumulative(cumulative);
        };
        NKikimrSysView::TDbTabletCounters pendingSnapshot, currentSnapshot, pending, current;
        setCounters(*pendingSnapshot.MutableExecutorCounters(), 80, 10);
        setCounters(*pendingSnapshot.MutableAppCounters(), 90, 20);
        setCounters(*pendingSnapshot.MutableMaxExecutorCounters(), 70, 30);
        setCounters(*pendingSnapshot.MutableMaxAppCounters(), 100, 40);
        currentSnapshot.SetType(TTabletTypes::DataShard);
        setCounters(*currentSnapshot.MutableExecutorCounters(), 0, 3);
        setCounters(*currentSnapshot.MutableAppCounters(), 5, 7);
        setCounters(*currentSnapshot.MutableMaxExecutorCounters(), 2, 11);
        setCounters(*currentSnapshot.MutableMaxAppCounters(), 0, 13);
        CalculateCountersDiff(&pending, pendingSnapshot);
        CalculateCountersDiff(&current, currentSnapshot);
        const auto pendingBefore = pending.SerializeAsString();
        const auto maxExecutorBefore = current.GetMaxExecutorCounters().SerializeAsString();
        const auto maxAppBefore = current.GetMaxAppCounters().SerializeAsString();

        MergeCounterDeltas(current, pending);

        NKikimrSysView::TDbCounters executor, app;
        TAggregateSimple<false>::Apply(&executor, current.GetExecutorCounters());
        TAggregateCumulative<false>::Apply(&executor, current.GetExecutorCounters());
        TAggregateSimple<false>::Apply(&app, current.GetAppCounters());
        TAggregateCumulative<false>::Apply(&app, current.GetAppCounters());
        UNIT_ASSERT_VALUES_EQUAL(executor.GetSimple(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(executor.GetCumulative(0), 13);
        UNIT_ASSERT_VALUES_EQUAL(app.GetSimple(0), 5);
        UNIT_ASSERT_VALUES_EQUAL(app.GetCumulative(0), 27);
        UNIT_ASSERT_VALUES_EQUAL(current.GetType(), TTabletTypes::DataShard);
        UNIT_ASSERT_VALUES_EQUAL(current.GetMaxExecutorCounters().SerializeAsString(), maxExecutorBefore);
        UNIT_ASSERT_VALUES_EQUAL(current.GetMaxAppCounters().SerializeAsString(), maxAppBefore);
        UNIT_ASSERT_VALUES_EQUAL(pending.SerializeAsString(), pendingBefore);
    }

    Y_UNIT_TEST(NonDerivativeHistogramIsEncodedAsCurrentValueNotDelta) {
        NKikimrSysView::TDbCounters previous, current, wire;
        AddDenseHistogram(previous, {1, 2, 5}, true);
        AddDenseHistogram(previous, {1, 4});
        AddDenseHistogram(current, {3, 0, 5}, true);
        AddDenseHistogram(current, {4, 4});

        CalculateCountersDiff(&wire, current, &previous);

        UNIT_ASSERT_VALUES_EQUAL(wire.HistogramSize(), 2);
        // The current value, not current - previous
        AssertEncodedHistogram(wire.GetHistogram(0), 3, {0, 3, 2, 5}, true);
        // An unflagged neighbour is still a delta
        AssertEncodedHistogram(wire.GetHistogram(1), 2, {0, 3}, false);
    }

    Y_UNIT_TEST(UnchangedNonDerivativeHistogramIsReemitted) {
        NKikimrSysView::TDbCounters previous, wire;
        AddDenseHistogram(previous, {0, 4, 0, 1}, true);
        auto current = previous;

        CalculateCountersDiff(&wire, current, &previous);

        AssertEncodedHistogram(wire.GetHistogram(0), 4, {1, 4, 3, 1}, true);
    }

    Y_UNIT_TEST(CopyCountersKeepsNonDerivativeFlag) {
        NKikimrSysView::TDbCounters current, wire, wireWithoutPrevious;
        AddDenseHistogram(current, {0, 7, 0, 2}, true);
        AddDenseHistogram(current, {6, 0});

        CopyCounters(&wire, current);
        CalculateCountersDiff(&wireWithoutPrevious, current, nullptr);

        for (const auto* encoded : {&wire, &wireWithoutPrevious}) {
            UNIT_ASSERT_VALUES_EQUAL(encoded->HistogramSize(), 2);
            AssertEncodedHistogram(encoded->GetHistogram(0), 4, {1, 7, 3, 2}, true);
            AssertEncodedHistogram(encoded->GetHistogram(1), 2, {0, 6}, false);
        }
    }

    Y_UNIT_TEST(AllZeroNonDerivativeHistogramIsStillEmitted) {
        NKikimrSysView::TDbCounters previous, current, wire, wireWithoutPrevious;
        AddDenseHistogram(previous, {0, 3, 1}, true);
        AddDenseHistogram(current, {0, 0, 0}, true);

        CalculateCountersDiff(&wire, current, &previous);
        CalculateCountersDiff(&wireWithoutPrevious, current);

        for (const auto* encoded : {&wire, &wireWithoutPrevious}) {
            UNIT_ASSERT_VALUES_EQUAL(encoded->HistogramSize(), 1);
            AssertEncodedHistogram(encoded->GetHistogram(0), 3, {}, true);
        }
    }

    Y_UNIT_TEST(AggregateCumulativeResizesButDoesNotAddNonDerivativeHistogram) {
        NKikimrSysView::TDbCounters wire, restored;
        AddEncodedHistogram(wire, 3, {1, 5}, true);
        AddEncodedHistogram(wire, 3, {1, 5}, false);

        TAggregateCumulative<false>::Apply(&restored, wire);
        TAggregateCumulative<false>::Apply(&restored, wire);

        UNIT_ASSERT_VALUES_EQUAL(restored.HistogramSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).BucketsSize(), 3);
        for (int b = 0; b < 3; ++b) {
            UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(0).GetBuckets(b), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(1).BucketsSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(1).GetBuckets(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(1).GetBuckets(1), 10);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetHistogram(1).GetBuckets(2), 0);
    }

    Y_UNIT_TEST(MergedNonDerivativeHistogramTakesCurrentValue) {
        NKikimrSysView::TDbCounters pending, current;
        pending.AddSimple(9);
        pending.SetCumulativeCount(2);
        pending.AddCumulative(1);
        pending.AddCumulative(4);
        AddEncodedHistogram(pending, 3, {1, 1}, true);
        AddEncodedHistogram(pending, 2, {0, 2}, false);
        current.AddSimple(7);
        current.SetCumulativeCount(2);
        current.AddCumulative(1);
        current.AddCumulative(3);
        // All buckets emptied: the current value is empty, nothing of the pending one may survive
        AddEncodedHistogram(current, 3, {}, true);
        AddEncodedHistogram(current, 2, {0, 3, 1, 1}, false);
        const auto pendingBefore = pending.SerializeAsString();

        MergeCounterDeltas(current, pending);

        AssertEncodedHistogram(current.GetHistogram(0), 3, {}, true);
        // The derivative histogram is still summed
        AssertEncodedHistogram(current.GetHistogram(1), 2, {0, 5, 1, 1}, false);
        // Cumulative is summed, Simple is the latest
        UNIT_ASSERT_VALUES_EQUAL(current.GetSimple(0), 7);
        UNIT_ASSERT_VALUES_EQUAL(current.GetCumulativeCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(current.CumulativeSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(current.GetCumulative(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(current.GetCumulative(1), 7);
        UNIT_ASSERT_VALUES_EQUAL(pending.SerializeAsString(), pendingBefore);
    }

    Y_UNIT_TEST(MergedNonDerivativeHistogramOfPendingIsKeptWhenCurrentLacksIt) {
        NKikimrSysView::TDbCounters pending, current;
        AddEncodedHistogram(pending, 3, {1, 6}, true);
        AddEncodedHistogram(pending, 2, {0, 2}, false);
        current.AddSimple(1);

        MergeCounterDeltas(current, pending);

        UNIT_ASSERT_VALUES_EQUAL(current.HistogramSize(), 2);
        AssertEncodedHistogram(current.GetHistogram(0), 3, {1, 6}, true);
        AssertEncodedHistogram(current.GetHistogram(1), 2, {0, 2}, false);
    }

    Y_UNIT_TEST(MergedTabletDeltasKeepNonDerivativeHistogramOfCurrent) {
        NKikimrSysView::TDbTabletCounters pending, current;
        AddEncodedHistogram(*pending.MutableExecutorCounters(), 3, {1, 1}, true);
        AddEncodedHistogram(*pending.MutableAppCounters(), 2, {0, 2}, true);
        AddEncodedHistogram(*current.MutableExecutorCounters(), 3, {0, 1, 2, 1}, true);
        AddEncodedHistogram(*current.MutableAppCounters(), 2, {}, true);

        MergeCounterDeltas(current, pending);

        AssertEncodedHistogram(current.GetExecutorCounters().GetHistogram(0), 3, {0, 1, 2, 1}, true);
        AssertEncodedHistogram(current.GetAppCounters().GetHistogram(0), 2, {}, true);
    }

    Y_UNIT_TEST(MarkHistogramsNonDerivativeMarksOnlyGivenIndices) {
        NKikimrSysView::TDbCounters counters;
        for (int i = 0; i < 3; ++i) {
            AddDenseHistogram(counters, {1, 2});
        }

        // Index 7 is out of range and ignored
        MarkHistogramsNonDerivative(&counters, {2, 7});

        UNIT_ASSERT_VALUES_EQUAL(counters.HistogramSize(), 3);
        UNIT_ASSERT(!counters.GetHistogram(0).GetNonDerivative());
        UNIT_ASSERT(!counters.GetHistogram(1).GetNonDerivative());
        UNIT_ASSERT(counters.GetHistogram(2).GetNonDerivative());
    }
}

} // namespace NKikimr::NSysView
