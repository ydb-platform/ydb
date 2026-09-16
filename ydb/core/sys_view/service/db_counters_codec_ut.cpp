#include "db_counters_codec.h"

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {

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
}

} // namespace NKikimr::NSysView
