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
}

} // namespace NKikimr::NSysView
