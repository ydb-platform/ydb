#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/protos/tablet.pb.h>
#include <util/generic/size_literals.h>
#include "tablet_metrics.h"

namespace NKikimr {
namespace NMetrics {

namespace {

void AssertMemoryReport(const NKikimrTabletBase::TMetrics& metrics, ui64 used, ui64 demand, ui64 reclaimable) {
    UNIT_ASSERT(metrics.HasMemory());
    UNIT_ASSERT(metrics.HasMemoryDemand());
    UNIT_ASSERT(metrics.HasMemoryReclaimable());
    UNIT_ASSERT_VALUES_EQUAL(metrics.GetMemory(), used);
    UNIT_ASSERT_VALUES_EQUAL(metrics.GetMemoryDemand(), demand);
    UNIT_ASSERT_VALUES_EQUAL(metrics.GetMemoryReclaimable(), reclaimable);
    UNIT_ASSERT_LE(metrics.GetMemoryReclaimable(), metrics.GetMemory());
    UNIT_ASSERT_LE(metrics.GetMemory(), metrics.GetMemoryDemand());
}

}

Y_UNIT_TEST_SUITE(TFlatMetrics) {
    Y_UNIT_TEST(MemoryReportIncludesZeroValues) {
        TResourceMetricsValues values;
        NKikimrTabletBase::TMetrics message;
        values.Fill(message);
        UNIT_ASSERT(!message.HasMemoryDemand());
        UNIT_ASSERT(!message.HasMemoryReclaimable());

        values.SetMemoryReport(10_MB, 12_MB, 0);
        values.Fill(message);
        AssertMemoryReport(message, 10_MB, 12_MB, 0);

        message.Clear();
        values.SetMemoryReport(0, 0, 0);
        values.Fill(message);
        AssertMemoryReport(message, 0, 0, 0);
    }

    Y_UNIT_TEST(MemoryReportClearsReclaimable) {
        TResourceMetrics metrics(1, 0, TActorId());
        NKikimrTabletBase::TMetrics message;
        metrics.SetMemoryReport(12_MB, 15_MB, 10_MB);
        UNIT_ASSERT(metrics.FillChanged(message));

        message.Clear();
        metrics.SetMemoryReport(12_MB, 15_MB, 0);
        UNIT_ASSERT(metrics.FillChanged(message));
        AssertMemoryReport(message, 12_MB, 15_MB, 0);

        message.Clear();
        UNIT_ASSERT(!metrics.FillChanged(message));
        UNIT_ASSERT(metrics.FillChanged(message, TInstant::Now(), true));
        AssertMemoryReport(message, 12_MB, 15_MB, 0);

        message.Clear();
        metrics.SetMemoryReport(0, 0, 0);
        UNIT_ASSERT(metrics.FillChanged(message));
        AssertMemoryReport(message, 0, 0, 0);

        TResourceMetrics emptyReport(2, 0, TActorId());
        emptyReport.SetMemoryReport(0, 0, 0);
        message.Clear();
        UNIT_ASSERT(emptyReport.FillChanged(message));
        AssertMemoryReport(message, 0, 0, 0);
    }

    Y_UNIT_TEST(MemoryReportPreservesInvariantsAcrossThresholds) {
        TResourceMetrics metrics(1, 0, TActorId());
        NKikimrTabletBase::TMetrics receiver;
        metrics.SetMemoryReport(11_MB + 100_KB, 12_MB, 10_MB + 900_KB);
        UNIT_ASSERT(metrics.FillChanged(receiver));

        // Used crosses a bucket boundary, while reclaimable stays in the same bucket.
        NKikimrTabletBase::TMetrics update;
        metrics.SetMemoryReport(10_MB + 500_KB, 12_MB, 10_MB + 400_KB);
        UNIT_ASSERT(metrics.FillChanged(update));
        receiver.MergeFrom(update);
        AssertMemoryReport(receiver, 10_MB + 500_KB, 12_MB, 10_MB + 400_KB);

        metrics.SetMemoryReport(10_MB + 900_KB, 11_MB + 100_KB, 0);
        update.Clear();
        UNIT_ASSERT(metrics.FillChanged(update));
        receiver.MergeFrom(update);

        // Used grows past the previously sent demand; demand's bucket does not change.
        metrics.SetMemoryReport(11_MB + 500_KB, 11_MB + 700_KB, 0);
        update.Clear();
        UNIT_ASSERT(metrics.FillChanged(update));
        receiver.MergeFrom(update);
        AssertMemoryReport(receiver, 11_MB + 500_KB, 11_MB + 700_KB, 0);
    }

    Y_UNIT_TEST(MemoryReportRetainsChangeThrottling) {
        TResourceMetrics metrics(1, 0, TActorId());
        NKikimrTabletBase::TMetrics message;
        metrics.SetMemoryReport(10_MB + 100_KB, 12_MB + 100_KB, 1_MB + 100_KB);
        UNIT_ASSERT(metrics.FillChanged(message));

        metrics.SetMemoryReport(10_MB + 200_KB, 12_MB + 200_KB, 1_MB + 200_KB);
        message.Clear();
        UNIT_ASSERT(!metrics.FillChanged(message));
        UNIT_ASSERT(metrics.FillChanged(message, TInstant::Now(), true));
        AssertMemoryReport(message, 10_MB + 200_KB, 12_MB + 200_KB, 1_MB + 200_KB);

        // A change in demand alone must also send the other two current values.
        metrics.SetMemoryReport(10_MB + 300_KB, 13_MB, 1_MB + 300_KB);
        message.Clear();
        UNIT_ASSERT(metrics.FillChanged(message));
        AssertMemoryReport(message, 10_MB + 300_KB, 13_MB, 1_MB + 300_KB);
    }

    Y_UNIT_TEST(LegacyMemoryDoesNotOverwriteMemoryReport) {
        TResourceMetrics metrics(1, 0, TActorId());
        metrics.SetMemoryReport(10_MB, 12_MB, 0);
        metrics.Memory.Set(20_MB);
        NKikimrTabletBase::TMetrics message;
        metrics.Fill(message);
        AssertMemoryReport(message, 10_MB, 12_MB, 0);

        message.Clear();
        UNIT_ASSERT(metrics.FillChanged(message));
        AssertMemoryReport(message, 10_MB, 12_MB, 0);

        metrics.Memory.Set(30_MB);
        message.Clear();
        UNIT_ASSERT(!metrics.FillChanged(message));
        UNIT_ASSERT(metrics.FillChanged(message, TInstant::Now(), true));
        AssertMemoryReport(message, 10_MB, 12_MB, 0);

        metrics.SetMemoryReport(30_MB, 35_MB, 5_MB);
        message.Clear();
        UNIT_ASSERT(metrics.FillChanged(message));
        AssertMemoryReport(message, 30_MB, 35_MB, 5_MB);
    }

    Y_UNIT_TEST(LegacyMemoryDoesNotEnableMemoryReport) {
        TResourceMetrics metrics(1, 0, TActorId());
        metrics.Memory.Set(10_MB);
        NKikimrTabletBase::TMetrics message;
        metrics.Fill(message);
        UNIT_ASSERT_VALUES_EQUAL(message.GetMemory(), 10_MB);
        UNIT_ASSERT(!message.HasMemoryDemand());
        UNIT_ASSERT(!message.HasMemoryReclaimable());

        message.Clear();
        UNIT_ASSERT(metrics.FillChanged(message));
        UNIT_ASSERT_VALUES_EQUAL(message.GetMemory(), 10_MB);
        UNIT_ASSERT(!message.HasMemoryDemand());
        UNIT_ASSERT(!message.HasMemoryReclaimable());

        metrics.Memory.Set(11_MB);
        message.Clear();
        UNIT_ASSERT(metrics.FillChanged(message, TInstant::Now(), true));
        UNIT_ASSERT_VALUES_EQUAL(message.GetMemory(), 11_MB);
        UNIT_ASSERT(!message.HasMemoryDemand());
        UNIT_ASSERT(!message.HasMemoryReclaimable());
    }

    Y_UNIT_TEST(TimeSeriesAvg4) {
        TTimeSeriesValue<ui64, TDuration::Minutes(1).GetValue(), 4> value;
        TInstant time = TInstant::Now();

        value.Increment(14, time);
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        time += TDuration::Seconds(15);

        value.Increment(14, time);
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        time += TDuration::Seconds(15);

        auto avg = value.GetValueAveragePerDuration(TDuration::Minutes(1));
        UNIT_ASSERT_C(avg >= 60 && avg <= 80, avg);
    }

    Y_UNIT_TEST(TimeSeriesAvg16) {
        TTimeSeriesValue<ui64, TDuration::Minutes(1).GetValue(), 16> value;
        TInstant time = TInstant::Now();

        value.Increment(14, time);
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        time += TDuration::Seconds(15);

        value.Increment(14, time);
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        time += TDuration::Seconds(15);

        auto avg = value.GetValueAveragePerDuration(TDuration::Minutes(1));
        UNIT_ASSERT_C(avg >= 60 && avg <= 65, avg);
    }

    Y_UNIT_TEST(TimeSeriesAvg16x60) {
        TTimeSeriesValue<ui64, TDuration::Minutes(1).GetValue(), 16> value;
        TInstant time = TInstant::Now();

        for (int i = 0; i < 60; ++i) {
            value.Increment(1, time);
            time += TDuration::Seconds(1);
        }
        auto avg = value.GetValueAveragePerDuration(TDuration::Minutes(1));
        UNIT_ASSERT_C(avg >= 55 && avg <= 65, avg);
    }

    Y_UNIT_TEST(TimeSeriesAvg16Signed) {
        TTimeSeriesValue<i64, TDuration::Minutes(1).GetValue(), 16> value;
        TInstant time = TInstant::Now();

        value.Increment(+14, time);
        time += TDuration::Seconds(15);
        value.Increment(-16, time);
        time += TDuration::Seconds(15);
        value.Increment(+13, time);
        time += TDuration::Seconds(15);
        value.Increment(-17, time);
        time += TDuration::Seconds(15);

        value.Increment(-14, time);
        time += TDuration::Seconds(15);
        value.Increment(+16, time);
        time += TDuration::Seconds(15);
        value.Increment(-13, time);
        time += TDuration::Seconds(15);
        value.Increment(+17, time);
        time += TDuration::Seconds(15);

        auto avg = value.GetValueAveragePerDuration(TDuration::Minutes(1));
        UNIT_ASSERT_C(avg >= 0 && avg <= 15, avg);
    }

    Y_UNIT_TEST(TimeSeriesKV) {
        TTimeSeriesValue<i64> value;
        TInstant time = TInstant::Now();
        TVector<i64> values = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,7476717,0,529363,-1065564};
        time -= TDuration::Days(1);
        for (i64 val : values) {
            value.Increment(val, time);
            time += TDuration::Hours(1);
        }
        auto avg = value.GetValueAveragePerDuration(TDuration::Seconds(1));
        UNIT_ASSERT_C(avg >= 112 && avg <= 114, avg);
    }

    Y_UNIT_TEST(TimeSeriesKV2) {
        TTimeSeriesValue<i64> value;
        TInstant time = TInstant::Now();
        TVector<i64> values = {0,0,0,0,1502,0,-64006,-100840,-151185,-4088398,-169038,-167227,-74841,-111563,-107191,-146359,-107399,-195925,-140440,-173191,-30211,-128287,-185191,-140449};
        time -= TDuration::Days(1);
        for (i64 val : values) {
            value.Increment(val, time);
            time += TDuration::Hours(1);
        }
        auto avg = value.GetValueAveragePerDuration(TDuration::Seconds(1));
        UNIT_ASSERT_C(avg < 0, avg);
    }

    Y_UNIT_TEST(TimeSeriesAVG) {
        TTimeSeriesValue<i64, 1000000ull * 60 * 5, 20> value;
        TInstant time = TInstant::Now();
        time -= TDuration::Seconds(2);
        value.Increment(400, time);
        time += TDuration::Seconds(1);
        value.Increment(400, time);
        auto avg = value.GetValueAveragePerDuration(TDuration::Seconds(1));
        UNIT_ASSERT_C(avg == 400, avg);
    }

    Y_UNIT_TEST(DecayingAverageAvg) {
        TDecayingAverageValue<ui64, NMetrics::DurationPerMinute> value;
        TInstant time = TInstant::Now();

        value.Increment(0, time); // first value will be skipped anyway
        UNIT_ASSERT(!value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(14, time);
        UNIT_ASSERT(!value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        UNIT_ASSERT(!value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        UNIT_ASSERT(!value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        UNIT_ASSERT(value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);

        value.Increment(14, time);
        UNIT_ASSERT(value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(16, time);
        UNIT_ASSERT(value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(13, time);
        UNIT_ASSERT(value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Seconds(15);
        value.Increment(17, time);
        UNIT_ASSERT(value.IsValueReady());
        UNIT_ASSERT(!value.IsValueObsolete(time));
        auto avg = value.GetValue();
        UNIT_ASSERT_C(avg == 60, avg);
        value.Set(avg, time);
        auto avg2 = value.GetValue();
        UNIT_ASSERT_C(avg2 == avg, avg2);
        time += TDuration::Minutes(1);
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Minutes(1);
        UNIT_ASSERT(!value.IsValueObsolete(time));
        time += TDuration::Minutes(1);
        UNIT_ASSERT(value.IsValueObsolete(time));
    }

    Y_UNIT_TEST(MaximumValue1) {
        TMaximumValueVariableWindowUI64 maximum;
        TInstant time = TInstant::Now();

        maximum.SetWindowSize(TDuration::MilliSeconds(60000));
        maximum.SetValue(90, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(100, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(80, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(70, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(50, time);

        // +12 seconds
        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);

        // +24 seconds
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);

        // +36 seconds
        time += TDuration::Seconds(2);
        maximum.SetValue(80, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);

        // +48 seconds
        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);

        // +60 seconds
        time += TDuration::Seconds(2);
        maximum.SetValue(60, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 80);
    }

    Y_UNIT_TEST(MaximumValue2) {
        TMaximumValueVariableWindowUI64 maximum;
        TInstant time = TInstant::Now();

        maximum.SetWindowSize(TDuration::MilliSeconds(60000));
        maximum.SetValue(100, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        // +12 seconds
        time += TDuration::Seconds(12);
        maximum.SetValue(60, time);

        // +24 seconds
        time += TDuration::Seconds(12);
        maximum.SetValue(60, time);

        // +36 seconds
        time += TDuration::Seconds(12);
        maximum.SetValue(80, time);

        // +48 seconds
        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        time += TDuration::Seconds(12);
        maximum.SetValue(60, time);

        // +60 seconds
        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        // +72 seconds
        time += TDuration::Seconds(12);
        maximum.SetValue(60, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 80);
    }

    void Dump(const TMaximumValueVariableWindowUI64& m) {
        Cerr << "[";
        for (auto it = m.GetValues().begin(); it != m.GetValues().end(); ++it) {
            if (it != m.GetValues().begin()) {
                Cerr << ",";
            }
            Cerr << *it;
        }
        Cerr << "]" << Endl;
    }

    Y_UNIT_TEST(MaximumValue3) {
        TMaximumValueVariableWindowUI64 maximum;
        TInstant time = TInstant::Now();

        maximum.SetWindowSize(TDuration::MilliSeconds(60000));
        maximum.SetValue(100, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        // +30 seconds
        time += TDuration::Seconds(30);
        maximum.SetValue(80, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        // +60 seconds
        time += TDuration::Seconds(30);
        maximum.SetValue(60, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 80);

        // +90 seconds
        time += TDuration::Seconds(30);
        maximum.SetValue(40, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 60);

        // +120 seconds
        time += TDuration::Seconds(30);
        maximum.SetValue(20, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 40);
    }

    Y_UNIT_TEST(MaximumValue4) {
        TMaximumValueVariableWindowUI64 maximum;
        TInstant time = TInstant::Now();

        maximum.SetWindowSize(TDuration::MilliSeconds(60000));
        maximum.SetValue(100, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 100);

        // +120 seconds
        time += TDuration::Seconds(120);
        maximum.SetValue(80, time);

        UNIT_ASSERT_VALUES_EQUAL(maximum.GetValue(), 80);
    }
}

}
}
