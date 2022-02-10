#include "metric_value.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TMetricValueTest) {

    class TTestHistogram: public IHistogramSnapshot {
    public:
        TTestHistogram(ui32 count = 1)
            : Count_{count}
        {}

    private:
        ui32 Count() const override {
            return Count_;
        }

        TBucketBound UpperBound(ui32 /*index*/) const override {
            return 1234.56;
        }

        TBucketValue Value(ui32 /*index*/) const override {
            return 42;
        }

        ui32 Count_{0};
    };

    IHistogramSnapshotPtr MakeHistogramSnapshot() {
        return MakeIntrusive<TTestHistogram>();
    }

    ISummaryDoubleSnapshotPtr MakeSummarySnapshot(ui64 count = 0u) {
        return MakeIntrusive<TSummaryDoubleSnapshot>(0.0, 0.0, 0.0, 0.0, count);
    }

    TLogHistogramSnapshotPtr MakeLogHistogram(ui64 count = 0) {
        TVector<double> buckets;
        for (ui64 i = 0; i < count; ++i) {
            buckets.push_back(i);
        }
        return MakeIntrusive<TLogHistogramSnapshot>(1.5, 0u, 0, buckets);
    }

    Y_UNIT_TEST(Sorted) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);

        TMetricTimeSeries timeSeries;
        timeSeries.Add(ts1, 3.14159);
        timeSeries.Add(ts1, 6.28318);
        timeSeries.Add(ts2, 2.71828);

        UNIT_ASSERT_EQUAL(timeSeries.Size(), 3);

        timeSeries.SortByTs();
        UNIT_ASSERT_EQUAL(timeSeries.Size(), 2);

        UNIT_ASSERT_EQUAL(ts1, timeSeries[0].GetTime());
        UNIT_ASSERT_DOUBLES_EQUAL(6.28318, timeSeries[0].GetValue().AsDouble(), Min<double>());

        UNIT_ASSERT_EQUAL(ts2, timeSeries[1].GetTime());
        UNIT_ASSERT_DOUBLES_EQUAL(2.71828, timeSeries[1].GetValue().AsDouble(), Min<double>());
    }

    Y_UNIT_TEST(Histograms) {
        auto ts = TInstant::Now();
        auto histogram = MakeIntrusive<TTestHistogram>();

        UNIT_ASSERT_VALUES_EQUAL(1, histogram->RefCount());
        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts, histogram.Get());
            UNIT_ASSERT_VALUES_EQUAL(2, histogram->RefCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, histogram->RefCount());
    }

    Y_UNIT_TEST(Summary) {
        auto ts = TInstant::Now();
        auto summary = MakeSummarySnapshot();
        UNIT_ASSERT_VALUES_EQUAL(1, summary->RefCount());
        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts, summary.Get());
            UNIT_ASSERT_VALUES_EQUAL(2, summary->RefCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, summary->RefCount());
    }

    Y_UNIT_TEST(LogHistogram) {
        auto ts = TInstant::Now();
        auto logHist = MakeLogHistogram();
        UNIT_ASSERT_VALUES_EQUAL(1, logHist->RefCount());
        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts, logHist.Get());
            UNIT_ASSERT_VALUES_EQUAL(2, logHist->RefCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, logHist->RefCount());
    }

    Y_UNIT_TEST(TimeSeriesMovable) {
        auto ts = TInstant::Now();
        auto histogram = MakeIntrusive<TTestHistogram>();

        UNIT_ASSERT_VALUES_EQUAL(1, histogram->RefCount());
        {
            TMetricTimeSeries timeSeriesA;
            timeSeriesA.Add(ts, histogram.Get());
            UNIT_ASSERT_VALUES_EQUAL(2, histogram->RefCount());

            TMetricTimeSeries timeSeriesB = std::move(timeSeriesA);
            UNIT_ASSERT_VALUES_EQUAL(2, histogram->RefCount());

            UNIT_ASSERT_VALUES_EQUAL(1, timeSeriesB.Size());
            UNIT_ASSERT_EQUAL(EMetricValueType::HISTOGRAM, timeSeriesB.GetValueType());

            UNIT_ASSERT_VALUES_EQUAL(0, timeSeriesA.Size());
            UNIT_ASSERT_EQUAL(EMetricValueType::UNKNOWN, timeSeriesA.GetValueType());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, histogram->RefCount());
    }

    Y_UNIT_TEST(HistogramsUnique) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);

        auto h1 = MakeIntrusive<TTestHistogram>();
        auto h2 = MakeIntrusive<TTestHistogram>();
        auto h3 = MakeIntrusive<TTestHistogram>();

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get()); // drop at the head
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h1.Get());

            timeSeries.Add(ts2, h2.Get()); // drop in the middle
            timeSeries.Add(ts2, h2.Get());
            timeSeries.Add(ts2, h2.Get());

            timeSeries.Add(ts3, h3.Get()); // drop at the end
            timeSeries.Add(ts3, h3.Get());
            timeSeries.Add(ts3, h3.Get());

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 9);

            UNIT_ASSERT_VALUES_EQUAL(4, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h3->RefCount());

            timeSeries.SortByTs();
            UNIT_ASSERT_EQUAL(timeSeries.Size(), 3);

            UNIT_ASSERT_VALUES_EQUAL(2, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h3->RefCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());
    }

    Y_UNIT_TEST(LogHistogramsUnique) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);

        auto h1 = MakeLogHistogram();
        auto h2 = MakeLogHistogram();
        auto h3 = MakeLogHistogram();

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get()); // drop at the head
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h1.Get());

            timeSeries.Add(ts2, h2.Get()); // drop in the middle
            timeSeries.Add(ts2, h2.Get());
            timeSeries.Add(ts2, h2.Get());

            timeSeries.Add(ts3, h3.Get()); // drop at the end
            timeSeries.Add(ts3, h3.Get());
            timeSeries.Add(ts3, h3.Get());

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 9);

            UNIT_ASSERT_VALUES_EQUAL(4, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h3->RefCount());

            timeSeries.SortByTs();
            UNIT_ASSERT_EQUAL(timeSeries.Size(), 3);

            UNIT_ASSERT_VALUES_EQUAL(2, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h3->RefCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());
    }

    Y_UNIT_TEST(SummaryUnique) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);

        auto h1 = MakeSummarySnapshot();
        auto h2 = MakeSummarySnapshot();
        auto h3 = MakeSummarySnapshot();

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get()); // drop at the head
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h1.Get());

            timeSeries.Add(ts2, h2.Get()); // drop in the middle
            timeSeries.Add(ts2, h2.Get());
            timeSeries.Add(ts2, h2.Get());

            timeSeries.Add(ts3, h3.Get()); // drop at the end
            timeSeries.Add(ts3, h3.Get());
            timeSeries.Add(ts3, h3.Get());

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 9);

            UNIT_ASSERT_VALUES_EQUAL(4, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(4, h3->RefCount());

            timeSeries.SortByTs();
            UNIT_ASSERT_EQUAL(timeSeries.Size(), 3);

            UNIT_ASSERT_VALUES_EQUAL(2, h1->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h2->RefCount());
            UNIT_ASSERT_VALUES_EQUAL(2, h3->RefCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, h1->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h2->RefCount());
        UNIT_ASSERT_VALUES_EQUAL(1, h3->RefCount());
    }

    Y_UNIT_TEST(HistogramsUnique2) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);
        auto ts4 = ts3 + TDuration::Seconds(1);
        auto ts5 = ts4 + TDuration::Seconds(1);

        auto h1 = MakeIntrusive<TTestHistogram>(1u);
        auto h2 = MakeIntrusive<TTestHistogram>(2u);
        auto h3 = MakeIntrusive<TTestHistogram>(3u);
        auto h4 = MakeIntrusive<TTestHistogram>(4u);
        auto h5 = MakeIntrusive<TTestHistogram>(5u);
        auto h6 = MakeIntrusive<TTestHistogram>(6u);
        auto h7 = MakeIntrusive<TTestHistogram>(7u);

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h2.Get());

            timeSeries.Add(ts2, h3.Get());

            timeSeries.Add(ts3, h4.Get());
            timeSeries.Add(ts3, h5.Get());

            timeSeries.Add(ts4, h6.Get());
            timeSeries.Add(ts5, h7.Get());

            timeSeries.SortByTs();

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[0].GetValue().AsHistogram()->Count(), 2);
            UNIT_ASSERT_EQUAL(timeSeries[1].GetValue().AsHistogram()->Count(), 3);
            UNIT_ASSERT_EQUAL(timeSeries[2].GetValue().AsHistogram()->Count(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[3].GetValue().AsHistogram()->Count(), 6);
            UNIT_ASSERT_EQUAL(timeSeries[4].GetValue().AsHistogram()->Count(), 7);
        }
    }

    Y_UNIT_TEST(LogHistogramsUnique2) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);
        auto ts4 = ts3 + TDuration::Seconds(1);
        auto ts5 = ts4 + TDuration::Seconds(1);

        auto h1 = MakeLogHistogram(1u);
        auto h2 = MakeLogHistogram(2u);
        auto h3 = MakeLogHistogram(3u);
        auto h4 = MakeLogHistogram(4u);
        auto h5 = MakeLogHistogram(5u);
        auto h6 = MakeLogHistogram(6u);
        auto h7 = MakeLogHistogram(7u);

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h2.Get());

            timeSeries.Add(ts2, h3.Get());

            timeSeries.Add(ts3, h4.Get());
            timeSeries.Add(ts3, h5.Get());

            timeSeries.Add(ts4, h6.Get());
            timeSeries.Add(ts5, h7.Get());

            timeSeries.SortByTs();

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[0].GetValue().AsLogHistogram()->Count(), 2);
            UNIT_ASSERT_EQUAL(timeSeries[1].GetValue().AsLogHistogram()->Count(), 3);
            UNIT_ASSERT_EQUAL(timeSeries[2].GetValue().AsLogHistogram()->Count(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[3].GetValue().AsLogHistogram()->Count(), 6);
            UNIT_ASSERT_EQUAL(timeSeries[4].GetValue().AsLogHistogram()->Count(), 7);
        }
    }

    Y_UNIT_TEST(SummaryUnique2) {
        auto ts1 = TInstant::Now();
        auto ts2 = ts1 + TDuration::Seconds(1);
        auto ts3 = ts2 + TDuration::Seconds(1);
        auto ts4 = ts3 + TDuration::Seconds(1);
        auto ts5 = ts4 + TDuration::Seconds(1);

        auto h1 = MakeSummarySnapshot(1u);
        auto h2 = MakeSummarySnapshot(2u);
        auto h3 = MakeSummarySnapshot(3u);
        auto h4 = MakeSummarySnapshot(4u);
        auto h5 = MakeSummarySnapshot(5u);
        auto h6 = MakeSummarySnapshot(6u);
        auto h7 = MakeSummarySnapshot(7u);

        {
            TMetricTimeSeries timeSeries;
            timeSeries.Add(ts1, h1.Get());
            timeSeries.Add(ts1, h2.Get());

            timeSeries.Add(ts2, h3.Get());

            timeSeries.Add(ts3, h4.Get());
            timeSeries.Add(ts3, h5.Get());

            timeSeries.Add(ts4, h6.Get());
            timeSeries.Add(ts5, h7.Get());

            timeSeries.SortByTs();

            UNIT_ASSERT_EQUAL(timeSeries.Size(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[0].GetValue().AsSummaryDouble()->GetCount(), 2);
            UNIT_ASSERT_EQUAL(timeSeries[1].GetValue().AsSummaryDouble()->GetCount(), 3);
            UNIT_ASSERT_EQUAL(timeSeries[2].GetValue().AsSummaryDouble()->GetCount(), 5);
            UNIT_ASSERT_EQUAL(timeSeries[3].GetValue().AsSummaryDouble()->GetCount(), 6);
            UNIT_ASSERT_EQUAL(timeSeries[4].GetValue().AsSummaryDouble()->GetCount(), 7);
        }
    }

    Y_UNIT_TEST(TMetricValueWithType) {
        // correct usage
        {
            double value = 1.23;
            TMetricValueWithType v{value};

            UNIT_ASSERT_VALUES_EQUAL(v.GetType(), EMetricValueType::DOUBLE);
            UNIT_ASSERT_VALUES_EQUAL(v.AsDouble(), value);
        }
        {
            ui64 value = 12;
            TMetricValueWithType v{value};

            UNIT_ASSERT_VALUES_EQUAL(v.GetType(), EMetricValueType::UINT64);
            UNIT_ASSERT_VALUES_EQUAL(v.AsUint64(), value);
        }
        {
            i64 value = i64(-12);
            TMetricValueWithType v{value};

            UNIT_ASSERT_VALUES_EQUAL(v.GetType(), EMetricValueType::INT64);
            UNIT_ASSERT_VALUES_EQUAL(v.AsInt64(), value);
        }
        {
            auto h = MakeHistogramSnapshot();
            UNIT_ASSERT_VALUES_EQUAL(h.RefCount(), 1);

            {
                auto value = h.Get();
                TMetricValueWithType v{value};

                UNIT_ASSERT_VALUES_EQUAL(h.RefCount(), 2);

                UNIT_ASSERT_VALUES_EQUAL(v.GetType(), EMetricValueType::HISTOGRAM);
                UNIT_ASSERT_VALUES_EQUAL(v.AsHistogram(), value);
            }

            UNIT_ASSERT_VALUES_EQUAL(h.RefCount(), 1);
        }
        {
            auto s = MakeSummarySnapshot();
            auto value = s.Get();

            UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);

            {
                TMetricValueWithType v{value};

                UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 2);

                UNIT_ASSERT_VALUES_EQUAL(v.GetType(), EMetricValueType::SUMMARY);
                UNIT_ASSERT_VALUES_EQUAL(v.AsSummaryDouble(), value);
            }

            UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);
        }
        {
            auto s = MakeSummarySnapshot();
            auto value = s.Get();

            UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);

            {
                TMetricValueWithType v{value};

                UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 2);

                v.Clear();
                UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);
            }

            UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);
        }
        {
            auto s = MakeSummarySnapshot();
            auto value = s.Get();

            {
                TMetricValueWithType v1{ui64{1}};

                UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);

                {
                    TMetricValueWithType v2{value};
                    UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 2);

                    v1 = std::move(v2);
                    UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(v1.AsSummaryDouble(), value);
                    UNIT_ASSERT_VALUES_EQUAL(v1.GetType(), EMetricValueType::SUMMARY);
                    UNIT_ASSERT_VALUES_EQUAL(v2.GetType(), EMetricValueType::UNKNOWN);
                }

                UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 2);
            }

            UNIT_ASSERT_VALUES_EQUAL(s.RefCount(), 1);
        }

        // incorrect usage
        {
            TMetricValueWithType v{1.23};

            UNIT_ASSERT_EXCEPTION(v.AsHistogram(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsSummaryDouble(), yexception);
        }
        {
            auto h = MakeHistogramSnapshot();
            TMetricValueWithType v{h.Get()};

            UNIT_ASSERT_EXCEPTION(v.AsUint64(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsInt64(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsDouble(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsSummaryDouble(), yexception);
        }
        {
            auto s = MakeSummarySnapshot();
            TMetricValueWithType v{s.Get()};

            UNIT_ASSERT_EXCEPTION(v.AsUint64(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsInt64(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsDouble(), yexception);
            UNIT_ASSERT_EXCEPTION(v.AsHistogram(), yexception);
        }
    }
}
