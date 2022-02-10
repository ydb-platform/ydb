#include <library/cpp/testing/unittest/registar.h>
#include "time_series_vec.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTimeSeriesVecTest) {
    Y_UNIT_TEST(Smoke) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 3;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        UNIT_ASSERT(tsv.Size() == Size);
        TInstant now = TInstant::Now();
        tsv.Add(now, 10);
        tsv.Add(now, 20);
        UNIT_ASSERT(tsv.Get(now) == 30);
        tsv.Add(now + Interval, 40);
        UNIT_ASSERT(tsv.Get(now) == 30);
        UNIT_ASSERT(tsv.Get(now + Interval) == 40);

        int total = 0;
        for (TInstant i = tsv.Begin(), e = tsv.End(); i != e; i = tsv.Next(i)) {
            total += tsv.Get(i);
        }
        UNIT_ASSERT(total == 70);
    }

    Y_UNIT_TEST(IndexesArePositive) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 3;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        UNIT_ASSERT(tsv.Begin().MicroSeconds() == 0);
        UNIT_ASSERT(tsv.End().MicroSeconds() == Interval.MicroSeconds() * 3);

        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv2(tsv, tsv.Begin(), tsv.End());
        UNIT_ASSERT(tsv2.Begin().MicroSeconds() == 0);
        UNIT_ASSERT(tsv2.End().MicroSeconds() == Interval.MicroSeconds() * 3);
    }

    Y_UNIT_TEST(OverwritingBuffer) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 4;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        UNIT_ASSERT(tsv.Size() == Size);
        TInstant now = TInstant::Now();
        for (ui64 i = 0; i < 10; ++i) {
            tsv.Add(now + Interval * i, i);
        }
        UNIT_ASSERT(tsv.Get(now + Interval * 6) == 6);
        UNIT_ASSERT(tsv.Get(now + Interval * 9) == 9);

        UNIT_ASSERT(tsv.Get(now + Interval * 5) != 5);
    }

    Y_UNIT_TEST(MakeSubseries) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 100;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        UNIT_ASSERT(tsv.Size() == Size);
        ui64 c = 0;
        for (TInstant i = tsv.Begin(), e = tsv.End(); i != e; i = tsv.Next(i)) {
            tsv.Add(i, c++);
        }

        int LastValue = -1;
        for (ui64 i = 0; i < 10; ++i) {
            TTimeSeriesVec<int, Interval.MicroSeconds()> tsv2(tsv, tsv.Begin() + Interval * i * 10, tsv.Begin() + Interval * (i + 1) * 10);
            UNIT_ASSERT(tsv2.Size() == 10);
            UNIT_ASSERT(tsv2.Get(tsv2.Begin()) == LastValue + 1);
            LastValue = tsv2.Get(tsv2.End() - Interval);
        }
    }

    Y_UNIT_TEST(AddValuesSameInterval) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 5;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        ui64 c = 0;
        for (TInstant i = tsv.Begin(), e = tsv.End(); i != e; i = tsv.Next(i)) {
            tsv.Add(i, c++);
        }

        int data[3] = {5, 6, 7};
        tsv.Add(tsv.Interval(), tsv.End(), data, 3);

        UNIT_ASSERT(tsv.Get(tsv.Begin()) == 3);
        UNIT_ASSERT(tsv.Get(tsv.End() - Interval) == 7);

        tsv.Add(tsv.Interval(), tsv.Begin(), data, 3);

        UNIT_ASSERT(tsv.Get(tsv.Begin()) == 8); // 3 + 5
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval) == 10); // 4 + 6
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 2) == 12); // 5 + 7
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 3) == 6); // 6
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 4) == 7); // 7

        int data2[7] = {8, 9, 10, 11, 12, 13, 14};

        tsv.Add(tsv.Interval(), tsv.End() - Interval, data2, 7);

        UNIT_ASSERT(tsv.Get(tsv.Begin()) == 10);
        UNIT_ASSERT(tsv.Get(tsv.End() - Interval) == 14);
    }

    Y_UNIT_TEST(AddValuesCustomInterval) {
        constexpr TDuration Interval = TDuration::MilliSeconds(1);
        constexpr size_t Size = 5;
        TTimeSeriesVec<int, Interval.MicroSeconds()> tsv(Size);
        ui64 c = 0;
        for (TInstant i = tsv.Begin(), e = tsv.End(); i != e; i = tsv.Next(i)) {
            tsv.Add(i, c++);
        }

        int data[3] = {5, 6, 7};
        tsv.Add(Interval * 2, tsv.Begin(), data, 3);

        UNIT_ASSERT(tsv.Get(tsv.Begin()) == 5); // 0 + 5
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval) == 1); // 1
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 2) == 8); // 2 + 6
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 3) == 3); // 3
        UNIT_ASSERT(tsv.Get(tsv.Begin() + Interval * 4) == 11); // 4 + 7
    }
}

} // NKikimr
