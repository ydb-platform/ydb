#include "tablet_counters.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/xrange.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTabletCountersPercentile) {

    static void CheckValues(const TTabletPercentileCounter& counter, const std::vector<ui64>& gold) {
        UNIT_ASSERT_VALUES_EQUAL(counter.GetRangeCount(), gold.size());

        std::vector<ui64> values;
        values.reserve(gold.size());
        for (auto i: xrange(gold.size())) {
            values.push_back(counter.GetRangeValue(i));
        }

        UNIT_ASSERT_VALUES_EQUAL(values, gold);
    }

    static void CheckRanges(
        const TTabletPercentileCounter& counter,
        const std::vector<TTabletPercentileCounter::TRangeDef>& gold)
    {
        UNIT_ASSERT_VALUES_EQUAL(counter.GetRangeCount(), gold.size());
        for (auto i: xrange(gold.size())) {
            const auto bound = counter.GetRangeBound(i);
            const char* name = counter.GetRangeName(i);
            UNIT_ASSERT_VALUES_EQUAL(bound, gold[i].RangeVal);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(name), TStringBuf(gold[i].RangeName));
        }
    }

    Y_UNIT_TEST(SingleBucket) {
        const TTabletPercentileCounter::TRangeDef ranges[1] = {
            {0, "0"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 1);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);

        counter.IncrementFor(1);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(1000);

        const std::vector<ui64> goldValues = {2, 6};
        CheckValues(counter, goldValues);
    }

    Y_UNIT_TEST(WithoutZero) {
        const TTabletPercentileCounter::TRangeDef ranges[4] = {
            {1, "1"},
            {2, "2"},
            {10, "10"},
            {11, "11"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 4);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);
        counter.IncrementFor(1);

        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);

        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(4);
        counter.IncrementFor(10);
        counter.IncrementFor(10);
        counter.IncrementFor(10);

        counter.IncrementFor(11);

        counter.IncrementFor(12);
        counter.IncrementFor(13);
        counter.IncrementFor(100);
        counter.IncrementFor(10000);

        const std::vector<ui64> goldValues = {3, 5, 6, 1, 4};
        CheckValues(counter, goldValues);
    }

    Y_UNIT_TEST(StartFromZero) {
        const TTabletPercentileCounter::TRangeDef ranges[5] = {
            {0, "0"},
            {1, "1"},
            {2, "2"},
            {10, "10"},
            {11, "11"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 5);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);

        counter.IncrementFor(1);

        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);

        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(4);
        counter.IncrementFor(10);
        counter.IncrementFor(10);
        counter.IncrementFor(10);

        counter.IncrementFor(11);

        counter.IncrementFor(13);
        counter.IncrementFor(100);
        counter.IncrementFor(10000);

        const std::vector<ui64> goldValues = {2, 1, 5, 6, 1, 3};
        CheckValues(counter, goldValues);
    }
}

} // NKikimr
