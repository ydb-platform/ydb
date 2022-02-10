#include "hyperlog_counter.h"

#include <util/random/random.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(THyperLogCounterTest) {
    Y_UNIT_TEST(TestGetSet) {
        THyperLogCounter c1;
        THyperLogCounter c2(17);
        THyperLogCounter c3(63);
        UNIT_ASSERT_EQUAL(c1.GetValue(), 0);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 0);
        UNIT_ASSERT_EQUAL(c2.GetValue(), 17);
        UNIT_ASSERT_EQUAL(c2.GetEstimatedCounter(), 262142);
        UNIT_ASSERT_EQUAL(c3.GetValue(), 63);
        UNIT_ASSERT_EQUAL(c3.GetEstimatedCounter(), ui64(-2));
    }

    Y_UNIT_TEST(TestIncrement) {
        THyperLogCounter c;
        auto randomProvider = CreateDefaultRandomProvider();
        const ui32 n = 1000000;
        ui32 countOfChanges = 0;
        double sumOfDeltas = 0;
        double sumOfQDeltas = 0;
        double minDelta = Max<double>();
        double maxDelta = Min<double>();
        ui32 count = 0;
        for (ui32 i = 0; i < n;) {
            if (c.Increment(*randomProvider))
                ++countOfChanges;

            ++i;
            if (true || i >= n / 2) {
                ++count;
                i64 effectiveCounter = c.GetEstimatedCounter();
                double delta = 1.0 * (effectiveCounter - i64(i)) / i;
                minDelta = Min(minDelta, delta);
                maxDelta = Max(maxDelta, delta);
                sumOfDeltas += delta;
                sumOfQDeltas += delta * delta;
            }
        }

        double avg = sumOfDeltas / count;
        double dev = sqrt((sumOfQDeltas - sumOfDeltas * sumOfDeltas / count) / (count - 1));
        Cout << "n: " << n << ", count of changes: " << countOfChanges << ", last value: " << (ui32)c.GetValue() << "\n";
        Cout << "min. delta: " << minDelta << ", max. delta: " << maxDelta << ", avg. : " << avg << ", dev. : " << dev << "\n";
    }

    Y_UNIT_TEST(TestAddRandom) {
        THyperLogCounter c;
        auto randomProvider = CreateDefaultRandomProvider();
        const ui32 n = 1000000;
        ui32 countOfChanges = 0;
        double sumOfDeltas = 0;
        double sumOfQDeltas = 0;
        double minDelta = Max<double>();
        double maxDelta = Min<double>();
        ui32 count = 0;
        i64 expectedValue = 0;
        for (ui32 i = 0; i < n;) {
            ui64 rndAdd = RandomNumber<ui64>(1000);
            if (c.Add(rndAdd, *randomProvider))
                ++countOfChanges;

            expectedValue += rndAdd;
            ++i;
            if (true || i >= n / 2) {
                ++count;
                i64 effectiveCounter = c.GetEstimatedCounter();
                double delta = 1.0 * (effectiveCounter - expectedValue) / expectedValue;
                minDelta = Min(minDelta, delta);
                maxDelta = Max(maxDelta, delta);
                sumOfDeltas += delta;
                sumOfQDeltas += delta * delta;
            }
        }

        double avg = sumOfDeltas / count;
        double dev = sqrt((sumOfQDeltas - sumOfDeltas * sumOfDeltas / count) / (count - 1));
        Cout << "n: " << n << ", sum: " << expectedValue << ", count of changes: " << countOfChanges << ", last value: " << (ui32)c.GetValue() << "\n";
        Cout << "min. delta: " << minDelta << ", max. delta: " << maxDelta << ", avg. : " << avg << ", dev. : " << dev << "\n";
    }

    Y_UNIT_TEST(TestAddFixed) {
        auto randomProvider = CreateDefaultRandomProvider();
        THyperLogCounter c;
        const ui32 n = 1000000;
        ui32 countOfChanges = 0;
        double sumOfDeltas = 0;
        double sumOfQDeltas = 0;
        double minDelta = Max<double>();
        double maxDelta = Min<double>();
        ui32 count = 0;
        i64 expectedValue = 0;
        for (ui32 i = 0; i < n;) {
            ui64 rndAdd = 1024;
            if (c.Add(rndAdd, *randomProvider))
                ++countOfChanges;

            expectedValue += rndAdd;
            ++i;
            if (true || i >= n / 2) {
                ++count;
                i64 effectiveCounter = c.GetEstimatedCounter();
                double delta = 1.0 * (effectiveCounter - expectedValue) / expectedValue;
                minDelta = Min(minDelta, delta);
                maxDelta = Max(maxDelta, delta);
                sumOfDeltas += delta;
                sumOfQDeltas += delta * delta;
            }
        }

        double avg = sumOfDeltas / count;
        double dev = sqrt((sumOfQDeltas - sumOfDeltas * sumOfDeltas / count) / (count - 1));
        Cout << "n: " << n << ", sum: " << expectedValue << ", count of changes: " << countOfChanges << ", last value: " << (ui32)c.GetValue() << "\n";
        Cout << "min. delta: " << minDelta << ", max. delta: " << maxDelta << ", avg. : " << avg << ", dev. : " << dev << "\n";
    }

    Y_UNIT_TEST(TestHybridIncrement) {
        auto randomProvider = CreateDefaultRandomProvider();
        THybridLogCounter c1;
        UNIT_ASSERT_EQUAL(c1.GetValue(), 0);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 0);
        for (ui32 i = 0; i < 128; ++i) {
            UNIT_ASSERT_EQUAL(c1.GetValue(), i);
            UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), i);
            UNIT_ASSERT(c1.Increment(*randomProvider));
        }

        UNIT_ASSERT_EQUAL(c1.GetValue(), THybridLogCounter::IsHyperLog | 6);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 126);
        for (ui32 i = 0; i < 128; ++i) {
            UNIT_ASSERT((c1.GetValue() & THybridLogCounter::IsHyperLog) != 0);
            c1.Increment(*randomProvider);
        }
    }

    Y_UNIT_TEST(TestHybridAdd) {
        auto randomProvider = CreateDefaultRandomProvider();
        THybridLogCounter c1;
        UNIT_ASSERT(c1.Add(23, *randomProvider));
        UNIT_ASSERT_EQUAL(c1.GetValue(), 23);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 23);
        UNIT_ASSERT(c1.Add(100, *randomProvider));
        UNIT_ASSERT_EQUAL(c1.GetValue(), 123);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 123);
        UNIT_ASSERT(c1.Add(100, *randomProvider));
        UNIT_ASSERT_EQUAL(c1.GetValue(), THybridLogCounter::IsHyperLog | 7);
        UNIT_ASSERT_EQUAL(c1.GetEstimatedCounter(), 254);

        THybridLogCounter c2(120);
        UNIT_ASSERT(c2.Add(10, *randomProvider));
        UNIT_ASSERT_EQUAL(c2.GetValue(), THybridLogCounter::IsHyperLog | 6);
        UNIT_ASSERT_EQUAL(c2.GetEstimatedCounter(), 126);

        THybridLogCounter c3;
        //for (ui32 i = 0; i < 5000; ++i)
        //    c3.Increment();
        c3.Add(5000, *randomProvider);
        ui64 est1 = c3.GetEstimatedCounter();
        Cout << "0 + 5000 => " << est1 << "\n";
        c3.Add(25000, *randomProvider);
        ui64 est2 = c3.GetEstimatedCounter();
        Cout << "5000 + 25000 => " << est2 << "\n";
        UNIT_ASSERT(est2 >= est1);
    }
}

} // NKikimr
