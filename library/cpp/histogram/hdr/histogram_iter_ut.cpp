#include "histogram_iter.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NHdr;

Y_UNIT_TEST_SUITE(THistogramIterTest) {
    Y_UNIT_TEST(RecordedValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValues(1000, 1000));
        UNIT_ASSERT(h.RecordValue(1000 * 1000));

        int index = 0;
        TRecordedValuesIterator it(h);

        while (it.Next()) {
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();
            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1000);
                UNIT_ASSERT_EQUAL(countInStep, 1000);
            } else if (index == 1) {
                UNIT_ASSERT_EQUAL(countInBucket, 1);
                UNIT_ASSERT_EQUAL(countInStep, 1);
            } else {
                UNIT_FAIL("unexpected index value: " << index);
            }

            index++;
        }

        UNIT_ASSERT_EQUAL(index, 2);
    }

    Y_UNIT_TEST(CorrectedRecordedValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValuesWithExpectedInterval(1000, 1000, 1000));
        UNIT_ASSERT(h.RecordValueWithExpectedInterval(1000 * 1000, 1000));

        int index = 0;
        i64 totalCount = 0;
        TRecordedValuesIterator it(h);

        while (it.Next()) {
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();
            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1001);
                UNIT_ASSERT_EQUAL(countInStep, 1001);
            } else {
                UNIT_ASSERT(countInBucket >= 1);
                UNIT_ASSERT(countInStep >= 1);
            }
            index++;
            totalCount += countInStep;
        }

        UNIT_ASSERT_EQUAL(index, 1000);
        UNIT_ASSERT_EQUAL(totalCount, 2000);
    }

    Y_UNIT_TEST(LinearValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValues(1000, 1000));
        UNIT_ASSERT(h.RecordValue(1000 * 1000));

        int index = 0;
        TLinearIterator it(h, 1000);

        while (it.Next()) {
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();
            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1000);
                UNIT_ASSERT_EQUAL(countInStep, 1000);
            } else if (index == 999) {
                UNIT_ASSERT_EQUAL(countInBucket, 1);
                UNIT_ASSERT_EQUAL(countInStep, 1);
            } else {
                UNIT_ASSERT_EQUAL(countInBucket, 0);
                UNIT_ASSERT_EQUAL(countInStep, 0);
            }

            index++;
        }

        UNIT_ASSERT_EQUAL(index, 1000);
    }

    Y_UNIT_TEST(CorrectLinearValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValuesWithExpectedInterval(1000, 1000, 1000));
        UNIT_ASSERT(h.RecordValueWithExpectedInterval(1000 * 1000, 1000));

        int index = 0;
        i64 totalCount = 0;
        TLinearIterator it(h, 1000);

        while (it.Next()) {
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();

            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1001);
                UNIT_ASSERT_EQUAL(countInStep, 1001);
            } else {
                UNIT_ASSERT_EQUAL(countInBucket, 1);
                UNIT_ASSERT_EQUAL(countInStep, 1);
            }

            index++;
            totalCount += countInStep;
        }

        UNIT_ASSERT_EQUAL(index, 1000);
        UNIT_ASSERT_EQUAL(totalCount, 2000);
    }

    Y_UNIT_TEST(LogarithmicValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValues(1000, 1000));
        UNIT_ASSERT(h.RecordValue(1000 * 1000));

        int index = 0;
        i64 expectedValue = 1000;
        TLogarithmicIterator it(h, 1000, 2.0);

        while (it.Next()) {
            i64 value = it.GetValue();
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();

            UNIT_ASSERT_EQUAL(value, expectedValue);

            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1000);
                UNIT_ASSERT_EQUAL(countInStep, 1000);
            } else if (index == 10) {
                UNIT_ASSERT_EQUAL(countInBucket, 0);
                UNIT_ASSERT_EQUAL(countInStep, 1);
            } else {
                UNIT_ASSERT_EQUAL(countInBucket, 0);
                UNIT_ASSERT_EQUAL(countInStep, 0);
            }

            index++;
            expectedValue *= 2;
        }

        UNIT_ASSERT_EQUAL(index, 11);
    }

    Y_UNIT_TEST(CorrectedLogarithmicValues) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValuesWithExpectedInterval(1000, 1000, 1000));
        UNIT_ASSERT(h.RecordValueWithExpectedInterval(1000 * 1000, 1000));

        int index = 0;
        i64 totalCount = 0;
        i64 expectedValue = 1000;
        TLogarithmicIterator it(h, 1000, 2.0);

        while (it.Next()) {
            i64 value = it.GetValue();
            i64 countInBucket = it.GetCount();
            i64 countInStep = it.GetCountAddedInThisIterationStep();

            UNIT_ASSERT_EQUAL(value, expectedValue);

            if (index == 0) {
                UNIT_ASSERT_EQUAL(countInBucket, 1001);
                UNIT_ASSERT_EQUAL(countInStep, 1001);
            }

            index++;
            totalCount += countInStep;
            expectedValue *= 2;
        }

        UNIT_ASSERT_EQUAL(index, 11);
        UNIT_ASSERT_EQUAL(totalCount, 2000);
    }

    Y_UNIT_TEST(LinearIterBucketsCorrectly) {
        THistogram h(255, 2);
        UNIT_ASSERT(h.RecordValue(193));
        UNIT_ASSERT(h.RecordValue(255));
        UNIT_ASSERT(h.RecordValue(0));
        UNIT_ASSERT(h.RecordValue(1));
        UNIT_ASSERT(h.RecordValue(64));
        UNIT_ASSERT(h.RecordValue(128));

        int index = 0;
        i64 totalCount = 0;
        TLinearIterator it(h, 64);

        while (it.Next()) {
            if (index == 0) {
                // change after iterator was created
                UNIT_ASSERT(h.RecordValue(2));
            }

            index++;
            totalCount += it.GetCountAddedInThisIterationStep();
        }

        UNIT_ASSERT_EQUAL(index, 4);
        UNIT_ASSERT_EQUAL(totalCount, 6);
    }
}
