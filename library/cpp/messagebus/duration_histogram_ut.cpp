#include <library/cpp/testing/unittest/registar.h>

#include "duration_histogram.h"

Y_UNIT_TEST_SUITE(TDurationHistogramTest) {
    Y_UNIT_TEST(BucketFor) {
        UNIT_ASSERT_VALUES_EQUAL(0u, TDurationHistogram::BucketFor(TDuration::MicroSeconds(0)));
        UNIT_ASSERT_VALUES_EQUAL(0u, TDurationHistogram::BucketFor(TDuration::MicroSeconds(1)));
        UNIT_ASSERT_VALUES_EQUAL(0u, TDurationHistogram::BucketFor(TDuration::MicroSeconds(900)));
        UNIT_ASSERT_VALUES_EQUAL(1u, TDurationHistogram::BucketFor(TDuration::MicroSeconds(1500)));
        UNIT_ASSERT_VALUES_EQUAL(2u, TDurationHistogram::BucketFor(TDuration::MicroSeconds(2500)));

        unsigned sb = TDurationHistogram::SecondBoundary;

        UNIT_ASSERT_VALUES_EQUAL(sb - 1, TDurationHistogram::BucketFor(TDuration::MilliSeconds(999)));
        UNIT_ASSERT_VALUES_EQUAL(sb, TDurationHistogram::BucketFor(TDuration::MilliSeconds(1000)));
        UNIT_ASSERT_VALUES_EQUAL(sb, TDurationHistogram::BucketFor(TDuration::MilliSeconds(1001)));

        UNIT_ASSERT_VALUES_EQUAL(TDurationHistogram::Buckets - 1, TDurationHistogram::BucketFor(TDuration::Hours(1)));
    }

    Y_UNIT_TEST(Simple) {
        TDurationHistogram h1;
        h1.AddTime(TDuration::MicroSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1u, h1.Times.front());

        TDurationHistogram h2;
        h1.AddTime(TDuration::Hours(1));
        UNIT_ASSERT_VALUES_EQUAL(1u, h1.Times.back());
    }

    Y_UNIT_TEST(LabelFor) {
        for (unsigned i = 0; i < TDurationHistogram::Buckets; ++i) {
            TDurationHistogram::LabelBefore(i);
            //Cerr << TDurationHistogram::LabelBefore(i) << "\n";
        }
    }
}
