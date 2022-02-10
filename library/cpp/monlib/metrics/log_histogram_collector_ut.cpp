#include "log_histogram_collector.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(LogHistogramCollector) {

    Y_UNIT_TEST(ExtendUpEmpty) {
        NMonitoring::TLogHistogramCollector collector(-1);
        collector.Collect(4.1944122207138854e+17);
        auto s = collector.Snapshot();
        UNIT_ASSERT_EQUAL(s->ZerosCount(), 0);
        UNIT_ASSERT_EQUAL(s->StartPower(), 1);
        UNIT_ASSERT_EQUAL(s->Count(), 100);
        UNIT_ASSERT_EQUAL(s->Bucket(s->Count() - 1), 1);
    }

    Y_UNIT_TEST(ExtendUpNonEmpty) {
        NMonitoring::TLogHistogramCollector collector(-1);
        collector.Collect(0.0);
        collector.Collect(1/(1.5*1.5*1.5));
        collector.Collect(1/1.5);
        auto s = collector.Snapshot();

        UNIT_ASSERT_EQUAL(s->ZerosCount(), 1);
        UNIT_ASSERT_EQUAL(s->StartPower(), -4);
        UNIT_ASSERT_EQUAL(s->Count(), 3);
        UNIT_ASSERT_EQUAL(s->Bucket(1), 1);
        UNIT_ASSERT_EQUAL(s->Bucket(2), 1);

        collector.Collect(4.1944122207138854e+17);
        s = collector.Snapshot();
        UNIT_ASSERT_EQUAL(s->ZerosCount(), 1);
        UNIT_ASSERT_EQUAL(s->StartPower(), 1);
        UNIT_ASSERT_EQUAL(s->Count(), 100);
        UNIT_ASSERT_EQUAL(s->Bucket(0), 2);
        UNIT_ASSERT_EQUAL(s->Bucket(99), 1);
    }
}
