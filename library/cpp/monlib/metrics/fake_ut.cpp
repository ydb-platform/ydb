#include "fake.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TFakeTest) {

    Y_UNIT_TEST(CreateOnStack) {
        TFakeMetricRegistry registry;
    }

    Y_UNIT_TEST(CreateOnHeap) {
        auto registry = MakeAtomicShared<TFakeMetricRegistry>();
        UNIT_ASSERT(registry);
    }

    Y_UNIT_TEST(Gauge) {
        TFakeMetricRegistry registry(TLabels{{"common", "label"}});

        IGauge* g = registry.Gauge(MakeLabels({{"my", "gauge"}}));
        UNIT_ASSERT(g);

        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 0.0, 1E-6);
        g->Set(12.34);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 0.0, 1E-6); // no changes

        double val = g->Add(1.2);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 0.0, 1E-6);
        UNIT_ASSERT_DOUBLES_EQUAL(val, 0.0, 1E-6);
    }
}
