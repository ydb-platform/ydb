#include "metric_sub_registry.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TMetricSubRegistryTest) {
    Y_UNIT_TEST(WrapRegistry) {
        TMetricRegistry registry;

        {
            TMetricSubRegistry subRegistry{{{"common", "label"}}, &registry};
            IIntGauge* g = subRegistry.IntGauge(MakeLabels({{"my", "gauge"}}));
            UNIT_ASSERT(g);
            g->Set(42);
        }

        TIntGauge* g = registry.IntGauge({{"my", "gauge"}, {"common", "label"}});
        UNIT_ASSERT(g);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 42);
    }

    Y_UNIT_TEST(CommonLabelsDoNotOverrideGeneralLabel) {
        TMetricRegistry registry;

        {
            TMetricSubRegistry subRegistry{{{"common", "label"}, {"my", "notOverride"}}, &registry};
            IIntGauge* g = subRegistry.IntGauge(MakeLabels({{"my", "gauge"}}));
            UNIT_ASSERT(g);
            g->Set(1234);
        }

        TIntGauge* knownGauge = registry.IntGauge({{"my", "gauge"}, {"common", "label"}});
        UNIT_ASSERT(knownGauge);
        UNIT_ASSERT_VALUES_EQUAL(knownGauge->Get(), 1234);

        TIntGauge* newGauge = registry.IntGauge({{"common", "label"}, {"my", "notOverride"}});
        UNIT_ASSERT(newGauge);
        UNIT_ASSERT_VALUES_EQUAL(newGauge->Get(), 0);
    }

    Y_UNIT_TEST(RemoveMetric) {
        TMetricRegistry registry;

        {
            TMetricSubRegistry subRegistry{{{"common", "label"}}, &registry};
            IIntGauge* g = subRegistry.IntGauge(MakeLabels({{"my", "gauge"}}));
            UNIT_ASSERT(g);
            g->Set(1234);
        }

        IIntGauge* g1 = registry.IntGauge({{"my", "gauge"}, {"common", "label"}});
        UNIT_ASSERT(g1);
        UNIT_ASSERT_VALUES_EQUAL(g1->Get(), 1234);

        {
            TMetricSubRegistry subRegistry{{{"common", "label"}}, &registry};
            subRegistry.RemoveMetric(TLabels{{"my", "gauge"}});
        }

        IIntGauge* g2 = registry.IntGauge({{"my", "gauge"}, {"common", "label"}});
        UNIT_ASSERT(g2);
        UNIT_ASSERT_VALUES_EQUAL(g2->Get(), 0);
    }
}
