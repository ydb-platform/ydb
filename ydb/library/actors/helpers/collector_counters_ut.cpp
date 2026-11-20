#include "collector_counters.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {

Y_UNIT_TEST_SUITE(TExecutorPoolCountersTest) {
    Y_UNIT_TEST(NoLegacyRangeHistograms) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TExecutorPoolCounters counters;
        counters.Init(root.Get(), "User", 1);
        auto pool = root->FindSubgroup("execpool", "User");
        UNIT_ASSERT(pool);
        for (TString name : {"ActivationTime", "EventDeliveryTime", "EventProcessingCount", "EventProcessingTime"}) {
            UNIT_ASSERT_C(!pool->FindSubgroup("sensor", name), name);
            UNIT_ASSERT_C(pool->FindHistogram(name + "Us"), name);
        }
    }
}

} // namespace NActors
