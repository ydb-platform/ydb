#include <ydb/library/signals/agent.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard {

Y_UNIT_TEST_SUITE(TValueAggregationAgentTest) {
    Y_UNIT_TEST(MinMaxSum) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TCommonCountersOwner owner("test", root);
        TValueAggregationAgent agent("Value", owner);
        // Released at once: the agent holds the last reference and must skip it.
        agent.GetClient()->SetValue(100);
        auto low = agent.GetClient();
        auto high = agent.GetClient();
        low->SetValue(3);
        high->SetValue(7);
        agent.ResendStatus();
        auto group = root->FindSubgroup("module_id", "test");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("Aggregation/MIN/Value")->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("Aggregation/MAX/Value")->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("Aggregation/SUM/Value")->Val(), 10);
    }
}

} // namespace NKikimr::NColumnShard
