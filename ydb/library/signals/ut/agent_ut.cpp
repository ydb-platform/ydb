#include <ydb/library/signals/agent.h>
#include <ydb/library/signals/node_counters.h>
#include <ydb/library/signals/owner.h>

#include <ydb/core/base/counters.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NKikimr::NColumnShard {

TEST(TValueAggregationAgentTest, MinMaxSum) {
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
    ASSERT_TRUE(group);
    EXPECT_EQ(group->FindCounter("Aggregation/MIN/Value")->Val(), 3);
    EXPECT_EQ(group->FindCounter("Aggregation/MAX/Value")->Val(), 7);
    EXPECT_EQ(group->FindCounter("Aggregation/SUM/Value")->Val(), 10);
}

TEST(TNodeServiceCountersTest, HiddenUntilExport) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto group = GetNodeServiceCounters(root, "TX_TEST");
    group->GetCounter("Value")->Set(5);
    EXPECT_FALSE(NMonitoring::ToJson(*root).Contains("TX_TEST"));

    ExportNodeServiceCounters(root);
    ExportNodeServiceCounters(root);
    EXPECT_EQ(GetServiceCounters(root, "tablets")->FindSubgroup("type", "TX_TEST"), group);
    EXPECT_EQ(GetNodeServiceCounters(root, "TX_TEST"), group);
    EXPECT_EQ(group->GetCounter("Value")->Val(), 5);
    EXPECT_TRUE(NMonitoring::ToJson(*root).Contains("TX_TEST"));
}

} // namespace NKikimr::NColumnShard
