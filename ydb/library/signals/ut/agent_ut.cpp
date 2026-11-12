#include <ydb/library/signals/agent.h>
#include <ydb/library/signals/owner.h>

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

TEST(TValueAggregationAgentTest, RegistersAfterClientWrite) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TCommonCountersOwner owner("test", root);
    TValueAggregationAgent agent("Value", owner);
    auto client = agent.GetClient();
    agent.ResendStatus();
    auto group = root->FindSubgroup("module_id", "test");
    EXPECT_FALSE(group->FindCounter("Aggregation/SUM/Value"));

    client->SetValue(5);
    agent.ResendStatus();
    EXPECT_EQ(group->FindCounter("Aggregation/SUM/Value")->Val(), 5);
    EXPECT_EQ(group->FindCounter("Aggregation/MAX/Value")->Val(), 5);

    client.reset();
    agent.ResendStatus();
    EXPECT_EQ(group->FindCounter("Aggregation/SUM/Value")->Val(), 0);
}

TEST(TValueAggregationAgentTest, RegistersAfterClientReleasedBeforeStatus) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TCommonCountersOwner owner("test", root);
    TValueAggregationAgent agent("Value", owner);
    agent.GetClient()->SetValue(5);
    agent.ResendStatus();
    EXPECT_EQ(root->FindSubgroup("module_id", "test")->FindCounter("Aggregation/SUM/Value")->Val(), 0);
}

} // namespace NKikimr::NColumnShard
