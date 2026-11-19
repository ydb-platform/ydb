#include "test_states.h"

#include <ydb/library/signals/agent.h>
#include <ydb/library/signals/histogram.h>
#include <ydb/library/signals/owner.h>
#include <ydb/library/signals/states.h>

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

TEST(TValueAggregationAgentTest, DropsReleasedClientsBeforeExport) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TCommonCountersOwner owner("test", root);
    TValueAggregationAgent agent("Value", owner);
    std::weak_ptr<TValueAggregationClient> released = agent.GetClient();
    agent.ResendStatus();
    EXPECT_TRUE(released.expired());
}

TEST(TIncrementalHistogramTest, RegistersOnFirstAdd) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TIncrementalHistogram histogram("test", "Sizes", "", std::set<i64>{1, 10}, root);
    auto group = root->FindSubgroup("module_id", "test")->FindSubgroup("metric", "Sizes");
    ASSERT_TRUE(group);
    histogram.BuildGuard();
    EXPECT_FALSE(group->FindCounter("Value/(-Inf,1]"));

    {
        auto guard = histogram.BuildGuard();
        guard->Add(5, 2);
        EXPECT_EQ(group->FindCounter("Value/(-Inf,1]")->Val(), 0);
        EXPECT_EQ(group->FindCounter("Value/(1,10]")->Val(), 2);
        EXPECT_EQ(group->FindCounter("Value/(10,+Inf)")->Val(), 0);
    }
    EXPECT_EQ(group->FindCounter("Value/(1,10]")->Val(), 0);
}

TEST(TLazyValueTest, RegistersOnFirstSet) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TCommonCountersOwner owner("test", root);
    auto value = owner.GetLazyValue("Max");
    auto group = root->FindSubgroup("module_id", "test");
    EXPECT_FALSE(group->FindCounter("Value/Max"));
    value->Set(3);
    EXPECT_EQ(group->FindCounter("Value/Max")->Val(), 3);
}

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap::NCounters {

TEST(TStateSignalsTest, RegistersEnteredStatesOnly) {
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    NColumnShard::TCommonCountersOwner owner("test", root);
    TStateSignalsOperator<NTest::ETestState> states(owner, "stages");
    {
        auto guard = states.BuildGuard(NTest::ETestState::First);
        guard.SetState(NTest::ETestState::Second);
    }
    auto group = root->FindSubgroup("module_id", "test")->FindSubgroup("states", "stages");
    ASSERT_TRUE(group);
    EXPECT_TRUE(group->FindSubgroup("state_id", "First"));
    EXPECT_EQ(group->FindSubgroup("state_id", "Second")->FindCounter("Value/Count")->Val(), 0);
    EXPECT_FALSE(group->FindSubgroup("state_id", "Third"));
    EXPECT_FALSE(group->FindSubgroup("state_id", "COUNT"));
}

}   // namespace NKikimr::NOlap::NCounters
