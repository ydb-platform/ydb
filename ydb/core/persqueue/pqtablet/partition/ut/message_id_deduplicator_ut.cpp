#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/pqtablet/partition/message_id_deduplicator.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NPQ;

struct MockTimeProvider : public ITimeProvider {

    MockTimeProvider() {
        Value = TInstant::Seconds(1761034384);
    }

    TInstant Now() override {
        return Value;
    }

    void Tick(TDuration duration) {
        Value += duration;
    }

    TInstant Value;
};

struct TestScenario {
    TPartitionId PartitionId = TPartitionId(1);

    TestScenario()
        : TimeProvider(MakeIntrusive<MockTimeProvider>())
        , Deduplicator(PartitionId, TimeProvider, TDuration::Seconds(10))
    {
    }

    std::optional<ui64> AddMessage(const TString& messageId, ui64 offset) {
        return Deduplicator.AddMessage(messageId, offset);
    }

    void CreateWAL() {
        Deduplicator.Compact();

        NKikimrPQ::TMessageDeduplicationIdWAL wal;
        if (auto key = Deduplicator.SerializeTo(wal); key) {
            Cerr << "Append WAL: " << key.value() << Endl;
            if (!WALs.empty() && WALs.back().first == key.value()) {
                WALs.back().second = std::move(wal);
            } else {
                WALs.push_back({std::move(key.value()), std::move(wal)});
            }

            Deduplicator.Commit();
        }
    }

    void AssertWALLoad() {
        TMessageIdDeduplicator target(PartitionId, TimeProvider, TDuration::Seconds(10));
        for (auto& wal : WALs) {
            target.ApplyWAL(std::move(wal.first), std::move(wal.second));
        }
        WALs.clear();

        EXPECT_EQ(target.GetQueue(), Deduplicator.GetQueue());
    }

    TIntrusivePtr<MockTimeProvider> TimeProvider;
    TMessageIdDeduplicator Deduplicator;
    std::deque<std::pair<TString, NKikimrPQ::TMessageDeduplicationIdWAL>> WALs;
};


TEST(TDeduplicatorTest, AddMessage) {
    TestScenario scenario;

    EXPECT_FALSE(scenario.AddMessage("message1", 1).has_value());
    scenario.CreateWAL();
    EXPECT_EQ(scenario.WALs.size(), 1ul);

    scenario.AssertWALLoad();
}

TEST(TDeduplicatorTest, AddTwoMessages) {
    TestScenario scenario;

    EXPECT_FALSE(scenario.AddMessage("message1", 1).has_value());
    scenario.CreateWAL();
    EXPECT_FALSE(scenario.AddMessage("message2", 2).has_value());
    scenario.CreateWAL();

    EXPECT_EQ(scenario.WALs.size(), 1ul);

    scenario.AssertWALLoad();
}

TEST(TDeduplicatorTest, AddDeduplicatedMessages) {
    TestScenario scenario;

    EXPECT_FALSE(scenario.AddMessage("message1", 3).has_value());
    scenario.CreateWAL();
    auto r = scenario.AddMessage("message1", 7);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r.value(), 3ul);
    scenario.CreateWAL();

    EXPECT_EQ(scenario.WALs.size(), 1ul);

    scenario.AssertWALLoad();
}

TEST(TDeduplicatorTest, AddTwoMessages_DifferentTime_OneBucket) {
    TestScenario scenario;

    EXPECT_FALSE(scenario.AddMessage("message1", 1).has_value());
    scenario.TimeProvider->Tick(TDuration::MilliSeconds(110));
    EXPECT_FALSE(scenario.AddMessage("message2", 2).has_value());
    scenario.CreateWAL();

    EXPECT_EQ(scenario.WALs.size(), 1ul);

    scenario.AssertWALLoad();
}

TEST(TDeduplicatorTest, AddManyMessages_SameTime_DifferentBucket) {
    TestScenario scenario;

    for (size_t i = 0; i < 999; ++i) {
        EXPECT_FALSE(scenario.AddMessage(TStringBuilder() << "message" << i, i).has_value());
    }
    scenario.CreateWAL();

    for (size_t i = 999; i < 2000; ++i) {
        EXPECT_FALSE(scenario.AddMessage(TStringBuilder() << "message" << i, i).has_value());
    }
    scenario.CreateWAL();

    EXPECT_EQ(scenario.WALs.size(), 2ul);

    scenario.AssertWALLoad();
}

