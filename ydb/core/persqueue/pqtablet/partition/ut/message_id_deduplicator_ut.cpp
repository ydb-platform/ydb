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
    TestScenario()
        : TimeProvider(MakeIntrusive<MockTimeProvider>())
        , Deduplicator(TimeProvider, TDuration::Seconds(10))
    {
    }

    bool AddMessage(const TString& messageId) {
        return Deduplicator.AddMessage(messageId);
    }

    void CreateWAL() {
        Deduplicator.Compact();

        WALs.emplace_back();
        Deduplicator.SerializeTo(WALs.back());
    }

    void AssertWALLoad() {
        TMessageIdDeduplicator target(TimeProvider, TDuration::Seconds(10));
        for (auto& wal : WALs) {
            target.ApplyWAL(std::move(wal));
        }
        WALs.clear();

        EXPECT_EQ(target.GetQueue(), Deduplicator.GetQueue());
    }

    TIntrusivePtr<MockTimeProvider> TimeProvider;
    TMessageIdDeduplicator Deduplicator;
    std::deque<NKikimrPQ::MessageDeduplicationIdWAL> WALs;
};


TEST(TDeduplicatorTest, AddMessage) {
    TestScenario scenario;

    EXPECT_FALSE(scenario.AddMessage("message1"));
    scenario.CreateWAL();
    scenario.AssertWALLoad();
}