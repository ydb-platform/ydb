#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/fair_scheduler.h>

#include <library/cpp/yt/string/format.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TFairSchedulerTest
    : public ::testing::Test
{
protected:
    const IFairSchedulerPtr<TString> Scheduler_ = CreateFairScheduler<TString>();
};

TEST_F(TFairSchedulerTest, Simple)
{
    Scheduler_->Enqueue("T1", "John");
    Scheduler_->Enqueue("T2", "John");

    EXPECT_EQ(Scheduler_->TryDequeue(), "T1");
    EXPECT_EQ(Scheduler_->TryDequeue(), "T2");
    EXPECT_EQ(Scheduler_->TryDequeue(), std::nullopt);
}

TEST_F(TFairSchedulerTest, Fairness1)
{
    Scheduler_->ChargeUser("Bob", TDuration::Seconds(1));
    Scheduler_->Enqueue("A1", "Alice");
    Scheduler_->Enqueue("A2", "Alice");
    Scheduler_->Enqueue("B1", "Bob");
    Scheduler_->Enqueue("B2", "Bob");

    EXPECT_EQ(Scheduler_->TryDequeue(), "A1");
    Scheduler_->ChargeUser("Alice", TDuration::Seconds(2));
    EXPECT_EQ(Scheduler_->TryDequeue(), "B1");
    Scheduler_->ChargeUser("Bob", TDuration::Seconds(2));
    EXPECT_EQ(Scheduler_->TryDequeue(), "A2");
    Scheduler_->ChargeUser("Alice", TDuration::Seconds(2));
    EXPECT_EQ(Scheduler_->TryDequeue(), "B2");
    Scheduler_->ChargeUser("Bob", TDuration::Seconds(2));
    EXPECT_EQ(Scheduler_->TryDequeue(), std::nullopt);
}

TEST_F(TFairSchedulerTest, Fairness2)
{
    Scheduler_->ChargeUser("Bob", TDuration::Seconds(1));
    for (int index = 1; index <= 10; ++index) {
        Scheduler_->Enqueue(Format("A%v", index), "Alice");
        Scheduler_->Enqueue(Format("B%v", index), "Bob");
    }

    EXPECT_EQ(Scheduler_->TryDequeue(), "A1");
    Scheduler_->ChargeUser("Alice", TDuration::Seconds(100500));

    for (int index = 1; index <= 10; ++index) {
        EXPECT_EQ(Scheduler_->TryDequeue(), Format("B%v", index));
        Scheduler_->ChargeUser("Bob", TDuration::Seconds(1));
    }

    for (int index = 2; index <= 10; ++index) {
        EXPECT_EQ(Scheduler_->TryDequeue(), Format("A%v", index));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
