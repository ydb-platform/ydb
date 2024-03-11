#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT {
namespace {

template <bool FailOnZeroDeadline = true>
void TestDeadlineShiftedByCorrectDuration(const TRelativeConstantBackoffStrategy& backoff, TDuration expectedShift)
{
    auto start = TInstant::Now();
    auto deadline = backoff.GetBackoffDeadline();

    if constexpr (FailOnZeroDeadline) {
        EXPECT_TRUE(deadline != TInstant::Zero());
    } else {
        if (deadline == TInstant::Zero()) {
            return;
        }
    }

    auto finish = TInstant::Now();

    EXPECT_TRUE(deadline - start < finish - start + expectedShift);
}

constexpr TExponentialBackoffOptions TestingExponentialOptions{
    .MinBackoff = TDuration::MicroSeconds(1),
    .MaxBackoff = TDuration::MicroSeconds(3),
    .BackoffMultiplier = 2,
    .BackoffJitter = 0.0,
};

constexpr TConstantBackoffOptions TestingConstantOptions{
    .Backoff = TDuration::MicroSeconds(5),
    .BackoffJitter = 0.0,
};

TEST(TBackoffStrategyTest, JustWorks)
{
    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetInvocationIndex(), 1);
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));
}

TEST(TBackoffStrategyTest, InitBackoff)
{
    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));
    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));
}

TEST(TBackoffStrategyTest, MaxBackoff)
{
    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(3));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(3));
}

TEST(TBackoffStrategyTest, MaxInvocations)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.InvocationCount = 1;

    TBackoffStrategy backoff(customOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_FALSE(backoff.Next());
}

TEST(TBackoffStrategyTest, WorkPastInvocationsLimit)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.InvocationCount = 1;

    TBackoffStrategy backoff(customOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_FALSE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    EXPECT_FALSE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(3));
}

TEST(TBackoffStrategyTest, ConstantOptions)
{
    TBackoffStrategy backoff(TestingConstantOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(5));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(5));
}

TEST(TBackoffStrategyTest, UpdateOptionsKeepState)
{
    auto newOptions = TestingExponentialOptions;
    newOptions.BackoffMultiplier = 1;

    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetInvocationIndex(), 1);

    backoff.UpdateOptions(newOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetInvocationIndex(), 2);
}

TEST(TBackoffStrategyTest, UpdateMultiplier)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.BackoffMultiplier = 1;

    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    backoff.UpdateOptions(customOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));
}

TEST(TBackoffStrategyTest, UpdateBackoffLimitsJustWorks)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.MaxBackoff = TDuration::MicroSeconds(10);

    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(3));

    backoff.UpdateOptions(customOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(6));
}

TEST(TBackoffStrategyTest, UpdateBackoffLimitsPersist)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.MaxBackoff = TDuration::MicroSeconds(1);

    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    backoff.UpdateOptions(customOptions);

    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));
}

TEST(TBackoffStrategyTest, UpdateBackoffLimitsIncorrectIntervalBias)
{
    auto customOptions = TestingExponentialOptions;
    customOptions.MinBackoff = TDuration::MicroSeconds(15);
    customOptions.MaxBackoff = TDuration::MicroSeconds(1);

    TBackoffStrategy backoff(TestingExponentialOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(2));

    backoff.UpdateOptions(customOptions);

    EXPECT_TRUE(backoff.Next());
    EXPECT_EQ(backoff.GetBackoff(), TDuration::MicroSeconds(1));
}

TEST(TBackoffStrategyTest, UpdateOptionsErasePreviousCustomisation)
{
    auto initOptions = TestingExponentialOptions;
    initOptions.InvocationCount = 0;

    auto newOptions = TestingExponentialOptions;
    newOptions.BackoffMultiplier = 1;

    TBackoffStrategy backoff(initOptions);

    EXPECT_FALSE(backoff.Next());

    backoff.UpdateOptions(newOptions);

    EXPECT_TRUE(backoff.Next());
}

TEST(TRelativeConstantBackoffStrategyTest, JustWorks)
{
    TRelativeConstantBackoffStrategy backoff(TestingConstantOptions);

    TInstant now = TInstant::Now();
    backoff.RecordInvocation();

    EXPECT_TRUE(!backoff.IsOverBackoff(now));

    TestDeadlineShiftedByCorrectDuration(backoff, TDuration::Seconds(1));
}

TEST(TRelativeConstantBackoffStrategyTest, NegativeInfinity)
{
    TRelativeConstantBackoffStrategy backoff(TestingConstantOptions);

    //! This is done in order to prevent spurious test failure
    //! if computer gets frozen for more than 3 seconds
    TInstant now = TInstant::Now();
    EXPECT_TRUE(backoff.IsOverBackoff(now));

    backoff.RecordInvocation();
    EXPECT_FALSE(backoff.IsOverBackoff(now));
}

TEST(TRelativeConstantBackoffStrategyTest, NegativeInityDeadline)
{
    TRelativeConstantBackoffStrategy backoff(TestingConstantOptions);

    auto deadline = backoff.GetBackoffDeadline();

    EXPECT_EQ(deadline, TInstant::Zero());
}

TEST(TRelativeConstantBackoffStrategyTest, RecordIfPastDeadline)
{
    TRelativeConstantBackoffStrategy backoff(TestingConstantOptions);

    auto now = TInstant::Now();
    EXPECT_TRUE(backoff.RecordInvocationIfOverBackoff());

    EXPECT_FALSE(backoff.IsOverBackoff(now));
}

TEST(TRelativeConstantBackoffStrategyTest, DeadlinePasses)
{
    TRelativeConstantBackoffStrategy backoff(TestingConstantOptions);

    EXPECT_TRUE(backoff.RecordInvocationIfOverBackoff());
    auto afterInvocation = TInstant::Now();

    EXPECT_TRUE(backoff.IsOverBackoff(afterInvocation + TDuration::Seconds(3) + TDuration::MicroSeconds(1)));
}

} // namespace
} // namespace NYT
