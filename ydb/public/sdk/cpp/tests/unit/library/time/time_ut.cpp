#include <ydb/public/sdk/cpp/src/library/time/time.h>

#include <gtest/gtest.h>

#include <thread>

using namespace std::chrono_literals;
using namespace NYdb;

TEST(DurationTest, Simple) {
    ASSERT_EQ(TDeadline::SafeDurationCast(1s), 1s);
    ASSERT_EQ(TDeadline::SafeDurationCast(1ms), 1ms);
    ASSERT_EQ(TDeadline::SafeDurationCast(1us), 1us);
    ASSERT_EQ(TDeadline::SafeDurationCast(1ns), 1ns);

    ASSERT_EQ(TDeadline::SafeDurationCast(TDuration::Seconds(1)), 1s);
    ASSERT_EQ(TDeadline::SafeDurationCast(TDuration::MilliSeconds(1)), 1ms);
    ASSERT_EQ(TDeadline::SafeDurationCast(TDuration::MicroSeconds(1)), 1us);
}

TEST(DurationTest, CornerValues) {
    ASSERT_EQ(TDeadline::SafeDurationCast(0s), TDeadline::Duration::zero());
    ASSERT_EQ(TDeadline::SafeDurationCast(0ms), TDeadline::Duration::zero());
    ASSERT_EQ(TDeadline::SafeDurationCast(0us), TDeadline::Duration::zero());
    ASSERT_EQ(TDeadline::SafeDurationCast(0ns), TDeadline::Duration::zero());

    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::seconds::max()), TDeadline::Duration::max());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::milliseconds::max()), TDeadline::Duration::max());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::microseconds::max()), TDeadline::Duration::max());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::nanoseconds::max()), TDeadline::Duration::max());

    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::seconds::min()), TDeadline::Duration::min());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::milliseconds::min()), TDeadline::Duration::min());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::microseconds::min()), TDeadline::Duration::min());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::nanoseconds::min()), TDeadline::Duration::min());

    ASSERT_EQ(TDeadline::SafeDurationCast(TDuration::Zero()), TDeadline::Duration::zero());
    ASSERT_EQ(TDeadline::SafeDurationCast(TDuration::Max()), TDeadline::Duration::max());

    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::seconds::max()), TDeadline::Duration::max());
    ASSERT_EQ(TDeadline::SafeDurationCast(std::chrono::milliseconds::max()), TDeadline::Duration::max());
}

TEST(DeadlineTest, Compare) {
    ASSERT_EQ(TDeadline::AfterDuration(TDeadline::Duration::max()), TDeadline::Max());
    ASSERT_EQ(TDeadline::AfterDuration(std::chrono::seconds::max()), TDeadline::Max());
    ASSERT_EQ(TDeadline::AfterDuration(std::chrono::milliseconds::max()), TDeadline::Max());
    ASSERT_EQ(TDeadline::AfterDuration(std::chrono::microseconds::max()), TDeadline::Max());
    ASSERT_EQ(TDeadline::AfterDuration(std::chrono::nanoseconds::max()), TDeadline::Max());

    ASSERT_EQ(TDeadline::AfterDuration(TDuration::Max()), TDeadline::Max());

    ASSERT_LT(TDeadline::Now(), TDeadline::Max());
    ASSERT_LT(TDeadline::Now(), TDeadline::AfterDuration(100s));
}
