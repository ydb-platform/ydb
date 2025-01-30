#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCoroutineTest
    : public ::testing::Test
{ };

void Coroutine0(TCoroutine<int()>& self)
{
    self.Yield(1);
    self.Yield(2);
    self.Yield(3);
    self.Yield(4);
    self.Yield(5);
}

TEST_F(TCoroutineTest, Nullary)
{
    TCoroutine<int()> coro(&Coroutine0);
    EXPECT_FALSE(coro.IsCompleted());

    int i;
    std::optional<int> actual;
    for (i = 1; /**/; ++i) {
        actual = coro.Run();
        if (coro.IsCompleted()) {
            break;
        }
        EXPECT_TRUE(actual);
        EXPECT_EQ(i, *actual);
    }

    EXPECT_FALSE(actual);
    EXPECT_EQ(6, i);

    EXPECT_TRUE(coro.IsCompleted());
}

void Coroutine1(TCoroutine<int(int)>& self, int arg)
{
    EXPECT_EQ(0, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(2, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(4, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(6, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(8, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(10, arg);
}

TEST_F(TCoroutineTest, Unary)
{
    TCoroutine<int(int)> coro(&Coroutine1);
    EXPECT_FALSE(coro.IsCompleted());

    // Alternative syntax.
    int i = 0, j = 0;
    std::optional<int> actual;
    while ((actual = coro.Run(j))) {
        ++i;
        EXPECT_EQ(i * 2 - 1, *actual);
        EXPECT_EQ(i * 2 - 2, j);
        j = *actual+ 1;
    }

    EXPECT_FALSE(actual);
    EXPECT_EQ(5, i);
    EXPECT_EQ(10, j);

    EXPECT_TRUE(coro.IsCompleted());
}

// In this case I've got lazy and set up these test cases.
struct TTestCase {
    int lhs;
    int rhs;
    int sum;
};

std::vector<TTestCase> Coroutine2TestCases = {
    { 10, 20, 30 },
    { 11, 21, 32 },
    { 12, 22, 34 },
    { 13, 23, 36 },
    { 14, 24, 38 },
    { 15, 25, 40 }
};

void Coroutine2(TCoroutine<int(int, int)>& self, int lhs, int rhs)
{
    for (int i = 0; i < std::ssize(Coroutine2TestCases); ++i) {
        EXPECT_EQ(Coroutine2TestCases[i].lhs, lhs) << "Iteration #" << i;
        EXPECT_EQ(Coroutine2TestCases[i].rhs, rhs) << "Iteration #" << i;
        std::tie(lhs, rhs) = self.Yield(lhs + rhs);
    }
}

TEST_F(TCoroutineTest, Binary)
{
    TCoroutine<int(int, int)> coro(&Coroutine2);
    EXPECT_FALSE(coro.IsCompleted());

    int i = 0;
    std::optional<int> actual;
    for (
        i = 0;
        (actual = coro.Run(
            i < std::ssize(Coroutine2TestCases) ? Coroutine2TestCases[i].lhs : 0,
            i < std::ssize(Coroutine2TestCases) ? Coroutine2TestCases[i].rhs : 0));
        ++i)
    {
        EXPECT_EQ(Coroutine2TestCases[i].sum, *actual);
    }

    EXPECT_FALSE(actual);
    EXPECT_EQ(i, std::ssize(Coroutine2TestCases));

    EXPECT_TRUE(coro.IsCompleted());
}

void Coroutine3(TCoroutine<void()>& self)
{
    for (int i = 0; i < 10; ++i) {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
        self.Yield();
    }
}

TEST_W(TCoroutineTest, WaitFor)
{
    TCoroutine<void()> coro(&Coroutine3);
    for (int i = 0; i < 11; ++i) {
        EXPECT_FALSE(coro.IsCompleted());
        coro.Run();
    }
    EXPECT_TRUE(coro.IsCompleted());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

