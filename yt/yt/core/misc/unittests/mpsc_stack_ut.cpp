#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <thread>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMpscStackTest, Simple)
{
    TMpscStack<int> stack;
    stack.Enqueue(1);
    stack.Enqueue(2);

    int x;
    EXPECT_FALSE(stack.IsEmpty());
    EXPECT_TRUE(stack.TryDequeue(&x));
    EXPECT_EQ(2, x);
    EXPECT_FALSE(stack.IsEmpty());
    EXPECT_TRUE(stack.TryDequeue(&x));
    EXPECT_EQ(1, x);
    EXPECT_TRUE(stack.IsEmpty());
    EXPECT_FALSE(stack.TryDequeue(&x));
}

TEST(TMpscStackTest, DequeueAll)
{
    {
        TMpscStack<int> stack;
        for (int i = 0; i < 5; ++i) {
            stack.Enqueue(i);
        }

        auto values = stack.DequeueAll(/*reverse*/ false);
        EXPECT_TRUE(stack.IsEmpty());
        EXPECT_EQ(values, std::vector<int>({4, 3, 2, 1, 0}));
    }
    {
        TMpscStack<int> stack;
        for (int i = 0; i < 5; ++i) {
            stack.Enqueue(i);
        }

        auto values = stack.DequeueAll(/*reverse*/ true);
        EXPECT_TRUE(stack.IsEmpty());
        EXPECT_EQ(values, std::vector<int>({0, 1, 2, 3, 4}));
    }
}

TEST(TMpscStackTest, ConcurrentTryDequeue)
{
    constexpr i64 Size = 100'000;

    TMpscStack<int> stack;

    auto run = [&] {
        Sleep(TDuration::MilliSeconds(50));
        for (int i = 0; i < Size; ++i) {
            stack.Enqueue(i);
        }
    };

    std::thread t1(run);
    std::thread t2(run);

    i64 sum = 0;
    for (int i = 0; i < 2 * Size; ++i) {
        int x = -1;
        while (!stack.TryDequeue(&x));
        sum += x;
    }

    EXPECT_EQ(sum, Size * (Size - 1));

    t1.join();
    t2.join();
    EXPECT_TRUE(stack.IsEmpty());
}

TEST(TMpscStackTest, ConcurrentTryDequeueAll)
{
    constexpr i64 Size = 100'000;

    TMpscStack<int> stack;

    auto run = [&] {
        Sleep(TDuration::MilliSeconds(50));
        for (int i = 0; i < Size; ++i) {
            stack.Enqueue(i);
        }
    };

    std::thread t1(run);
    std::thread t2(run);

    int count = 0;
    i64 sum = 0;
    while (count < 2 * Size) {
        auto values = stack.DequeueAll(/*reverse*/ rand() % 2 == 0);
        count += values.size();
        for (int x : values) {
            sum += x;
        }
    }

    EXPECT_EQ(sum, Size * (Size - 1));

    t1.join();
    t2.join();
    EXPECT_TRUE(stack.IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
