#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>
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

TEST(TMpscStackTest, DequeueFiltered)
{
    {
        TMpscStack<int> stack;
        for (int i = 0; i < 5; ++i) {
            stack.Enqueue(i);
        }

        stack.FilterElements([] (int value) {
            return value % 2 == 0;
        });
        EXPECT_FALSE(stack.IsEmpty());
        auto values = stack.DequeueAll(/*reverse*/ false);

        EXPECT_EQ(values, std::vector<int>({4, 2, 0}));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class Tag>
struct TWidget
    : public TIntrusiveListItem<TWidget<Tag>, Tag>
{
    int Value;
};

struct Tag1
{ };

struct Tag2
{ };

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusiveMpscStackTest, Simple)
{
    TWidget<Tag1> one{.Value = 1};
    TWidget<Tag1> two{.Value = 2};

    TIntrusiveMpscStack<TWidget<Tag1>, Tag1> stack;

    stack.Push(&one);
    stack.Push(&two);

    auto list = stack.PopAll();

    EXPECT_EQ(list.Size(), 2u);

    auto twoPtr = list.PopBack();
    auto onePtr = list.PopBack();

    EXPECT_EQ(onePtr, &one);
    EXPECT_EQ(twoPtr, &two);

    stack.Push(twoPtr);

    list = stack.PopAll();
    EXPECT_EQ(list.Size(), 1u);

    onePtr = list.PopBack();
    EXPECT_EQ(onePtr, twoPtr);
}

TEST(TIntrusiveMpscStackTest, ConcurrentTryDequeue)
{
    constexpr i64 Size = 100'000;

    TIntrusiveMpscStack<TWidget<Tag1>, Tag1> stack;

    auto run = [&] {
        Sleep(TDuration::MilliSeconds(50));
        for (int i = 0; i < Size; ++i) {
            stack.Push(new TWidget<Tag1>(TWidget<Tag1>{
                .Value = i,
            }));
        }
    };

    std::thread t1(run);
    std::thread t2(run);

    i64 sum = 0;
    i64 expectedSum = Size * (Size - 1);

    while (sum < expectedSum) {
        auto list = stack.PopAll();

        while (!list.Empty()) {
            auto item = list.PopBack();
            sum += item->Value;
            delete item;
        }
    }

    EXPECT_EQ(sum, expectedSum);

    t1.join();
    t2.join();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
