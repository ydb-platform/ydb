#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/nonblocking_batch.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto Quantum = TDuration::Seconds(1);

template <class T>
void EnqueueAll(
    const TNonblockingBatchPtr<int>& batch,
    std::initializer_list<T> list)
{
    for (auto&& element : list) {
        batch->Enqueue(std::move(element));
    }
}

TEST(TNonblockingBatchTest, Simple)
{
    auto b = New<TNonblockingBatch<int>>(3, TDuration::Max());
    b->Enqueue(1);
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    b->Enqueue(2);
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    b->Enqueue(3);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2, 3}));
    b->Enqueue(10);
    b->Enqueue(11);
    ASSERT_FALSE(e2.IsSet());
    b->Enqueue(12);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({10, 11, 12}));
    b->Enqueue(0);
    b->Enqueue(1);
    b->Enqueue(2);
    auto e3 = b->DequeueBatch();
    ASSERT_TRUE(e3.IsSet());
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>({0, 1, 2}));
}

TEST(TNonblockingBatchTest, Duration)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatch<int>>(2, timeout);
    auto e1 = b->DequeueBatch();
    Sleep(overTimeout);
    ASSERT_FALSE(e1.IsSet());
    b->Enqueue(1);
    auto e2 = b->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>{1});
    b->Enqueue(2);
    ASSERT_FALSE(e2.IsSet());
    b->Enqueue(3);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({2, 3}));
}

TEST(TNonblockingBatchTest, Dequeue)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatch<int>>(2, timeout);
    EnqueueAll(b, {1, 2, 3, 4, 5});
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    }
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 4}));
    }
    {
        auto e = b->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        Sleep(overTimeout);
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({5}));
    }
    EnqueueAll(b, {6, 7, 8});
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({6, 7}));
    }
    {
        auto e = b->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        EnqueueAll(b, {9, 10, 11});
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({8, 9}));
    }
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({10, 11}));
    }
}

TEST(TNonblockingBatchTest, Drop)
{
    auto timeout = Quantum;

    auto b = New<TNonblockingBatch<int>>(2, timeout);
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(b, {1, 2, 3});
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    b->Drop();
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>());
    b->Enqueue(10);
    auto e3 = b->DequeueBatch();
    ASSERT_FALSE(e3.IsSet());
    b->Drop();
    ASSERT_TRUE(e3.IsSet());
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>());
}

TEST(TNonblockingBatchTest, EnqueueTimeout)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatch<int>>(3, timeout);
    Sleep(overTimeout);
    {
        auto e = b->DequeueBatch();
        b->Enqueue(1);
        ASSERT_FALSE(e.IsSet());
        Sleep(overTimeout);
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({1}));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
