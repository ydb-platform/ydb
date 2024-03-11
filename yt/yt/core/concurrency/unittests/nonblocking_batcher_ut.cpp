#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/nonblocking_batcher.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto Quantum = TDuration::Seconds(1);

template <class TQueuePtr, class T>
void EnqueueAll(
    const TQueuePtr& batch,
    std::initializer_list<T> list)
{
    for (auto&& element : list) {
        batch->Enqueue(std::move(element));
    }
}

TEST(TNonblockingBatcherTest, Simple)
{
    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(3), TDuration::Max());
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

TEST(TNonblockingBatcherTest, Duration)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
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

TEST(TNonblockingBatcherTest, Dequeue)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
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

TEST(TNonblockingBatcherTest, Drop)
{
    auto timeout = Quantum;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
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

TEST(TNonblockingBatcherTest, EnqueueTimeout)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(3), timeout);
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

struct TSumLimiter
{
    explicit TSumLimiter(int limit)
        : SumLimit(limit)
    { }

    bool IsFull() const
    {
        return Sum >= SumLimit;
    }

    void Add(int element)
    {
        Sum += element;
    }


    int Sum = 0;
    int SumLimit;
};

TEST(TNonblockingBatcherTest, SumLimiter)
{
    auto timeout = Quantum;

    auto b = New<TNonblockingBatcher<int, TSumLimiter>>(TSumLimiter(10), timeout);
    EnqueueAll(b, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1});
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    auto e3 = b->DequeueBatch();
    auto e4 = b->DequeueBatch();
    auto e5 = b->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_TRUE(e2.IsSet());
    ASSERT_TRUE(e3.IsSet());
    ASSERT_TRUE(e4.IsSet());
    ASSERT_FALSE(e5.IsSet());
    b->Enqueue(9);
    ASSERT_TRUE(e5.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2, 3, 4}));
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({5, 6}));
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>({7, 8}));
    ASSERT_EQ(e4.Get().ValueOrThrow(), std::vector<int>({9, 10}));
    ASSERT_EQ(e5.Get().ValueOrThrow(), std::vector<int>({1, 9}));
}

TEST(TNonblockingBatcherTest, CompositeLimiterLimiter)
{
    auto timeout = Quantum;
    using TLimiter = TCompositeBatchLimiter<TBatchSizeLimiter, TSumLimiter>;
    auto b = New<TNonblockingBatcher<int, TLimiter>>(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{10}), timeout);
    EnqueueAll(b, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1});
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    auto e3 = b->DequeueBatch();
    auto e4 = b->DequeueBatch();
    auto e5 = b->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_TRUE(e2.IsSet());
    ASSERT_TRUE(e3.IsSet());
    ASSERT_TRUE(e4.IsSet());
    ASSERT_FALSE(e5.IsSet());
    b->Enqueue(9);
    ASSERT_TRUE(e5.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2, 3}));
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({4, 5, 6}));
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>({7, 8}));
    ASSERT_EQ(e4.Get().ValueOrThrow(), std::vector<int>({9, 10}));
    ASSERT_EQ(e5.Get().ValueOrThrow(), std::vector<int>({1, 9}));
}

TEST(TNonblockingBatcherTest, UpdateLimiter)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    using TLimiter = TCompositeBatchLimiter<TBatchSizeLimiter, TSumLimiter>;
    auto b = New<TNonblockingBatcher<int, TLimiter>>(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{6}), timeout);
    EnqueueAll(b, {1, 2, 3, 3, 2});
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({1, 2, 3}));
    }
    b->UpdateBatchLimiter(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{4}));
    {
        // continue to use previous limiter
        auto e = b->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        Sleep(overTimeout);
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 2}));
    }
    EnqueueAll(b, {3, 2});
    {
        // new batch with new limiter
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 2}));
    }
    b->UpdateBatchLimiter(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{100}));
    EnqueueAll(b, {5, 6, 7});
    {
        auto e = b->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({5, 6, 7}));
    }
}

TEST(TNonblockingBatcherTest, NoEmptyBatches)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, false);
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(b, {1});
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(b, {2});
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1}));
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({2}));
}

TEST(TNonblockingBatcherTest, AllowEmptyBatches)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, true);
    auto e1 = b->DequeueBatch();
    auto e2 = b->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e1.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({}));
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({}));
}

TEST(TNonblockingBatcherTest, IncompleteBatchAfterDeque)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto b = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, false);
    b->Enqueue(1);
    b->Enqueue(2);
    b->Enqueue(3);
    auto e1 = b->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    Sleep(overTimeout);
    b->Enqueue(4);
    auto e2 = b->DequeueBatch();
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({3, 4}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
