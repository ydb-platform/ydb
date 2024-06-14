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
    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(3), TDuration::Max());
    batcher->Enqueue(1);
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    batcher->Enqueue(2);
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    batcher->Enqueue(3);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2, 3}));
    batcher->Enqueue(10);
    batcher->Enqueue(11);
    ASSERT_FALSE(e2.IsSet());
    batcher->Enqueue(12);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({10, 11, 12}));
    batcher->Enqueue(0);
    batcher->Enqueue(1);
    batcher->Enqueue(2);
    auto e3 = batcher->DequeueBatch();
    ASSERT_TRUE(e3.IsSet());
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>({0, 1, 2}));
}

TEST(TNonblockingBatcherTest, Duration)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
    auto e1 = batcher->DequeueBatch();
    Sleep(overTimeout);
    ASSERT_FALSE(e1.IsSet());
    batcher->Enqueue(1);
    auto e2 = batcher->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>{1});
    batcher->Enqueue(2);
    ASSERT_FALSE(e2.IsSet());
    batcher->Enqueue(3);
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({2, 3}));
}

TEST(TNonblockingBatcherTest, Dequeue)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
    EnqueueAll(batcher, {1, 2, 3, 4, 5});
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    }
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 4}));
    }
    {
        auto e = batcher->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        Sleep(overTimeout);
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({5}));
    }
    EnqueueAll(batcher, {6, 7, 8});
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({6, 7}));
    }
    {
        auto e = batcher->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        EnqueueAll(batcher, {9, 10, 11});
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({8, 9}));
    }
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({10, 11}));
    }
}

TEST(TNonblockingBatcherTest, Drop)
{
    auto timeout = Quantum;

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout);
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(batcher, {1, 2, 3});
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    batcher->Drop();
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>());
    batcher->Enqueue(10);
    auto e3 = batcher->DequeueBatch();
    ASSERT_FALSE(e3.IsSet());
    batcher->Drop();
    ASSERT_TRUE(e3.IsSet());
    ASSERT_EQ(e3.Get().ValueOrThrow(), std::vector<int>());
}

TEST(TNonblockingBatcherTest, EnqueueTimeout)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(3), timeout);
    Sleep(overTimeout);
    {
        auto e = batcher->DequeueBatch();
        batcher->Enqueue(1);
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

    auto batcher = New<TNonblockingBatcher<int, TSumLimiter>>(TSumLimiter(10), timeout);
    EnqueueAll(batcher, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1});
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
    auto e3 = batcher->DequeueBatch();
    auto e4 = batcher->DequeueBatch();
    auto e5 = batcher->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_TRUE(e2.IsSet());
    ASSERT_TRUE(e3.IsSet());
    ASSERT_TRUE(e4.IsSet());
    ASSERT_FALSE(e5.IsSet());
    batcher->Enqueue(9);
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
    auto batcher = New<TNonblockingBatcher<int, TLimiter>>(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{10}), timeout);
    EnqueueAll(batcher, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1});
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
    auto e3 = batcher->DequeueBatch();
    auto e4 = batcher->DequeueBatch();
    auto e5 = batcher->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_TRUE(e2.IsSet());
    ASSERT_TRUE(e3.IsSet());
    ASSERT_TRUE(e4.IsSet());
    ASSERT_FALSE(e5.IsSet());
    batcher->Enqueue(9);
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
    auto batcher = New<TNonblockingBatcher<int, TLimiter>>(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{6}), timeout);
    EnqueueAll(batcher, {1, 2, 3, 3, 2});
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({1, 2, 3}));
    }
    batcher->UpdateBatchLimiter(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{4}));
    {
        // continue to use previous limiter
        auto e = batcher->DequeueBatch();
        ASSERT_FALSE(e.IsSet());
        Sleep(overTimeout);
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 2}));
    }
    EnqueueAll(batcher, {3, 2});
    {
        // new batch with new limiter
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({3, 2}));
    }
    batcher->UpdateBatchLimiter(TLimiter(TBatchSizeLimiter{3}, TSumLimiter{100}));
    EnqueueAll(batcher, {5, 6, 7});
    {
        auto e = batcher->DequeueBatch();
        ASSERT_TRUE(e.IsSet());
        ASSERT_EQ(e.Get().ValueOrThrow(), std::vector<int>({5, 6, 7}));
    }
}

TEST(TNonblockingBatcherTest, NoEmptyBatches)
{
    auto timeout = Quantum;
    auto overTimeout = timeout * 2;

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, false);
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(batcher, {1});
    ASSERT_FALSE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_TRUE(e1.IsSet());
    ASSERT_FALSE(e2.IsSet());
    Sleep(overTimeout);
    ASSERT_FALSE(e2.IsSet());
    EnqueueAll(batcher, {2});
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

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, true);
    auto e1 = batcher->DequeueBatch();
    auto e2 = batcher->DequeueBatch();
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

    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), timeout, false);
    batcher->Enqueue(1);
    batcher->Enqueue(2);
    batcher->Enqueue(3);
    auto e1 = batcher->DequeueBatch();
    ASSERT_TRUE(e1.IsSet());
    ASSERT_EQ(e1.Get().ValueOrThrow(), std::vector<int>({1, 2}));
    Sleep(overTimeout);
    batcher->Enqueue(4);
    auto e2 = batcher->DequeueBatch();
    ASSERT_TRUE(e2.IsSet());
    ASSERT_EQ(e2.Get().ValueOrThrow(), std::vector<int>({3, 4}));
}

TEST(TNonblockingBatcherTest, Drain)
{
    auto batcher = New<TNonblockingBatcher<int>>(TBatchSizeLimiter(2), TDuration::Max());
    for (int i = 0; i < 5; ++i) {
        batcher->Enqueue(i);
    }

    auto batches = batcher->Drain();

    ASSERT_EQ(std::ssize(batches), 3);
    ASSERT_EQ(batches[0], std::vector({0, 1}));
    ASSERT_EQ(batches[1], std::vector({2, 3}));
    ASSERT_EQ(batches[2], std::vector({4}));

    batcher->Enqueue(0);
    auto batch = batcher->DequeueBatch();
    auto drainedBatches = batcher->Drain();

    ASSERT_FALSE(batch.Get().IsOK());
    ASSERT_EQ(std::ssize(drainedBatches), 1);
    ASSERT_EQ(drainedBatches[0], std::vector({0}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
