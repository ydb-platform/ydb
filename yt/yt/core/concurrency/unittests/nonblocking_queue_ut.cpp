#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/nonblocking_queue.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TNonblockingQueueTest, DequeueFirst)
{
    TNonblockingQueue<int> queue;
    auto result1 = queue.Dequeue();
    auto result2 = queue.Dequeue();

    EXPECT_FALSE(result1.IsSet());
    EXPECT_FALSE(result2.IsSet());

    queue.Enqueue(1);

    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    queue.Enqueue(2);

    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

TEST(TNonblockingQueueTest, EnqueueFirst)
{
    TNonblockingQueue<int> queue;
    queue.Enqueue(1);
    queue.Enqueue(2);

    auto result1 = queue.Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    auto result2 = queue.Dequeue();
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

TEST(TNonblockingQueueTest, Mixed)
{
    TNonblockingQueue<int> queue;
    queue.Enqueue(1);

    auto result1 = queue.Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    auto result2 = queue.Dequeue();
    EXPECT_FALSE(result2.IsSet());

    queue.Enqueue(2);
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

TEST(TNonblockingQueueTest, DequeueFirstAsync)
{
    TNonblockingQueue<int> queue;
    auto result = queue.Dequeue();
    EXPECT_FALSE(result.IsSet());

    auto promise = NewPromise<int>();
    queue.Enqueue(promise.ToFuture());
    EXPECT_FALSE(result.IsSet());

    promise.Set(1);
    EXPECT_TRUE(result.IsSet());
    EXPECT_EQ(result.Get().Value(), 1);
}

TEST(TNonblockingQueueTest, EnqueueFirstAsync)
{
    TNonblockingQueue<int> queue;
    auto promise1 = NewPromise<int>();
    auto promise2 = NewPromise<int>();
    queue.Enqueue(promise1.ToFuture());
    queue.Enqueue(promise2.ToFuture());

    auto result1 = queue.Dequeue();
    EXPECT_FALSE(result1.IsSet());

    promise1.Set(1);
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(result1.Get().Value(), 1);

    promise2.Set(2);
    auto result2 = queue.Dequeue();
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(result2.Get().Value(), 2);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBoundedNonblockingQueueTest, DequeueFirst)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto resultDequeue1 = queue.Dequeue();
    auto resultDequeue2 = queue.Dequeue();

    EXPECT_FALSE(resultDequeue1.IsSet());
    EXPECT_FALSE(resultDequeue2.IsSet());

    auto resultEnqueue1 = queue.Enqueue(1);
    EXPECT_TRUE(resultEnqueue1.IsSet());

    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(1, resultDequeue1.Get().Value());

    auto resultEnqueue2 = queue.Enqueue(2);
    EXPECT_TRUE(resultEnqueue2.IsSet());

    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(2, resultDequeue2.Get().Value());
}

TEST(TBoundedNonblockingQueueTest, EnqueueFirst)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto resultEnqueue1 = queue.Enqueue(1);
    EXPECT_TRUE(resultEnqueue1.IsSet());

    auto resultEnqueue2 = queue.Enqueue(2);
    EXPECT_FALSE(resultEnqueue2.IsSet());

    auto resultDequeue1 = queue.Dequeue();
    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(1, resultDequeue1.Get().Value());

    EXPECT_TRUE(resultEnqueue2.IsSet());

    auto resultDequeue2 = queue.Dequeue();
    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(2, resultDequeue2.Get().Value());
}

TEST(TBoundedNonblockingQueueTest, MixedEnqueueFirst)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto resultEnqueue1 = queue.Enqueue(1);
    EXPECT_TRUE(resultEnqueue1.IsSet());

    auto resultDequeue1 = queue.Dequeue();
    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(1, resultDequeue1.Get().Value());

    auto resultDequeue2 = queue.Dequeue();
    EXPECT_FALSE(resultDequeue2.IsSet());

    auto resultEnqueue2 = queue.Enqueue(2);
    EXPECT_TRUE(resultEnqueue2.IsSet());

    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(2, resultDequeue2.Get().Value());
}

TEST(TBoundedNonblockingQueueTest, MixedDequeueFirst)
{
    TBoundedNonblockingQueue<int> queue(1);

    auto resultDequeue1 = queue.Dequeue();
    EXPECT_FALSE(resultDequeue1.IsSet());

    auto resultEnqueue1 = queue.Enqueue(1);
    EXPECT_TRUE(resultEnqueue1.IsSet());
    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(1, resultDequeue1.Get().Value());

    auto resultEnqueue2 = queue.Enqueue(2);
    EXPECT_TRUE(resultEnqueue2.IsSet());

    auto resultDequeue2 = queue.Dequeue();
    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(2, resultDequeue2.Get().Value());
}

TEST(TBoundedNonblockingQueueTest, DequeueFirstAsync)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto resultDequeue = queue.Dequeue();
    EXPECT_FALSE(resultDequeue.IsSet());

    auto promise = NewPromise<int>();
    auto resultEnqueue = queue.Enqueue(promise.ToFuture());
    EXPECT_FALSE(resultDequeue.IsSet());
    EXPECT_TRUE(resultEnqueue.IsSet());

    promise.Set(1);
    EXPECT_TRUE(resultDequeue.IsSet());
    EXPECT_EQ(resultDequeue.Get().Value(), 1);
}

TEST(TBoundedNonblockingQueueTest, EnqueueFirstAsync)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto promise1 = NewPromise<int>();
    auto promise2 = NewPromise<int>();
    auto resultEnqueue1 = queue.Enqueue(promise1.ToFuture());
    EXPECT_TRUE(resultEnqueue1.IsSet());
    auto resultEnqueue2 = queue.Enqueue(promise2.ToFuture());
    EXPECT_FALSE(resultEnqueue2.IsSet());

    auto resultDequeue1 = queue.Dequeue();
    EXPECT_FALSE(resultDequeue1.IsSet());
    EXPECT_TRUE(resultEnqueue2.IsSet());

    promise1.Set(1);
    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(resultDequeue1.Get().Value(), 1);

    promise2.Set(2);
    auto resultDequeue2 = queue.Dequeue();
    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(resultDequeue2.Get().Value(), 2);
}

TEST(TBoundedNonblockingQueueTest, EnqueueFirstAsync2)
{
    TBoundedNonblockingQueue<int> queue(1);
    auto promise1 = NewPromise<int>();
    auto promise2 = NewPromise<int>();
    auto resultEnqueue1 = queue.Enqueue(promise1.ToFuture());
    EXPECT_TRUE(resultEnqueue1.IsSet());
    auto resultEnqueue2 = queue.Enqueue(promise2.ToFuture());
    EXPECT_FALSE(resultEnqueue2.IsSet());

    auto resultDequeue1 = queue.Dequeue();
    EXPECT_FALSE(resultDequeue1.IsSet());

    EXPECT_TRUE(resultEnqueue2.IsSet());

    auto resultDequeue2 = queue.Dequeue();
    EXPECT_FALSE(resultDequeue2.IsSet());

    promise1.Set(1);
    EXPECT_TRUE(resultDequeue1.IsSet());
    EXPECT_EQ(resultDequeue1.Get().Value(), 1);

    promise2.Set(2);
    EXPECT_TRUE(resultDequeue2.IsSet());
    EXPECT_EQ(resultDequeue2.Get().Value(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

