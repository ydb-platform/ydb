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

} // namespace
} // namespace NYT::NConcurrency

