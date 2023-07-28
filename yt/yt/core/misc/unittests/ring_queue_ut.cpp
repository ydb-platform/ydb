#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRingQueueTest, PodRandomOperations)
{
    TRingQueue<int> queue;
    EXPECT_EQ(0, std::ssize(queue));
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.begin(), queue.end());

    const int N = 100000;
    // We perform same sequence of operations on this deque.
    std::deque<int> deque;

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(queue.size(), deque.size());
        EXPECT_EQ(queue.empty(), deque.empty());
        if (!queue.empty()) {
            EXPECT_EQ(queue.back(), deque.back());
            EXPECT_EQ(queue.front(), deque.front());
        }

        int r = rand() % 100;
        if (r == 0) {
            queue.clear();
            deque.clear();
            EXPECT_EQ(0, std::ssize(queue));
            EXPECT_TRUE(queue.empty());
            EXPECT_EQ(queue.begin(), queue.end());
        } else if (r < 10) {
            auto queueIterator = queue.begin();
            auto dequeIterator = deque.begin();
            while (dequeIterator != deque.end()) {
                EXPECT_NE(queueIterator, queue.end());
                EXPECT_EQ(*queueIterator, *dequeIterator);
                queue.move_forward(queueIterator);
                ++dequeIterator;
            }
            EXPECT_EQ(queueIterator, queue.end());
        } else if (r < 55 && !queue.empty()) {
            queue.pop();
            deque.pop_front();
        } else {
            int value = rand();
            queue.push(value);
            deque.push_back(value);
        }
    }
}

class TFoo
    : public virtual TRefCounted
{
public:
    explicit TFoo(int& counter)
        : Counter_(counter)
    {
        ++Counter_;
    }

    ~TFoo()
    {
        --Counter_;
    }
private:
    int& Counter_;
};

DEFINE_REFCOUNTED_TYPE(TFoo)
DECLARE_REFCOUNTED_TYPE(TFoo)

TEST(TRingQueueTest, TestLifetimeWithRefCount)
{
    const int N = 100000;
    int counter = 0;

    {
        TRingQueue<TFooPtr> queue;

        for (int i = 0; i < N; ++i) {
            queue.push(New<TFoo>(counter));
            EXPECT_EQ(counter, std::ssize(queue));
        }
        queue.clear();
        EXPECT_EQ(counter, 0);
    }

    {
        TRingQueue<TFooPtr> queue;

        for (int i = 0; i < N; ++i) {
            queue.push(New<TFoo>(counter));
            EXPECT_EQ(counter, std::ssize(queue));
            queue.pop();
            EXPECT_EQ(counter, std::ssize(queue));
        }
        queue.clear();
        EXPECT_EQ(counter, 0);
    }

    {
        TRingQueue<TFooPtr> queue;

        for (int i = 0; i < N; ++i) {
            if (!queue.empty() && rand() % 2 == 0) {
                queue.pop();
            } else {
                queue.push(New<TFoo>(counter));
            }
            EXPECT_EQ(counter, std::ssize(queue));
        }
        queue.clear();
        EXPECT_EQ(counter, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
