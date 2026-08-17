#include <library/cpp/yt/containers/static_ring_queue.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <deque>
#include <random>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t Capacity = 8;

using TQueue = TStaticRingQueue<int, Capacity>;

std::vector<int> MakeRange(int begin, int end)
{
    std::vector<int> result;
    for (int value = begin; value < end; ++value) {
        result.push_back(value);
    }
    return result;
}

void Append(TQueue* queue, const std::vector<int>& values)
{
    queue->Append(values.data(), values.data() + values.size());
}

std::vector<int> CopyTail(const TQueue& queue, size_t size)
{
    std::vector<int> result(size);
    queue.CopyTailTo(size, result.data());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticRingQueueTest, Empty)
{
    TQueue queue;
    EXPECT_EQ(0u, queue.Size());
    EXPECT_TRUE(CopyTail(queue, 0).empty());
}

TEST(TStaticRingQueueTest, AppendBelowCapacity)
{
    TQueue queue;
    Append(&queue, MakeRange(0, 3));

    EXPECT_EQ(3u, queue.Size());
    EXPECT_EQ(MakeRange(0, 3), CopyTail(queue, 3));
    EXPECT_EQ(MakeRange(1, 3), CopyTail(queue, 2));
    EXPECT_EQ(MakeRange(2, 3), CopyTail(queue, 1));
}

TEST(TStaticRingQueueTest, AppendExactlyCapacity)
{
    TQueue queue;
    Append(&queue, MakeRange(0, Capacity));

    EXPECT_EQ(Capacity, queue.Size());
    EXPECT_EQ(MakeRange(0, Capacity), CopyTail(queue, Capacity));
}

TEST(TStaticRingQueueTest, SingleAppendAboveCapacityKeepsTail)
{
    TQueue queue;
    Append(&queue, MakeRange(0, 100));

    EXPECT_EQ(Capacity, queue.Size());
    EXPECT_EQ(MakeRange(100 - Capacity, 100), CopyTail(queue, Capacity));
}

TEST(TStaticRingQueueTest, AppendsWrapAround)
{
    TQueue queue;
    for (int index = 0; index < 10; ++index) {
        Append(&queue, MakeRange(3 * index, 3 * index + 3));
    }

    EXPECT_EQ(Capacity, queue.Size());
    EXPECT_EQ(MakeRange(30 - Capacity, 30), CopyTail(queue, Capacity));
}

TEST(TStaticRingQueueTest, EmptyAppendIsNoop)
{
    TQueue queue;
    Append(&queue, MakeRange(0, 5));
    Append(&queue, {});

    EXPECT_EQ(5u, queue.Size());
    EXPECT_EQ(MakeRange(0, 5), CopyTail(queue, 5));
}

TEST(TStaticRingQueueTest, RandomOperations)
{
    TQueue queue;
    std::deque<int> reference;

    std::mt19937 generator(42);
    int next = 0;

    for (int iteration = 0; iteration < 1000; ++iteration) {
        int appendSize = generator() % (2 * Capacity + 1);
        auto values = MakeRange(next, next + appendSize);
        next += appendSize;

        Append(&queue, values);
        for (auto value : values) {
            reference.push_back(value);
        }
        while (reference.size() > Capacity) {
            reference.pop_front();
        }

        ASSERT_EQ(reference.size(), queue.Size());

        size_t copySize = generator() % (reference.size() + 1);
        std::vector<int> expected(reference.end() - copySize, reference.end());
        ASSERT_EQ(expected, CopyTail(queue, copySize));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
