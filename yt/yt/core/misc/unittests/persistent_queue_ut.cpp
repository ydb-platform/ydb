#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/persistent_queue.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TQueueType = TPersistentQueue<int, 10>;
using TSnapshot = TPersistentQueueSnapshot<int, 10>;

TEST(TPersistentQueue, Empty)
{
    TQueueType queue;
    EXPECT_EQ(0u, queue.Size());
    EXPECT_TRUE(queue.Empty());
    EXPECT_EQ(queue.Begin(), queue.End());

    auto snapshot = queue.MakeSnapshot();
    EXPECT_EQ(0u, snapshot.Size());
    EXPECT_TRUE(snapshot.Empty());
    EXPECT_EQ(snapshot.Begin(), snapshot.End());
}

TEST(TPersistentQueue, EnqueueDequeue)
{
    TQueueType queue;

    const int N = 100;

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(i, static_cast<ssize_t>(queue.Size()));
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(N - i, static_cast<ssize_t>(queue.Size()));
        EXPECT_EQ(i, queue.Dequeue());
    }
}

TEST(TPersistentQueue, Iterate)
{
    TQueueType queue;

    const int N = 100;

    for (int i = 0; i < 2 * N; ++i) {
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(i, queue.Dequeue());
    }

    int expected = N;
    for (int x : queue) {
        EXPECT_EQ(expected, x);
        ++expected;
    }
}

TEST(TPersistentQueue, Snapshot1)
{
    TQueueType queue;

    const int N = 100;
    std::vector<TSnapshot> snapshots;

    for (int i = 0; i < N; ++i) {
        snapshots.push_back(queue.MakeSnapshot());
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        const auto& snapshot = snapshots[i];
        EXPECT_EQ(i, static_cast<ssize_t>(snapshot.Size()));
        int expected = 0;
        for (int x : snapshot) {
            EXPECT_EQ(expected, x);
            ++expected;
        }
    }
}

TEST(TPersistentQueue, Snapshot2)
{
    TQueueType queue;

    const int N = 100;
    std::vector<TSnapshot> snapshots;

    for (int i = 0; i < N; ++i) {
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        snapshots.push_back(queue.MakeSnapshot());
        EXPECT_EQ(i, queue.Dequeue());
    }

    for (int i = 0; i < N; ++i) {
        const auto& snapshot = snapshots[i];
        EXPECT_EQ(i, static_cast<ssize_t>(N - snapshot.Size()));
        int expected = i;
        for (int x : snapshot) {
            EXPECT_EQ(expected, x);
            ++expected;
        }
    }
}

TEST(TPersistentQueue, Clear)
{
    TQueueType queue;

    queue.Enqueue(1);

    EXPECT_EQ(1u, queue.Size());

    queue.Clear();

    EXPECT_EQ(0u, queue.Size());

    auto snapshot = queue.MakeSnapshot();
    EXPECT_EQ(snapshot.Begin(), snapshot.End());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
