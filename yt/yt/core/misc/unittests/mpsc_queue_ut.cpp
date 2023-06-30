#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/mpsc_queue.h>

#include <thread>
#include <array>

namespace NYT {
namespace {

using ::testing::TProbeState;
using ::testing::TProbe;

////////////////////////////////////////////////////////////////////////////////

TEST(TMpscQueueTest, SingleThreaded1)
{
    TMpscQueue<int> queue;

    queue.Enqueue(1);
    queue.Enqueue(2);
    queue.Enqueue(3);

    int value;

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(1, value);

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(2, value);

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(3, value);

    EXPECT_FALSE(queue.TryDequeue(&value));
};

TEST(TMpscQueueTest, SingleThreaded2)
{
    TMpscQueue<int> queue;

    queue.Enqueue(1);
    queue.Enqueue(2);

    int value;

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(1, value);

    queue.Enqueue(3);

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(2, value);

    EXPECT_TRUE(queue.TryDequeue(&value));
    EXPECT_EQ(3, value);

    EXPECT_FALSE(queue.TryDequeue(&value));
};

TEST(TMpscQueueTest, MultiThreaded)
{
    TMpscQueue<int> queue;

    constexpr int N = 10000;
    constexpr int T = 4;

    auto barrier = NewPromise<void>();

    auto producer = [&] {
        barrier.ToFuture().Get();
        for (int i = 0; i < N; ++i) {
            queue.Enqueue(i);
        }
    };

    auto consumer = [&] {
        std::array<int, N> counts{};
        barrier.ToFuture().Get();
        for (int i = 0; i < N * T; ++i) {
            int item;
            while (!queue.TryDequeue(&item));
            counts[item]++;
        }
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(counts[i], T);
        }
    };

    std::vector<std::thread> threads;

    for (int i = 0; i < T; ++i) {
        threads.emplace_back(producer);
    }
    threads.emplace_back(consumer);

    barrier.Set();

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST(TMpscQueueTest, Drain)
{
    TProbeState probeState;
    TMpscQueue<TProbe> queue;

    queue.Enqueue(TProbe(&probeState));
    queue.Enqueue(TProbe(&probeState));
    queue.Enqueue(TProbe(&probeState));
    {
        auto probe = TProbe::ExplicitlyCreateInvalidProbe();
        EXPECT_TRUE(queue.TryDequeue(&probe));
    }
    EXPECT_EQ(3, probeState.Constructors);
    EXPECT_EQ(1, probeState.Destructors);
    EXPECT_EQ(5, probeState.ShadowDestructors);
    EXPECT_EQ(3, probeState.MoveConstructors);

    queue.DrainConsumer();
    EXPECT_EQ(3, probeState.Constructors);
    EXPECT_EQ(3, probeState.Destructors);
    EXPECT_EQ(7, probeState.ShadowDestructors);
    EXPECT_EQ(3, probeState.MoveConstructors);

    queue.Enqueue(TProbe(&probeState));
    queue.DrainProducer();
    EXPECT_EQ(4, probeState.Constructors);
    EXPECT_EQ(4, probeState.Destructors);
    EXPECT_EQ(9, probeState.ShadowDestructors);
    EXPECT_EQ(4, probeState.MoveConstructors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
