#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/relaxed_mpsc_queue.h>

#include <thread>
#include <array>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIntNode
{
    int Value;
    TRelaxedMpscQueueHook Hook;

    explicit TIntNode(int value)
        : Value(value)
    { }
};

TEST(TRelaxedMpscQueueTest, SimpleSingleThreaded)
{
    TRelaxedIntrusiveMpscQueue<TIntNode, &TIntNode::Hook> queue;

    queue.Enqueue(std::make_unique<TIntNode>(1));
    queue.Enqueue(std::make_unique<TIntNode>(2));
    queue.Enqueue(std::make_unique<TIntNode>(3));

    auto n1 = queue.TryDequeue();
    EXPECT_EQ(1, n1->Value);
    auto n2 = queue.TryDequeue();
    EXPECT_EQ(2, n2->Value);
    auto n3 = queue.TryDequeue();
    EXPECT_EQ(3, n3->Value);

    EXPECT_FALSE(static_cast<bool>(queue.TryDequeue()));
};

TEST(TRelaxedMpscQueueTest, SimpleMultiThreaded)
{
    TRelaxedIntrusiveMpscQueue<TIntNode, &TIntNode::Hook> queue;

    constexpr int N = 10000;
    constexpr int T = 4;

    auto barrier = NewPromise<void>();

    auto producer = [&] {
        barrier.ToFuture().Get();
        for (int i = 0; i < N; ++i) {
            queue.Enqueue(std::make_unique<TIntNode>(i));
        }
    };

    auto consumer = [&] {
        std::array<int, N> counts{};
        barrier.ToFuture().Get();
        for (int i = 0; i < N * T; ++i) {
            while (true) {
                if (auto item = queue.TryDequeue()) {
                    counts[item->Value]++;
                    break;
                }
            }
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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
