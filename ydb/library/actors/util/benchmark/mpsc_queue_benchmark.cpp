#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <library/cpp/threading/bounded_queue/bounded_queue.h>
#include <library/cpp/threading/queue/mpsc_intrusive_unordered.h>
#include <ydb/library/actors/util/funnel_queue.h>
#include <ydb/library/actors/util/intrusive_funnel_queue.h>

#include <util/generic/vector.h>
#include <util/thread/lfqueue.h>

#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

namespace {

constexpr int ItemsPerProducer = 1 << 20;

struct TNode
    : NThreading::TIntrusiveNode
    , TIntrusiveFunnelQueueItem<TNode>
{
    ui64 Value = 0;
};

class TFunnelQueueAdapter
{
public:
    void Push(TNode* node) noexcept
    {
        Queue_.Push(std::move(node));
    }

    TNode* Pop() noexcept
    {
        if (Queue_.IsEmpty()) {
            return nullptr;
        }

        auto* node = Queue_.Top();
        Queue_.Pop();
        return node;
    }

private:
    TFunnelQueue<TNode*> Queue_;
};

class TPooledFunnelQueueAdapter
{
public:
    void Push(TNode* node) noexcept
    {
        Queue_.Push(std::move(node));
    }

    TNode* Pop() noexcept
    {
        if (Queue_.IsEmpty()) {
            return nullptr;
        }

        auto* node = Queue_.Top();
        Queue_.Pop();
        return node;
    }

private:
    TPooledFunnelQueue<TNode*> Queue_;
};

class TMpscIntrusiveUnorderedAdapter
{
public:
    void Push(TNode* node) noexcept
    {
        Queue_.Push(node);
    }

    TNode* Pop() noexcept
    {
        return static_cast<TNode*>(Queue_.Pop());
    }

private:
    NThreading::TMPSCIntrusiveUnordered Queue_;
};

class TIntrusiveFunnelQueueAdapter
{
public:
    void Push(TNode* node) noexcept
    {
        Queue_.Push(node);
    }

    TNode* Pop() noexcept
    {
        return Queue_.Pop();
    }

private:
    TIntrusiveFunnelQueue<TNode> Queue_;
};

class TIntrusiveFunnelQueueTryPopAdapter
{
public:
    void Push(TNode* node) noexcept
    {
        Queue_.Push(node);
    }

    TNode* Pop() noexcept
    {
        return Queue_.TryPop().Item;
    }

private:
    TIntrusiveFunnelQueue<TNode> Queue_;
};

class TLockFreeQueueAdapter
{
public:
    TLockFreeQueueAdapter() = default;

    explicit TLockFreeQueueAdapter(size_t)
    { }

    void Push(TNode* node)
    {
        Queue_.Enqueue(node);
    }

    TNode* Pop()
    {
        TNode* node = nullptr;
        Queue_.Dequeue(&node);
        return node;
    }

private:
    TLockFreeQueue<TNode*> Queue_;
};

class TBoundedQueueAdapter
{
public:
    explicit TBoundedQueueAdapter(size_t size)
        : Queue_(size)
    { }

    void Push(TNode* node) noexcept
    {
        while (!Queue_.Enqueue(node)) {
            std::this_thread::yield();
        }
    }

    TNode* Pop() noexcept
    {
        TNode* node = nullptr;
        return Queue_.Dequeue(node) ? node : nullptr;
    }

private:
    NThreading::TBoundedQueue<TNode*> Queue_;
};

// Runs one consumer concurrently with N producers. Every producer owns a fixed
// set of nodes; the end-of-round barrier ensures that intrusive nodes are only
// reused after the consumer has removed all of them from the queue.
template <class TQueue>
void RunMpsc(benchmark::State& state)
{
    const auto producerCount = static_cast<size_t>(state.range(0));
    const auto itemsPerProducer = static_cast<size_t>(state.range(1));
    const auto totalItems = producerCount * itemsPerProducer;

    TQueue queue;
    TVector<TNode> nodes(totalItems);
    for (size_t index = 0; index < nodes.size(); ++index) {
        nodes[index].Value = index;
    }

    std::atomic<bool> stopping = false;
    std::barrier roundStart(static_cast<std::ptrdiff_t>(producerCount + 2));
    std::barrier roundEnd(static_cast<std::ptrdiff_t>(producerCount + 2));

    TVector<std::thread> threads;
    threads.reserve(producerCount + 1);

    for (size_t producer = 0; producer < producerCount; ++producer) {
        threads.emplace_back([&, producer] {
            auto* const first = nodes.data() + producer * itemsPerProducer;
            for (;;) {
                roundStart.arrive_and_wait();
                if (stopping.load(std::memory_order_relaxed)) {
                    return;
                }

                for (size_t index = 0; index < itemsPerProducer; ++index) {
                    queue.Push(first + index);
                }
                roundEnd.arrive_and_wait();
            }
        });
    }

    threads.emplace_back([&] {
        for (;;) {
            roundStart.arrive_and_wait();
            if (stopping.load(std::memory_order_relaxed)) {
                return;
            }

            size_t consumed = 0;
            while (consumed < totalItems) {
                if (auto* node = queue.Pop()) {
                    benchmark::DoNotOptimize(node->Value);
                    ++consumed;
                } else {
                    std::this_thread::yield();
                }
            }
            roundEnd.arrive_and_wait();
        }
    });

    for (auto _ : state) {
        roundStart.arrive_and_wait();
        roundEnd.arrive_and_wait();
    }

    stopping.store(true, std::memory_order_relaxed);
    roundStart.arrive_and_wait();
    for (auto& thread : threads) {
        thread.join();
    }

    state.SetItemsProcessed(state.iterations() * totalItems);
}

void BM_FunnelQueue(benchmark::State& state)
{
    RunMpsc<TFunnelQueueAdapter>(state);
}

void BM_TMPSCIntrusiveUnordered(benchmark::State& state)
{
    RunMpsc<TMpscIntrusiveUnorderedAdapter>(state);
}

void BM_TPooledFunnelQueue(benchmark::State& state)
{
    RunMpsc<TPooledFunnelQueueAdapter>(state);
}

void BM_TIntrusiveFunnelQueue(benchmark::State& state)
{
    RunMpsc<TIntrusiveFunnelQueueAdapter>(state);
}

void BM_TIntrusiveFunnelQueueTryPop(benchmark::State& state)
{
    RunMpsc<TIntrusiveFunnelQueueTryPopAdapter>(state);
}

void BM_TLockFreeQueue(benchmark::State& state)
{
    RunMpsc<TLockFreeQueueAdapter>(state);
}

// Runs one producer concurrently with N consumers. A shared counter lets each
// consumer leave the round once all nodes have been removed from the queue.
template <class TQueue>
void RunSpmc(benchmark::State& state)
{
    const auto consumerCount = static_cast<size_t>(state.range(0));
    const auto itemCount = static_cast<size_t>(state.range(1));

    TQueue queue(itemCount);
    TVector<TNode> nodes(itemCount);
    for (size_t index = 0; index < nodes.size(); ++index) {
        nodes[index].Value = index;
    }

    std::atomic<bool> stopping = false;
    std::atomic<size_t> consumed = 0;
    std::barrier roundStart(static_cast<std::ptrdiff_t>(consumerCount + 2));
    std::barrier roundEnd(static_cast<std::ptrdiff_t>(consumerCount + 2));

    TVector<std::thread> threads;
    threads.reserve(consumerCount + 1);

    threads.emplace_back([&] {
        for (;;) {
            roundStart.arrive_and_wait();
            if (stopping.load(std::memory_order_relaxed)) {
                return;
            }

            for (auto& node : nodes) {
                queue.Push(&node);
            }
            roundEnd.arrive_and_wait();
        }
    });

    for (size_t consumer = 0; consumer < consumerCount; ++consumer) {
        threads.emplace_back([&] {
            for (;;) {
                roundStart.arrive_and_wait();
                if (stopping.load(std::memory_order_relaxed)) {
                    return;
                }

                while (consumed.load(std::memory_order_relaxed) < itemCount) {
                    if (auto* node = queue.Pop()) {
                        benchmark::DoNotOptimize(node->Value);
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        std::this_thread::yield();
                    }
                }
                roundEnd.arrive_and_wait();
            }
        });
    }

    for (auto _ : state) {
        consumed.store(0, std::memory_order_relaxed);
        roundStart.arrive_and_wait();
        roundEnd.arrive_and_wait();
    }

    stopping.store(true, std::memory_order_relaxed);
    roundStart.arrive_and_wait();
    for (auto& thread : threads) {
        thread.join();
    }

    state.SetItemsProcessed(state.iterations() * itemCount);
}

void BM_TLockFreeQueueSpmc(benchmark::State& state)
{
    RunSpmc<TLockFreeQueueAdapter>(state);
}

void BM_TBoundedQueueSpmc(benchmark::State& state)
{
    RunSpmc<TBoundedQueueAdapter>(state);
}

void MpscArguments(benchmark::Benchmark* benchmark)
{
    for (int producers : {1, 4, 16}) {
        benchmark->Args({producers, ItemsPerProducer});
    }
}

void PooledFunnelQueueArguments(benchmark::Benchmark* benchmark)
{
    // The freelist in TPooledFunnelQueue is subject to ABA when multiple
    // producers take entries while the consumer returns entries to the pool.
    // Keep this benchmark single-producer until the freelist is fixed.
    benchmark->Args({1, ItemsPerProducer});
}

void SpmcArguments(benchmark::Benchmark* benchmark)
{
    for (int consumers : {1, 4, 16}) {
        benchmark->Args({consumers, ItemsPerProducer});
    }
}

BENCHMARK(BM_FunnelQueue)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TMPSCIntrusiveUnordered)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TPooledFunnelQueue)
    ->Apply(PooledFunnelQueueArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TIntrusiveFunnelQueue)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TIntrusiveFunnelQueueTryPop)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TLockFreeQueue)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TLockFreeQueueSpmc)
    ->Apply(SpmcArguments)
    ->ArgNames({"consumers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_TBoundedQueueSpmc)
    ->Apply(SpmcArguments)
    ->ArgNames({"consumers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

} // namespace
