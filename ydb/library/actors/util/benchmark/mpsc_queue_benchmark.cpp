#include <library/cpp/testing/gbenchmark/benchmark.h>

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

class TLockFreeQueueAdapter
{
public:
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

void BM_TLockFreeQueue(benchmark::State& state)
{
    RunMpsc<TLockFreeQueueAdapter>(state);
}

void MpscArguments(benchmark::Benchmark* benchmark)
{
    constexpr int ItemsPerProducer = 4096;
    for (int producers : {1, 4, 16}) {
        benchmark->Args({producers, ItemsPerProducer});
    }
}

void PooledFunnelQueueArguments(benchmark::Benchmark* benchmark)
{
    // The freelist in TPooledFunnelQueue is subject to ABA when multiple
    // producers take entries while the consumer returns entries to the pool.
    // Keep this benchmark single-producer until the freelist is fixed.
    constexpr int ItemsPerProducer = 4096;
    benchmark->Args({1, ItemsPerProducer});
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

BENCHMARK(BM_TLockFreeQueue)
    ->Apply(MpscArguments)
    ->ArgNames({"producers", "items_per_producer"})
    ->UseRealTime()
    ->Unit(benchmark::kNanosecond);

} // namespace
