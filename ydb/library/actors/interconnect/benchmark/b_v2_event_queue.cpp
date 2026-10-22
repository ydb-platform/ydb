// Micro-benchmarks for TIncomingEventQueue, the lock-free MPSC queue every outgoing event crosses on its
// way from an actor thread to the v2 engine's shard worker. Its scaling with the number of producers is
// what decides whether the engine's single-consumer design holds up on a busy node.

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/interconnect/interconnect_uring_event_queue.h>

#include <atomic>
#include <thread>

using namespace NActors;

namespace {

    const TActorId Sender(1, 2, 3, 4);
    const TActorId Recipient(2, 3, 4, 5);

    // A handle wrapping a shared, already-serialized body: allocating one costs a single small allocation
    // plus a refcount bump, with no protobuf work, so the measurement stays focused on the queue itself.
    TIntrusivePtr<TEventSerializedData> MakeSharedBody() {
        return MakeIntrusive<TEventSerializedData>(TRope(TString(64, 'x')), TEventSerializationInfo{});
    }

    std::unique_ptr<IEventHandle> MakeHandle(const TIntrusivePtr<TEventSerializedData>& body) {
        return std::make_unique<IEventHandle>(/*type=*/1, /*flags=*/0, Recipient, Sender, body, /*cookie=*/0);
    }

    TIncomingEventQueue::TRecord MakeRecord(std::unique_ptr<IEventHandle> ev, ui64 seqNo) {
        return TIncomingEventQueue::TRecord{
            .Ev = std::move(ev),
            .Conn = 1,
            .Callback = nullptr,
            .ReceivedTimestamp = 0,
            .SeqNo = seqNo,
        };
    }

    // Uncontended cost of a full push/pop cycle. The same handle is recycled through the queue, so no
    // allocator traffic is folded into the number.
    void EventQueuePushPop(benchmark::State& state) {
        const auto body = MakeSharedBody();
        TIncomingEventQueue queue;
        auto handle = MakeHandle(body);

        ui64 seqNo = 0;
        for (auto _ : state) {
            queue.Push(MakeRecord(std::move(handle), seqNo++));
            auto record = queue.Pop();
            handle = std::move(record->Ev);
        }

        state.SetItemsProcessed(state.iterations());
    }

    // Push cost with several producers contending for the queue head. A dedicated drainer thread (outside
    // the measured region) keeps the queue from growing without bound; comparing across thread counts shows
    // how the head cache line behaves under contention. Each iteration allocates its handle, which is what
    // happens in production too -- the constant per-handle allocation cost does not hide the scaling.
    class TMultiProducerFixture: public benchmark::Fixture {
    public:
        void SetUp(benchmark::State& state) override {
            if (state.thread_index() != 0) {
                return;
            }
            Body = MakeSharedBody();
            Stopping.store(false);
            Drainer = std::thread([this] {
                while (!Stopping.load(std::memory_order_relaxed)) {
                    while (Queue.Pop()) {
                    }
                }
                while (Queue.Pop()) {
                }
            });
        }

        void TearDown(benchmark::State& state) override {
            if (state.thread_index() != 0) {
                return;
            }
            Stopping.store(true, std::memory_order_relaxed);
            Drainer.join();
            Body.Reset();
        }

    protected:
        TIncomingEventQueue Queue;
        TIntrusivePtr<TEventSerializedData> Body;

    private:
        std::thread Drainer;
        std::atomic<bool> Stopping{false};
    };

    BENCHMARK_DEFINE_F(TMultiProducerFixture, Push)(benchmark::State& state) {
        ui64 seqNo = 0;
        for (auto _ : state) {
            Queue.Push(MakeRecord(MakeHandle(Body), seqNo++));
        }
        state.SetItemsProcessed(state.iterations());
    }

    BENCHMARK(EventQueuePushPop)->Unit(benchmark::kNanosecond);

    BENCHMARK_REGISTER_F(TMultiProducerFixture, Push)
        ->ThreadRange(1, 8)
        ->Unit(benchmark::kNanosecond);

} // namespace
