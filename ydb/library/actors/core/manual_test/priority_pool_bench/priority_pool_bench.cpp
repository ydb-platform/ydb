#include <benchmark/benchmark.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <util/system/event.h>

#include <array>
#include <atomic>

namespace NActors {
namespace {

constexpr ui32 Pairs = 512;
constexpr ui32 DeliveriesPerPair = 512;
constexpr ui64 EventsPerBatch = ui64(Pairs) * DeliveriesPerPair;
static_assert(Pairs % 4 == 0 && DeliveriesPerPair % 2 == 0);

struct TEvPing : TEventLocal<TEvPing, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    ui32 Remaining = DeliveriesPerPair;
};

struct TBatch {
    TManualEvent Done;
    std::atomic<ui32> PendingPairs{0};
};

// One event envelope circulates inside each pair, as in SendActivateReceive.
// Both actors have the same priority and execute equal, finite work. Waiting for
// ALL pairs prevents the High cohort from hiding unfinished Normal work.
class TPingActor : public TActor<TPingActor> {
    const TActorId& Peer;
    TBatch& Batch;

    STFUNC(Receive) {
        auto* ping = ev->Get<TEvPing>();
        if (--ping->Remaining) {
            ev->Rewrite(TEvPing::EventType, Peer);
            Send(ev);
        } else if (Batch.PendingPairs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            Batch.Done.Signal();
        }
    }

public:
    TPingActor(const TActorId& peer, TBatch& batch)
        : TActor(&TThis::Receive)
        , Peer(peer)
        , Batch(batch)
    {}
};

class TWorkload {
    // These objects outlive actor-system teardown, including on a timeout.
    TBatch Batch;
    std::array<std::array<TActorId, 2>, Pairs> Actors;
    std::unique_ptr<TActorSystem> System;

public:
    TWorkload(ui32 threads, ui32 quantum, bool waker, bool priority, ui32 highPercent) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        TBasicExecutorPoolConfig config;
        config.PoolName = "Batch";
        config.Threads = threads;
        config.MinThreadCount = threads;
        config.MaxThreadCount = threads;
        config.DefaultThreadCount = threads;
        config.SpinThreshold = 0;
        config.EventsPerMailbox = quantum;
        config.EnableWaker = waker;
        // Explicit executors: fixed dedicated workers, no harmonizer or shared
        // pool. Only the activation queue implementation differs.
        config.UsePriority = priority;
        setup->Executors[0] = CreateBasicExecutorPool(config);
        setup->Scheduler.Reset(new TBasicSchedulerThread);
        System = std::make_unique<TActorSystem>(setup);
        System->Start();
        for (ui32 pair = 0; pair < Pairs; ++pair) {
            // Interleave classes in registration and submission order.
            const bool high = pair % 4 < highPercent / 25;
            for (ui32 member = 0; member < 2; ++member) {
                THolder<IActor> actor(new TPingActor(Actors[pair][1 - member], Batch));
                if (high) {
                    actor->SetMailboxPriority(EMailboxPriority::High);
                }
                Actors[pair][member] = System->Register(actor.Release(), TMailboxType::HTSwap, 0);
            }
        }
    }

    ~TWorkload() {
        System->Stop();
    }

    bool RunBatch() {
        Batch.Done.Reset();
        Batch.PendingPairs.store(Pairs, std::memory_order_release);
        for (const auto& pair : Actors) {
            Y_ABORT_UNLESS(System->Send(new IEventHandle(pair[0], pair[1], new TEvPing)));
        }
        return Batch.Done.WaitT(TDuration::Seconds(30));
    }
};

void BM_PingPong(benchmark::State& state) {
    TWorkload workload(state.range(0), state.range(1), state.range(2), state.range(3), state.range(4));
    // Registration, startup and one complete warmup batch are outside timing.
    if (!workload.RunBatch()) {
        state.SkipWithError("warmup did not complete every actor pair");
        return;
    }
    for (auto _ : state) {
        if (!workload.RunBatch()) {
            state.SkipWithError("measured batch did not complete every actor pair");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * EventsPerBatch);
    state.counters["events_per_batch"] = EventsPerBatch;
    state.counters["mailboxes"] = 2 * Pairs;
}

void Arguments(benchmark::Benchmark* bench) {
    for (int threads : {1, 4, 24}) {
        for (int quantum : {1, int(TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX)}) {
            for (int waker : {0, 1}) {
                bench->Args({threads, quantum, waker, 0, 0});
                for (int highPercent : {0, 25, 50, 75, 100}) {
                    bench->Args({threads, quantum, waker, 1, highPercent});
                }
            }
        }
    }
}

BENCHMARK(BM_PingPong)
    ->Apply(Arguments)
    ->ArgNames({"workers", "quantum", "waker", "priority", "high_pct"})
    ->MeasureProcessCPUTime()
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
} // namespace NActors
