#include <benchmark/benchmark.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/async/yield.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/threading/future/future.h>

using namespace NActors;
using namespace NThreading;

class TPingTargetActor : public TActor<TPingTargetActor> {
public:
    TPingTargetActor()
        : TActor(&TThis::StateWork)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPing, Handle);
        }
    }

    void Handle(TEvents::TEvPing::TPtr& ev) {
        Send(ev->Sender, new TEvents::TEvPong, 0, ev->Cookie);
    }
};

class TPingDriverManualActor : public TActorBootstrapped<TPingDriverManualActor> {
public:
    TPingDriverManualActor(const TActorId& target, benchmark::State& state, TPromise<void> promise)
        : Target(target)
        , State(state)
        , Promise(std::move(promise))
    {}

    ~TPingDriverManualActor() {
        Promise.SetValue();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        Step();
    }

    void Step() {
        if (State.KeepRunning()) {
            Send(Target, new TEvents::TEvPing, 0, ++LastCookie);
            return;
        }
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPong, Handle);
        }
    }

    void Handle(TEvents::TEvPong::TPtr&) {
        Step();
    }

private:
    const TActorId Target;
    benchmark::State& State;
    TPromise<void> Promise;
    ui64 LastCookie = 0;
};

class TPingDriverAsyncActor : public TActorBootstrapped<TPingDriverAsyncActor> {
public:
    TPingDriverAsyncActor(const TActorId& target, benchmark::State& state, TPromise<void> promise)
        : Target(target)
        , State(state)
        , Promise(std::move(promise))
    {}

    ~TPingDriverAsyncActor() {
        Promise.SetValue();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        for (size_t i = 0; i < 64; ++i) {
            co_await Step();
        }
        const auto before = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats().HeapAllocations;
        for (auto _ : State) {
            co_await Step();
        }

        const auto stats = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats();
        State.counters["heap_allocations_per_op"] = double(stats.HeapAllocations - before) / State.iterations();
        State.counters["retained_bytes"] = stats.CachedBytes;
        PassAway();
    }

    async<void> Step() {
        ui64 cookie = ++LastCookie;
        Send(Target, new TEvents::TEvPing(), 0, cookie);
        co_await ActorWaitForEvent<TEvents::TEvPong>(cookie);
    }

    STFUNC(StateWork) {
        Y_UNUSED(ev);
    }

private:
    const TActorId Target;
    benchmark::State& State;
    TPromise<void> Promise;
    ui64 LastCookie = 0;
};

template<class TDriver>
void BM_PingActor(benchmark::State& state, size_t budget = TAsyncFrameCache::DefaultSizeBytes) {

    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
    setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>(budget));
    setup->NodeId = 0;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[ setup->ExecutorsCount ]);
    for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
        setup->Executors[i] = new TBasicExecutorPool(i, 1, 1, "basic");
    }
    setup->Scheduler = new TBasicSchedulerThread;

    TActorSystem actorSystem(setup);
    actorSystem.Start();

    auto target = actorSystem.Register(new TPingTargetActor);
    auto promise = NewPromise<void>();
    auto future = promise.GetFuture();
    actorSystem.Register(new TDriver(target, state, std::move(promise)));
    future.GetValueSync();

    actorSystem.Stop();
}

void BM_ManualPingActor(benchmark::State& state) {
    BM_PingActor<TPingDriverManualActor>(state);
}

void BM_AsyncPingActor(benchmark::State& state) {
    BM_PingActor<TPingDriverAsyncActor>(state);
}

BENCHMARK(BM_ManualPingActor)->MeasureProcessCPUTime();
BENCHMARK(BM_AsyncPingActor)->MeasureProcessCPUTime();

void BM_AsyncPingActorDisabled(benchmark::State& state) {
    BM_PingActor<TPingDriverAsyncActor>(state, 0);
}
BENCHMARK(BM_AsyncPingActorDisabled)->MeasureProcessCPUTime();

class TManualYieldActor : public TActorBootstrapped<TManualYieldActor> {
public:
    TManualYieldActor(benchmark::State& state, TPromise<void> promise)
        : State(state)
        , Promise(std::move(promise))
    {}

    ~TManualYieldActor() {
        Promise.SetValue();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        Step();
    }

    void Step() {
        if (State.KeepRunning()) {
            Send(SelfId(), new TEvents::TEvWakeup);
            return;
        }
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        Step();
    }

    private:
    benchmark::State& State;
    TPromise<void> Promise;
};

class TAsyncYieldActor : public TActorBootstrapped<TAsyncYieldActor> {
public:
    TAsyncYieldActor(benchmark::State& state, TPromise<void> promise)
        : State(state)
        , Promise(std::move(promise))
    {}

    ~TAsyncYieldActor() {
        Promise.SetValue();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        for (auto _ : State) {
            co_await AsyncYield();
        }

        PassAway();
    }

    STFUNC(StateWork) {
        Y_UNUSED(ev);
    }

private:
    benchmark::State& State;
    TPromise<void> Promise;
};

template<class TDriver>
void BM_YieldActor(benchmark::State& state, size_t budget = TAsyncFrameCache::DefaultSizeBytes) {

    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
    setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>(budget));
    setup->NodeId = 0;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[ setup->ExecutorsCount ]);
    for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
        setup->Executors[i] = new TBasicExecutorPool(i, 1, 1, "basic");
    }
    setup->Scheduler = new TBasicSchedulerThread;

    TActorSystem actorSystem(setup);
    actorSystem.Start();

    auto promise = NewPromise<void>();
    auto future = promise.GetFuture();
    actorSystem.Register(new TDriver(state, std::move(promise)));
    future.GetValueSync();

    actorSystem.Stop();
}

void BM_ManualYieldActor(benchmark::State& state) {
    BM_YieldActor<TManualYieldActor>(state);
}

void BM_AsyncYieldActor(benchmark::State& state) {
    BM_YieldActor<TAsyncYieldActor>(state);
}

BENCHMARK(BM_ManualYieldActor)->MeasureProcessCPUTime();
BENCHMARK(BM_AsyncYieldActor)->MeasureProcessCPUTime();

class TAsyncLoopActor : public TActorBootstrapped<TAsyncLoopActor> {
public:
    TAsyncLoopActor(benchmark::State& state, TPromise<void> promise)
        : State(state)
        , Promise(std::move(promise))
    {}

    ~TAsyncLoopActor() {
        Promise.SetValue();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        for (size_t i = 0; i < 64; ++i) {
            benchmark::DoNotOptimize(co_await Step());
        }
        const auto before = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats().HeapAllocations;
        for (auto _ : State) {
            benchmark::DoNotOptimize(co_await Step());
        }

        const auto stats = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats();
        // Step is inline: allocation elision is allowed in this control.
        State.counters["heap_allocations_per_op"] = double(stats.HeapAllocations - before) / State.iterations();
        State.counters["retained_bytes"] = stats.CachedBytes;
        PassAway();
    }

    async<int> Step() {
        co_return ++LastCookie;
    }

    STFUNC(StateWork) {
        Y_UNUSED(ev);
    }

private:
    benchmark::State& State;
    TPromise<void> Promise;
    ui64 LastCookie = 0;
};

void BM_CallAsync(benchmark::State& state) {
    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
    setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>());
    setup->NodeId = 0;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[ setup->ExecutorsCount ]);
    for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
        setup->Executors[i] = new TBasicExecutorPool(i, 1, 1, "basic");
    }
    setup->Scheduler = new TBasicSchedulerThread;

    TActorSystem actorSystem(setup);
    actorSystem.Start();

    auto promise = NewPromise<void>();
    auto future = promise.GetFuture();
    actorSystem.Register(new TAsyncLoopActor(state, std::move(promise)));
    future.GetValueSync();

    actorSystem.Stop();
}

BENCHMARK(BM_CallAsync)->MeasureProcessCPUTime();

// Keep the entry points out of line so these benchmarks measure allocated
// frames, including the short-lived child, rather than allocation elision.
class TFrameCacheBenchmarkActor : public TActorBootstrapped<TFrameCacheBenchmarkActor> {
    benchmark::State& State;
    TPromise<void> Promise;
    ui64 Value = 0;

public:
    TFrameCacheBenchmarkActor(benchmark::State& state, TPromise<void> promise)
        : State(state)
        , Promise(std::move(promise))
    {}

    ~TFrameCacheBenchmarkActor() { Promise.SetValue(); }

    void Bootstrap() {
        Become(&TFrameCacheBenchmarkActor::StateWork);
        // Google Benchmark warmup invocations create new workers. Warm this
        // worker before timing and before snapshotting heap allocations.
        for (size_t i = 0; i < 64; ++i) {
            Root();
        }
        const auto before = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats().HeapAllocations;
        for (auto _ : State) {
            Root();
        }
        const auto stats = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats();
        State.counters["heap_allocations_per_op"] = double(stats.HeapAllocations - before) / State.iterations();
        State.counters["frame_allocations_per_op"] = 2;
        State.counters["retained_bytes"] = stats.CachedBytes;
        PassAway();
    }

    Y_NO_INLINE void Root() {
        benchmark::DoNotOptimize(co_await Child());
    }

    Y_NO_INLINE async<ui64> Child() {
        co_return ++Value;
    }

    STFUNC(StateWork) { Y_UNUSED(ev); }
};

void BM_FrameCacheDisabled(benchmark::State& state) {
    BM_YieldActor<TFrameCacheBenchmarkActor>(state, 0);
}

void BM_FrameCacheEnabled(benchmark::State& state) {
    BM_YieldActor<TFrameCacheBenchmarkActor>(state);
}

BENCHMARK(BM_FrameCacheDisabled)->MeasureProcessCPUTime();
BENCHMARK(BM_FrameCacheEnabled)->MeasureProcessCPUTime();

struct TChurnState {
    benchmark::State& State;
    TPromise<void> Promise;
    size_t WarmupLeft = 64;
    size_t HeapBefore = 0;
};

class TFrameCacheChurnActor : public TActorBootstrapped<TFrameCacheChurnActor> {
    std::shared_ptr<TChurnState> Context;
    ui64 Value = 0;
public:
    TFrameCacheChurnActor(benchmark::State& state, TPromise<void> promise)
        : Context(std::make_shared<TChurnState>(TChurnState{state, std::move(promise)}))
    {}
    explicit TFrameCacheChurnActor(std::shared_ptr<TChurnState> context)
        : Context(std::move(context))
    {}

    void Bootstrap() {
        Become(&TFrameCacheChurnActor::StateWork);
        auto& context = *Context;
        if (context.WarmupLeft) {
            Root();
            if (!--context.WarmupLeft) {
                context.HeapBefore = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats().HeapAllocations;
            }
        } else if (context.State.KeepRunning()) {
            Root();
        } else {
            const auto stats = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats();
            context.State.counters["heap_allocations_per_op"] =
                double(stats.HeapAllocations - context.HeapBefore) / context.State.iterations();
            context.State.counters["frame_allocations_per_op"] = 2;
            context.State.counters["retained_bytes"] = stats.CachedBytes;
            context.Promise.SetValue();
            PassAway();
            return;
        }
        Register(new TFrameCacheChurnActor(Context));
        PassAway();
    }

    Y_NO_INLINE void Root() { benchmark::DoNotOptimize(co_await Child()); }
    Y_NO_INLINE async<ui64> Child() { co_return ++Value; }
    STFUNC(StateWork) { Y_UNUSED(ev); }
};

void BM_FrameCacheChurnDisabled(benchmark::State& state) {
    BM_YieldActor<TFrameCacheChurnActor>(state, 0);
}
void BM_FrameCacheChurnEnabled(benchmark::State& state) {
    BM_YieldActor<TFrameCacheChurnActor>(state);
}
BENCHMARK(BM_FrameCacheChurnDisabled)->MeasureProcessCPUTime();
BENCHMARK(BM_FrameCacheChurnEnabled)->MeasureProcessCPUTime();

class TAsyncRescheduleRunnableActor : public TActorBootstrapped<TAsyncRescheduleRunnableActor> {
public:
    TAsyncRescheduleRunnableActor(benchmark::State& state, TPromise<void> promise)
        : State(state)
        , Promise(std::move(promise))
    {}

    ~TAsyncRescheduleRunnableActor() {
        Promise.SetValue();
    }

    struct TRescheduleRunnable : public TActorRunnableItem::TImpl<TRescheduleRunnable> {
        static constexpr bool IsActorAwareAwaiter = true;

        constexpr bool await_ready() { return false; }
        constexpr void await_resume() {}

        void await_suspend(std::coroutine_handle<> caller) noexcept {
            Caller = caller;
            TActorRunnableQueue::Schedule(this);
        }

        void DoRun(IActor*) noexcept {
            Caller.resume();
        }

        std::coroutine_handle<> Caller;
    };

    void Bootstrap() {
        Become(&TThis::StateWork);

        for (auto _ : State) {
            co_await TRescheduleRunnable{};
        }

        PassAway();
    }

    STFUNC(StateWork) {
        Y_UNUSED(ev);
    }

private:
    benchmark::State& State;
    TPromise<void> Promise;
};

void BM_RescheduleRunnableAsync(benchmark::State& state) {
    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
    setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>());
    setup->NodeId = 0;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[ setup->ExecutorsCount ]);
    for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
        setup->Executors[i] = new TBasicExecutorPool(i, 1, 1, "basic");
    }
    setup->Scheduler = new TBasicSchedulerThread;

    TActorSystem actorSystem(setup);
    actorSystem.Start();

    auto promise = NewPromise<void>();
    auto future = promise.GetFuture();
    actorSystem.Register(new TAsyncRescheduleRunnableActor(state, std::move(promise)));
    future.GetValueSync();

    actorSystem.Stop();
}

BENCHMARK(BM_RescheduleRunnableAsync)->MeasureProcessCPUTime();
