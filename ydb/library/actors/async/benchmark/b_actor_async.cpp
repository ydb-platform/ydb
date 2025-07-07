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

        for (auto _ : State) {
            co_await Step();
        }

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
void BM_PingActor(benchmark::State& state) {
    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
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
void BM_YieldActor(benchmark::State& state) {
    THolder<TActorSystemSetup> setup(new TActorSystemSetup);
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

        for (auto _ : State) {
            benchmark::DoNotOptimize(co_await Step());
        }

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
