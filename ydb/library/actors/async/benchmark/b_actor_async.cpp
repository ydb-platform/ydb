#include <benchmark/benchmark.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
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
void BM_Actor(benchmark::State& state) {
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

void BM_ManualActor(benchmark::State& state) {
    BM_Actor<TPingDriverManualActor>(state);
}

BENCHMARK(BM_ManualActor)->MeasureProcessCPUTime();

void BM_AsyncActor(benchmark::State& state) {
    BM_Actor<TPingDriverAsyncActor>(state);
}

BENCHMARK(BM_AsyncActor)->MeasureProcessCPUTime();

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
