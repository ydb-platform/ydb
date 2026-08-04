#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/core/base/appdata.h>
#include <future>

using namespace NActors;

inline THolder<TActorSystemSetup> CreateSimpleSetupForFuzzer() {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 10, "fuzz"));
    TSchedulerConfig schedulerCfg;
    setup->Scheduler.Reset(CreateSchedulerThread(schedulerCfg));
    return setup;
}

template<typename TFunc>
class TFuzzRunnerActor : public TActorBootstrapped<TFuzzRunnerActor<TFunc>> {
public:
    TFuzzRunnerActor(TFunc func, std::promise<void>&& done)
        : Func(std::move(func))
        , Done(std::move(done))
    {}

    void Bootstrap() {
        try {
            Func();
        } catch (...) {
        }
        Done.set_value();
        this->PassAway();
    }

private:
    TFunc Func;
    std::promise<void> Done;
};

template<typename TFunc>
void RunWithMockedActorSystem(TFunc&& func) {
    auto setup = CreateSimpleSetupForFuzzer();
    TMap<TString, ui32> servicePools;
    auto appData = std::make_unique<NKikimr::TAppData>(0, 1, 2, 3, servicePools, nullptr, nullptr, nullptr, nullptr);
    TActorSystem actorSystem(setup, appData.get());
    actorSystem.Start();

    std::promise<void> done;
    auto fut = done.get_future();
    actorSystem.Register(new TFuzzRunnerActor<TFunc>(std::forward<TFunc>(func), std::move(done)));
    fut.wait();

    actorSystem.Stop();
}
