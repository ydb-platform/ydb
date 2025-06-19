#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/should_continue.h>
#include <util/system/sigset.h>
#include <util/generic/xrange.h>
#include <atomic>

using namespace NActors;

static TProgramShouldContinue ShouldContinue;

void OnTerminate(int) {
    ShouldContinue.ShouldStop();
}

struct TSharedState {
    std::atomic<ui64> PrevSmallTaskEndTs;
    std::atomic<ui64> MaxSmallTaskInterval;
};

class TWorkerActor : public TActorBootstrapped<TWorkerActor> {
public:
    TWorkerActor(TDuration duration, std::function<void()> onFinish)
        : _duration(duration)
        , _onFinish(onFinish)
    {}

    void Work() {
        ui64 sum = 0;
        ui64 end = GetCycleCountFast() + Us2Ts(_duration.MicroSeconds());

        while (GetCycleCountFast() < end) {
            for (auto& item : _data) {
                sum += item;
            }
            for (auto& item : _data) {
                item += sum++;
            }
        }
        _onFinish();
    }

    void Bootstrap() {
        ui64 i = 0;
        for (auto& item : _data) {
            item = i++;
        }
        Work();
        PassAway();
    }

private:
    std::array<ui64, 1000> _data;
    TDuration _duration;
    std::function<void()> _onFinish;
};

class TSchedulerActor : public TActorBootstrapped<TSchedulerActor> {
public:
    struct TWorkerConfig {
        i16 PoolId;
        TDuration Duration;
    };

    TSchedulerActor(TDuration interval, std::vector<TWorkerConfig> workers, std::function<void()> onFinish)
        : _interval(interval)
        , _workers(workers)
        , _onFinish(onFinish)
    {}

    void Bootstrap() {
        Become(&TSchedulerActor::StateWork);
        HandleWakeUp();
    }

    void CreateWorkers() {
        for (auto& worker : _workers) {
            Register(new TWorkerActor(worker.Duration, _onFinish), TMailboxType::HTSwap, worker.PoolId);
        }
    }

    void HandleWakeUp() {
        CreateWorkers();
        Schedule(_interval, new NActors::TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(NActors::TEvents::TEvWakeup, HandleWakeUp);
        }
    }

private:
    TDuration _interval;
    std::vector<TWorkerConfig> _workers;
    std::function<void()> _onFinish;
};

class TSmallTaskActor : public TSchedulerActor {
public:
    TSmallTaskActor(std::function<void()> onFinish)
        : TSchedulerActor(TDuration::MilliSeconds(10), {
            {0, TDuration::MilliSeconds(1)}
        }, onFinish)
    {}

    void Bootstrap() {
        TSchedulerActor::Bootstrap();
    }
};

class THugeTaskActor : public TSchedulerActor {
public:
    THugeTaskActor(std::function<void()> onFinish, i16 firstPoolTaskCount, i16 secondPoolTaskCount)
        : TSchedulerActor(TDuration::MilliSeconds(500), GetWorkers(firstPoolTaskCount, secondPoolTaskCount), onFinish)
    {}

    std::vector<TSchedulerActor::TWorkerConfig> GetWorkers(i16 firstPoolTaskCount, i16 secondPoolTaskCount) {
        std::vector<TSchedulerActor::TWorkerConfig> workers;
        for (i16 i = 0; i < firstPoolTaskCount; ++i) {
            workers.emplace_back(0, TDuration::MilliSeconds(100));
        }
        for (i16 i = 0; i < secondPoolTaskCount; ++i) {
            workers.emplace_back(1, TDuration::MilliSeconds(100));
        }
        return workers;
    }
};

THolder<TActorSystemSetup> BuildActorSystemSetup(bool useSharedThread = true) {
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = 1;
    setup->CpuManager.Basic.emplace_back(TBasicExecutorPoolConfig{
        .PoolId = 0,
        .Threads = 3,
        .SpinThreshold = 0,
        .MinThreadCount = 1,
        .MaxThreadCount = 3,
        .DefaultThreadCount = 1,
        .HasSharedThread = useSharedThread,
    });
    setup->CpuManager.Basic.emplace_back(TBasicExecutorPoolConfig{
        .PoolId = 1,
        .Threads = 3,
        .SpinThreshold = 0,
        .MinThreadCount = 2,
        .MaxThreadCount = 4,
        .DefaultThreadCount = 2,
        .HasSharedThread = useSharedThread,
    });

    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

THolder<TActorSystemSetup> BuildActorSystemSetupSharedOnlyCore() {
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = 1;
    setup->CpuManager.Basic.emplace_back(TBasicExecutorPoolConfig{
        .PoolId = 0,
        .Threads = 0,
        .SpinThreshold = 0,
        .MinThreadCount = 0,
        .MaxThreadCount = 1,
        .DefaultThreadCount = 1,
        .HasSharedThread = true,
        .AdjacentPools = {1},
    });
    setup->CpuManager.Basic.emplace_back(TBasicExecutorPoolConfig{
        .PoolId = 1,
        .Threads = 0,
        .SpinThreshold = 0,
        .MinThreadCount = 0,
        .MaxThreadCount = 0,
        .DefaultThreadCount = 0,
        .HasSharedThread = false,
        .ForcedForeignSlotCount = 1,
    });

    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

void TestSpecificCase(THolder<TActorSystemSetup> actorSystemSetup, i16 firstPoolTaskCount, i16 secondPoolTaskCount) {
    TActorSystem actorSystem(actorSystemSetup);

    actorSystem.Start();

    TSharedState state;
    state.PrevSmallTaskEndTs.store(GetCycleCountFast());
    actorSystem.Register(new TSmallTaskActor([&state]() {
        ui64 current = GetCycleCountFast();
        ui64 prev = state.PrevSmallTaskEndTs.exchange(current);
        ui64 interval = current - prev;
        for (;;) {
            ui64 maxInterval = state.MaxSmallTaskInterval.load();
            if (interval > maxInterval) {
                if (state.MaxSmallTaskInterval.compare_exchange_weak(maxInterval, interval)) {
                    break;
                }
            } else {
                break;
            }
        }
    }));
    actorSystem.Register(new THugeTaskActor([]{}, firstPoolTaskCount, secondPoolTaskCount));

    auto printState = [&](i16 poolId) {
        TExecutorPoolState state;
        actorSystem.GetExecutorPoolState(poolId, state);
        TStringBuilder flagBuilder;
        if (state.IsNeedy) {
            flagBuilder << "N";
        }
        if (state.IsHoggish) {
            flagBuilder << "H";
        }
        if (state.IsStarved) {
            flagBuilder << "S";
        }
        if (!flagBuilder.empty()) {
            flagBuilder << " ";
        }
        Cout << "Pool " << poolId << " state: " << flagBuilder << state.ElapsedCpu << " " << state.CurrentLimit << "/" << state.PossibleMaxLimit << "(" << state.MaxLimit << ") shared cpu quota: " << state.SharedCpuQuota << Endl;
    };

    ui64 seconds = 0;
    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(1000));
        ui64 maxInterval = state.MaxSmallTaskInterval.exchange(0);
        Cout << "--------------------------------" << Endl;
        Cout << "Seconds: " << seconds++ << Endl;
        Cout << "Max small task interval: " << std::round(Ts2Us(maxInterval))  << " us" << Endl;
        printState(0);
        printState(1);
        THarmonizerStats stats = actorSystem.GetHarmonizerStats();
        Cout << "Harmonizer stats: budget: " << stats.Budget << " shared free cpu: " << stats.SharedFreeCpu << Endl;
    }

    actorSystem.Stop();
    actorSystem.Cleanup();
}

int main(int argc, char **argv) {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    THolder<TActorSystemSetup> actorSystemSetup;
    i16 firstPoolTaskCount = 0;
    i16 secondPoolTaskCount = 0;
    if (argc > 1) {
        if (argv[1] == std::string("specific-case")) {
            actorSystemSetup = BuildActorSystemSetup(true);
            firstPoolTaskCount = 6;
            secondPoolTaskCount = 3;
        } else if (argv[1] == std::string("specific-case-no-shared-thread")) {
            actorSystemSetup = BuildActorSystemSetup(false);
            firstPoolTaskCount = 6;
            secondPoolTaskCount = 3;
        } else if (argv[1] == std::string("2-cores")) {
            actorSystemSetup = BuildActorSystemSetupSharedOnlyCore();
            firstPoolTaskCount = 2;
            secondPoolTaskCount = 1;
        } else {
            Cout << "Usage: " << argv[0] << " specific-case|specific-case-no-shared-thread|2-cores" << Endl;
            return 1;
        }
    } else {
        Cout << "Usage: " << argv[0] << " specific-case|specific-case-no-shared-thread|2-cores" << Endl;
        return 1;
    }

    Cerr << "First pool task count: " << firstPoolTaskCount << ", second pool task count: " << secondPoolTaskCount << Endl;
    TestSpecificCase(std::move(actorSystemSetup), firstPoolTaskCount, secondPoolTaskCount);
    return ShouldContinue.GetReturnCode();
}
