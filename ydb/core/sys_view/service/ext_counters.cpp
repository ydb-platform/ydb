#include "ext_counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NSysView {

class TExtCountersUpdaterActor
    : public TActorBootstrapped<TExtCountersUpdaterActor>
{
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    const TExtCountersConfig Config;

    TCounterPtr MemoryUsedBytes;
    TCounterPtr MemoryLimitBytes;
    TVector<TCounterPtr> CpuUsedCorePercents;
    TVector<TCounterPtr> CpuLimitCorePercents;

    TCounterPtr AnonRssSize;
    TCounterPtr CGroupMemLimit;
    TVector<TCounterPtr> PoolElapsedMicrosec;
    TVector<TCounterPtr> PoolCurrentThreadCount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXT_COUNTERS_UPDATER_ACTOR;
    }

    explicit TExtCountersUpdaterActor(TExtCountersConfig&& config)
        : Config(std::move(config))
    {}

    void Bootstrap() {
        auto ydbGroup = GetServiceCounters(AppData()->Counters, "ydb");

        MemoryUsedBytes = ydbGroup->GetNamedCounter("name",
            "resources.memory.used_bytes", false);
        MemoryLimitBytes = ydbGroup->GetNamedCounter("name",
            "resources.memory.limit_bytes", false);

        auto poolCount = Config.Pools.size();
        CpuUsedCorePercents.resize(poolCount);
        CpuLimitCorePercents.resize(poolCount);

        for (size_t i = 0; i < poolCount; ++i) {
            auto name = to_lower(Config.Pools[i].Name);
            CpuUsedCorePercents[i] = ydbGroup->GetSubgroup("pool", name)->GetNamedCounter("name",
                "resources.cpu.used_core_percents", true);
            CpuLimitCorePercents[i] = ydbGroup->GetSubgroup("pool", name)->GetNamedCounter("name",
                "resources.cpu.limit_core_percents", false);
        }

        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        Become(&TThis::StateWork);
    }

private:
    void Initialize() {
        if (!AnonRssSize) {
            auto utilsGroup = GetServiceCounters(AppData()->Counters, "utils");

            AnonRssSize = utilsGroup->FindCounter("Process/AnonRssSize");
            CGroupMemLimit = utilsGroup->FindCounter("Process/CGroupMemLimit");

            PoolElapsedMicrosec.resize(Config.Pools.size());
            PoolCurrentThreadCount.resize(Config.Pools.size());
            for (size_t i = 0; i < Config.Pools.size(); ++i) {
                auto poolGroup = utilsGroup->FindSubgroup("execpool", Config.Pools[i].Name);
                if (poolGroup) {
                    PoolElapsedMicrosec[i] = poolGroup->FindCounter("ElapsedMicrosec");
                    PoolCurrentThreadCount[i] = poolGroup->FindCounter("CurrentThreadCount");
                }
            }
        }
    }

    void Transform() {
        Initialize();

        if (AnonRssSize) {
            MemoryUsedBytes->Set(AnonRssSize->Val());
        }
        if (CGroupMemLimit) {
            MemoryLimitBytes->Set(CGroupMemLimit->Val());
        }
        for (size_t i = 0; i < Config.Pools.size(); ++i) {
            if (PoolElapsedMicrosec[i]) {
                double usedCore = PoolElapsedMicrosec[i]->Val() / 10000.;
                CpuUsedCorePercents[i]->Set(usedCore);
            }
            if (PoolCurrentThreadCount[i] && PoolCurrentThreadCount[i]->Val()) {
                double limitCore = PoolCurrentThreadCount[i]->Val() * 100;
                CpuLimitCorePercents[i]->Set(limitCore);
            } else {
                double limitCore = Config.Pools[i].ThreadCount * 100;
                CpuLimitCorePercents[i]->Set(limitCore);
            }
        }
    }

    void HandleWakeup() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        Transform();
    }

    STRICT_STFUNC(StateWork, {
        cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
    })
};

IActor* CreateExtCountersUpdater(TExtCountersConfig&& config) {
    return new TExtCountersUpdaterActor(std::move(config));
}

}
}

