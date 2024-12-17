#include "ext_counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/graph/api/events.h>
#include <ydb/core/graph/api/service.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKikimr {
namespace NSysView {

class TExtCountersUpdaterActor
    : public TActorBootstrapped<TExtCountersUpdaterActor>
{
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
    using THistogramPtr = ::NMonitoring::THistogramPtr;
    using THistogramSnapshotPtr = ::NMonitoring::IHistogramSnapshotPtr;

    const TExtCountersConfig Config;

    TCounterPtr MemoryUsedBytes;
    TCounterPtr MemoryLimitBytes;
    TCounterPtr StorageUsedBytes;
    TCounterPtr StorageUsedBytesOnSsd;
    TCounterPtr StorageUsedBytesOnHdd;
    TVector<TCounterPtr> CpuUsedCorePercents;
    TVector<TCounterPtr> CpuLimitCorePercents;
    THistogramPtr ExecuteLatencyMs;

    TCounterPtr AnonRssSize;
    TCounterPtr CGroupMemLimit;
    TCounterPtr MemoryHardLimit;
    TVector<TCounterPtr> PoolElapsedMicrosec;
    TVector<TCounterPtr> PoolCurrentThreadCount;
    TVector<ui64> PoolElapsedMicrosecPrevValue;
    TVector<ui64> ExecuteLatencyMsValues;
    TVector<ui64> ExecuteLatencyMsPrevValues;
    TVector<ui64> ExecuteLatencyMsBounds;

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
        StorageUsedBytes = ydbGroup->GetNamedCounter("name",
            "resources.storage.used_bytes", false);
        StorageUsedBytesOnSsd = ydbGroup->GetNamedCounter("name",
            "resources.storage.used_bytes.ssd", false);
        StorageUsedBytesOnHdd = ydbGroup->GetNamedCounter("name",
            "resources.storage.used_bytes.hdd", false);

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

        ExecuteLatencyMs = ydbGroup->FindNamedHistogram("name", "table.query.execution.latency_milliseconds");

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
            PoolElapsedMicrosecPrevValue.resize(Config.Pools.size());
            for (size_t i = 0; i < Config.Pools.size(); ++i) {
                auto poolGroup = utilsGroup->FindSubgroup("execpool", Config.Pools[i].Name);
                if (poolGroup) {
                    PoolElapsedMicrosec[i] = poolGroup->FindCounter("ElapsedMicrosec");
                    PoolCurrentThreadCount[i] = poolGroup->FindCounter("CurrentThreadCount");
                    if (PoolElapsedMicrosec[i]) {
                        PoolElapsedMicrosecPrevValue[i] = PoolElapsedMicrosec[i]->Val();
                    }
                }
            }
        }
        if (!MemoryHardLimit) {
            auto utilsGroup = GetServiceCounters(AppData()->Counters, "utils");
            auto memoryControllerGroup = utilsGroup->FindSubgroup("component", "memory_controller");
            if (memoryControllerGroup) {
                MemoryHardLimit = memoryControllerGroup->FindCounter("Stats/HardLimit");
            }
        }
    }

    void Transform() {
        Initialize();
        auto metrics(MakeHolder<NGraph::TEvGraph::TEvSendMetrics>());
        if (AnonRssSize) {
            MemoryUsedBytes->Set(AnonRssSize->Val());
            metrics->AddMetric("resources.memory.used_bytes", AnonRssSize->Val());
        }
        if (CGroupMemLimit && !MemoryHardLimit) {
            MemoryLimitBytes->Set(CGroupMemLimit->Val());
        } else if (MemoryHardLimit) {
            MemoryLimitBytes->Set(MemoryHardLimit->Val());
        }
        if (StorageUsedBytes->Val() != 0) {
            metrics->AddMetric("resources.storage.used_bytes", StorageUsedBytes->Val());
        }
        if (StorageUsedBytesOnSsd->Val() != 0) {
            metrics->AddMetric("resources.storage.used_bytes.ssd", StorageUsedBytesOnSsd->Val());
        }
        if (StorageUsedBytesOnHdd->Val() != 0) {
            metrics->AddMetric("resources.storage.used_bytes.hdd", StorageUsedBytesOnHdd->Val());
        }
        if (!Config.Pools.empty()) {
            double cpuUsage = 0;
            for (size_t i = 0; i < Config.Pools.size(); ++i) {
                double usedCore = 0;
                double limitCore = 0;
                if (PoolElapsedMicrosec[i]) {
                    auto elapsedMs = PoolElapsedMicrosec[i]->Val();
                    CpuUsedCorePercents[i]->Set(elapsedMs / 10000.);
                    if (PoolElapsedMicrosecPrevValue[i] != 0) {
                        usedCore = (elapsedMs - PoolElapsedMicrosecPrevValue[i]) / 1000000.;
                        cpuUsage += usedCore;
                    }
                    PoolElapsedMicrosecPrevValue[i] = elapsedMs;
                }
                if (PoolCurrentThreadCount[i] && PoolCurrentThreadCount[i]->Val()) {
                    limitCore = PoolCurrentThreadCount[i]->Val();
                    CpuLimitCorePercents[i]->Set(limitCore * 100);
                } else {
                    limitCore = Config.Pools[i].ThreadCount * 100;
                    CpuLimitCorePercents[i]->Set(limitCore * 100);
                }
                if (limitCore > 0) {
                    metrics->AddArithmeticMetric(TStringBuilder() << "resources.cpu." << Config.Pools[i].Name << ".usage",
                        usedCore, '/', limitCore);
                }
            }
            metrics->AddMetric("resources.cpu.usage", cpuUsage);
        }
        if (ExecuteLatencyMs) {
            THistogramSnapshotPtr snapshot = ExecuteLatencyMs->Snapshot();
            ui32 count = snapshot->Count();
            if (ExecuteLatencyMsValues.empty()) {
                ExecuteLatencyMsValues.resize(count);
                ExecuteLatencyMsPrevValues.resize(count);
                ExecuteLatencyMsBounds.resize(count);
            }
            ui64 total = 0;
            for (ui32 n = 0; n < count; ++n) {
                ui64 value = snapshot->Value(n);;
                ui64 diff = value - ExecuteLatencyMsPrevValues[n];
                total += diff;
                ExecuteLatencyMsValues[n] = diff;
                ExecuteLatencyMsPrevValues[n] = value;
                if (ExecuteLatencyMsBounds[n] == 0) {
                    NMonitoring::TBucketBound bound = snapshot->UpperBound(n);
                    ExecuteLatencyMsBounds[n] = bound == Max<NMonitoring::TBucketBound>() ? Max<ui64>() : bound;
                }
            }
            metrics->AddMetric("queries.requests", total);
            if (total != 0) {
                metrics->AddHistogramMetric("queries.latencies", ExecuteLatencyMsValues, ExecuteLatencyMsBounds);
            }
        }
        if (metrics->Record.MetricsSize() > 0) {
            Send(NGraph::MakeGraphServiceId(), metrics.Release());
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
