#include "config_helpers.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/cpu_topology.h>

#include <optional>
#include <utility>

namespace NKikimr {

namespace NActorSystemConfigHelpers {

namespace {

using TExecutorConfig = NKikimrConfig::TActorSystemConfig::TExecutor;

template <class TConfig>
static TCpuMask ParseAffinity(const TConfig& cfg) {
    TCpuMask result;
    if (cfg.GetCpuList()) {
        result = TCpuMask(cfg.GetCpuList());
    } else if (cfg.GetX().size() > 0) {
        result = TCpuMask(cfg.GetX().data(), cfg.GetX().size());
    } else {  // use all processors
        TAffinity available;
        available.Current();
        result = available;
    }
    if (cfg.GetExcludeCpuList()) {
        result = result - TCpuMask(cfg.GetExcludeCpuList());
    }
    return result;
}

TDuration GetSelfPingInterval(const NKikimrConfig::TActorSystemConfig& systemConfig) {
    return systemConfig.HasSelfPingInterval()
        ? TDuration::MicroSeconds(systemConfig.GetSelfPingInterval())
        : TDuration::MilliSeconds(10);
}

NActors::EASProfile ConvertActorSystemProfile(NKikimrConfig::TActorSystemConfig::EActorSystemProfile profile) {
    switch (profile) {
        case NKikimrConfig::TActorSystemConfig::DEFAULT:
            return NActors::EASProfile::Default;
        case NKikimrConfig::TActorSystemConfig::LOW_CPU_CONSUMPTION:
            return NActors::EASProfile::LowCpuConsumption;
        case NKikimrConfig::TActorSystemConfig::LOW_LATENCY:
            return NActors::EASProfile::LowLatency;
    }
}

const TCpuTopologyGroup& GetPlacementGroup(
        const TExecutorConfig& poolConfig, const TCpuTopology& cpuTopology, ui32 poolId) {
    const ui32 groupIndex = poolConfig.GetPlacement();
    Y_ABORT_UNLESS(groupIndex < cpuTopology.PlacementGroups.size(),
        "Executor id %" PRIu32 " ('%s') placement group index %" PRIu32
        " is out of range; available placement groups: %zu",
        poolId, poolConfig.GetName().c_str(), groupIndex, cpuTopology.PlacementGroups.size());

    const auto& placementGroup = cpuTopology.PlacementGroups[groupIndex];
    Y_ABORT_UNLESS(placementGroup.Cpus.CpuCount(),
        "Executor id %" PRIu32 " ('%s') placement group index %" PRIu32 " has no CPUs",
        poolId, poolConfig.GetName().c_str(), groupIndex);
    return placementGroup;
}

void AddExecutorPool(
        NActors::TCpuManagerConfig& cpuManager,
        const TExecutorConfig& poolConfig,
        const NKikimrConfig::TActorSystemConfig& systemConfig,
        ui32 poolId,
        const NMonitoring::TDynamicCounterPtr& counters,
        const TCpuTopology* cpuTopology) {
    Y_ABORT_UNLESS(!poolConfig.HasHarmonizerNeedyCpuWindowSeconds()
        || poolConfig.GetType() == TExecutorConfig::BASIC,
        "HarmonizerNeedyCpuWindowSeconds is supported only for BASIC executors");
    Y_ABORT_UNLESS(!poolConfig.HasEnableWaker()
        || poolConfig.GetType() == NKikimrConfig::TActorSystemConfig::TExecutor::BASIC,
        "EnableWaker is supported only for BASIC executors");

    switch (poolConfig.GetType()) {
        case TExecutorConfig::BASIC: {
            NActors::TBasicExecutorPoolConfig basic;
            basic.PoolId = poolId;
            basic.PoolName = poolConfig.GetName();
            if (poolConfig.HasPlacement()) {
                Y_ABORT_UNLESS(cpuTopology);
                const auto& placementGroup = GetPlacementGroup(poolConfig, *cpuTopology, poolId);
                basic.Affinity = placementGroup.Cpus;
            } else {
                basic.Affinity = ParseAffinity(poolConfig.GetAffinity());
            }
            if (poolConfig.HasMaxAvgPingDeviation() && counters) {
                auto poolGroup = counters->GetSubgroup("execpool", basic.PoolName);
                auto& poolInfo = cpuManager.PingInfoByPool[poolId];
                poolInfo.AvgPingCounter = poolGroup->GetCounter("SelfPingAvgUs", false);
                poolInfo.AvgPingCounterWithSmallWindow = poolGroup->GetCounter("SelfPingAvgUsIn1s", false);
                const TDuration maxAvgPing = GetSelfPingInterval(systemConfig)
                    + TDuration::MicroSeconds(poolConfig.GetMaxAvgPingDeviation());
                poolInfo.MaxAvgPingUs = maxAvgPing.MicroSeconds();
            }
            basic.Threads = Max(poolConfig.GetThreads(), poolConfig.GetMaxThreads());
            basic.SpinThreshold = poolConfig.GetSpinThreshold();
            basic.RealtimePriority = poolConfig.GetRealtimePriority();
            basic.HasSharedThread = poolConfig.GetHasSharedThread();
            basic.EnableWaker = poolConfig.GetEnableWaker();
            if (poolConfig.HasTimePerMailboxMicroSecs()) {
                basic.TimePerMailbox = TDuration::MicroSeconds(poolConfig.GetTimePerMailboxMicroSecs());
            } else if (systemConfig.HasTimePerMailboxMicroSecs()) {
                basic.TimePerMailbox = TDuration::MicroSeconds(systemConfig.GetTimePerMailboxMicroSecs());
            }
            if (poolConfig.HasEventsPerMailbox()) {
                basic.EventsPerMailbox = poolConfig.GetEventsPerMailbox();
            } else if (systemConfig.HasEventsPerMailbox()) {
                basic.EventsPerMailbox = systemConfig.GetEventsPerMailbox();
            }
            basic.ActorSystemProfile = ConvertActorSystemProfile(systemConfig.GetActorSystemProfile());
            Y_ABORT_UNLESS(basic.EventsPerMailbox != 0);
            basic.MinThreadCount = poolConfig.GetMinThreads();
            basic.MaxThreadCount = poolConfig.GetMaxThreads();
            basic.DefaultThreadCount = poolConfig.GetThreads();
            basic.Priority = poolConfig.GetPriority();
            const ui32 harmonizerNeedyCpuWindowSeconds = poolConfig.GetHarmonizerNeedyCpuWindowSeconds();
            Y_ABORT_UNLESS(harmonizerNeedyCpuWindowSeconds >= 1 && harmonizerNeedyCpuWindowSeconds <= 32,
                "HarmonizerNeedyCpuWindowSeconds must be in range [1, 32], got %" PRIu32,
                harmonizerNeedyCpuWindowSeconds);
            basic.HarmonizerNeedyCpuWindowSeconds = static_cast<ui8>(harmonizerNeedyCpuWindowSeconds);
            for (const auto& pool : poolConfig.GetAdjacentPools()) {
                basic.AdjacentPools.push_back(pool);
            }
            if (poolConfig.HasForcedForeignSlots()) {
                basic.ForcedForeignSlotCount = poolConfig.GetForcedForeignSlots();
            }
            if (poolConfig.HasAllThreadsAreShared()) {
                basic.AllThreadsAreShared = poolConfig.GetAllThreadsAreShared();
            }
            cpuManager.Basic.emplace_back(std::move(basic));
            break;
        }

        case TExecutorConfig::IO: {
            NActors::TIOExecutorPoolConfig io;
            io.PoolId = poolId;
            io.PoolName = poolConfig.GetName();
            io.Threads = poolConfig.GetThreads();
            io.Affinity = ParseAffinity(poolConfig.GetAffinity());
            cpuManager.IO.emplace_back(std::move(io));
            break;
        }

        default:
            Y_ABORT();
    }
}

void AddExecutorPoolsImpl(
        NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig,
        const NMonitoring::TDynamicCounterPtr& counters,
        const TCpuTopology* suppliedCpuTopology) {
    bool hasPlacement = false;
    for (ui32 poolId = 0; poolId < static_cast<ui32>(systemConfig.ExecutorSize()); ++poolId) {
        const auto& poolConfig = systemConfig.GetExecutor(poolId);
        if (poolConfig.HasPlacement()) {
            hasPlacement = true;
            Y_ABORT_UNLESS(!poolConfig.HasAffinity(),
                "Executor id %" PRIu32 " ('%s') must not define both Affinity and Placement",
                poolId, poolConfig.GetName().c_str());
            Y_ABORT_UNLESS(poolConfig.GetType() == TExecutorConfig::BASIC,
                "Executor id %" PRIu32 " ('%s') must be BASIC to define Placement",
                poolId, poolConfig.GetName().c_str());
        }
    }

    std::optional<TCpuTopology> parsedCpuTopology;
    const TCpuTopology* cpuTopology = suppliedCpuTopology;
    if (hasPlacement) {
        if (!cpuTopology) {
            auto result = ParseCpuTopology();
            Y_ABORT_UNLESS(result, "Failed to parse CPU topology for executor placement: %s", result.error().c_str());
            parsedCpuTopology.emplace(std::move(*result));
            cpuTopology = &*parsedCpuTopology;
        }
        ui32 cpuCountPerGroup = 0;
        for (const auto& group : cpuTopology->PlacementGroups) {
            ui32 curGroupCpuCount = group.Cpus.CpuCount();
            if (!cpuCountPerGroup) {
                cpuCountPerGroup = curGroupCpuCount;
            }
            Y_ABORT_UNLESS(cpuCountPerGroup == curGroupCpuCount, "CPU topology placement groups are uneven");
        }
    }

    cpuManager.PingInfoByPool.resize(systemConfig.ExecutorSize());
    for (ui32 poolId = 0; poolId < static_cast<ui32>(systemConfig.ExecutorSize()); ++poolId) {
        AddExecutorPool(cpuManager, systemConfig.GetExecutor(poolId), systemConfig,
            poolId, counters, cpuTopology);
    }
}

} // anonymous namespace

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig,
        NMonitoring::TDynamicCounterPtr counters) {
    AddExecutorPoolsImpl(cpuManager, systemConfig, counters, nullptr);
}

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig,
        NMonitoring::TDynamicCounterPtr counters,
        const TCpuTopology& cpuTopology) {
    AddExecutorPoolsImpl(cpuManager, systemConfig, counters, &cpuTopology);
}

TVector<ui32> GetInterconnectSessionExecutorPoolIds(
        const NKikimrConfig::TActorSystemConfig& systemConfig) {
    const auto& poolIds = systemConfig.GetInterconnectSessionExecutor();
    return TVector<ui32>(poolIds.begin(), poolIds.end());
}

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config) {
    const ui64 resolution = config.HasResolution() ? config.GetResolution() : 1024;
    Y_DEBUG_ABORT_UNLESS((resolution & (resolution - 1)) == 0);  // resolution must be power of 2
    const ui64 spinThreshold = config.HasSpinThreshold() ? config.GetSpinThreshold() : 0;
    const ui64 progressThreshold = config.HasProgressThreshold() ? config.GetProgressThreshold() : 10000;
    const bool useSchedulerActor = config.HasUseSchedulerActor() ? config.GetUseSchedulerActor() : false;

    return NActors::TSchedulerConfig(resolution, spinThreshold, progressThreshold, useSchedulerActor);
}

}  // namespace NActorSystemConfigHelpers

namespace NKikimrConfigHelpers {

NMemory::TResourceBrokerConfig CreateMemoryControllerResourceBrokerConfig(const NKikimrConfig::TAppConfig& config) {
    NMemory::TResourceBrokerConfig resourceBrokerSelfConfig; // for backward compatibility
    auto mergeResourceBrokerConfigs = [&](const NKikimrResourceBroker::TResourceBrokerConfig& resourceBrokerConfig) {
        if (resourceBrokerConfig.HasResourceLimit() && resourceBrokerConfig.GetResourceLimit().HasMemory()) {
            resourceBrokerSelfConfig.LimitBytes = resourceBrokerConfig.GetResourceLimit().GetMemory();
        }
        for (const auto& queue : resourceBrokerConfig.GetQueues()) {
            if (queue.HasLimit() && queue.GetLimit().HasMemory()) {
                resourceBrokerSelfConfig.QueueLimits[queue.GetName()] = queue.GetLimit().GetMemory();
            }
        }
    };
    if (config.HasBootstrapConfig() && config.GetBootstrapConfig().HasResourceBroker()) {
        mergeResourceBrokerConfigs(config.GetBootstrapConfig().GetResourceBroker());
    }
    if (config.HasResourceBrokerConfig()) {
        mergeResourceBrokerConfigs(config.GetResourceBrokerConfig());
    }
    return resourceBrokerSelfConfig;
}

}  // namespace NKikimrConfigHelpers

}  // namespace NKikimr
