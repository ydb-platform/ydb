#include "config_helpers.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/cpu_topology.h>

#include <util/string/builder.h>

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

TString GetStoragePoolName(const TExecutorConfig& poolConfig, ui32 groupIndex, ui32 placementGroups) {
    const TString baseName = poolConfig.HasName() ? poolConfig.GetName() : "Storage";
    return placementGroups == 1 ? baseName : TStringBuilder() << baseName << groupIndex;
}

ui32 GetExecutorPoolCount(const NKikimrConfig::TActorSystemConfig& systemConfig) {
    ui32 poolCount = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        if (poolConfig.GetType() != TExecutorConfig::NUMA) {
            ++poolCount;
        } else {
            poolCount += poolConfig.GetPlacementGroups();
        }
    }
    return poolCount;
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

void AddExecutorPool(
    NActors::TCpuManagerConfig& cpuManager,
    const TExecutorConfig& poolConfig,
    const NKikimrConfig::TActorSystemConfig& systemConfig,
    ui32 poolId,
    const TString& poolName,
    NMonitoring::TDynamicCounterPtr counters,
    const std::optional<TCpuTopologyGroup>& placementGroup = std::nullopt,
    const std::optional<TCpuMask>& defaultAffinity = std::nullopt)
{
    switch (poolConfig.GetType()) {
        case TExecutorConfig::BASIC:
        case TExecutorConfig::NUMA: {
            NActors::TBasicExecutorPoolConfig basic;
            basic.PoolId = poolId;
            basic.PoolName = poolName;
            basic.UseRingQueue = systemConfig.HasUseRingQueue() && systemConfig.GetUseRingQueue();
            if (poolConfig.GetType() == TExecutorConfig::NUMA) {
                Y_ABORT_UNLESS(placementGroup);
                Y_ABORT_UNLESS(!poolConfig.HasAffinity(), "NUMA executor must not define Affinity");
                Y_ABORT_UNLESS(placementGroup->Cpus.CpuCount(), "NUMA executor placement group %" PRIu32 " has no CPUs", placementGroup->Id);
                basic.Threads = poolConfig.HasPlacementGroupThreads()
                    ? poolConfig.GetPlacementGroupThreads()
                    : placementGroup->Cpus.CpuCount();
                Y_ABORT_UNLESS(basic.Threads, "NUMA executor placement group %" PRIu32 " has zero threads", placementGroup->Id);
                basic.Affinity = placementGroup->Cpus;
                basic.MinThreadCount = basic.Threads;
                basic.MaxThreadCount = basic.Threads;
                basic.DefaultThreadCount = basic.Threads;
            } else {
                Y_ABORT_UNLESS(!placementGroup);
                basic.Threads = Max(poolConfig.GetThreads(), poolConfig.GetMaxThreads());
                if (!poolConfig.HasAffinity() && defaultAffinity) {
                    Y_ABORT_UNLESS(defaultAffinity->CpuCount(),
                        "NUMA placement groups consume all CPUs; executor pool '%s' has no CPUs left and no explicit Affinity",
                        poolName.c_str());
                    basic.Affinity = *defaultAffinity;
                } else {
                    basic.Affinity = ParseAffinity(poolConfig.GetAffinity());
                }
                basic.MinThreadCount = poolConfig.GetMinThreads();
                basic.MaxThreadCount = poolConfig.GetMaxThreads();
                basic.DefaultThreadCount = poolConfig.GetThreads();
            }
            if (poolConfig.HasMaxAvgPingDeviation() && counters) {
                auto poolGroup = counters->GetSubgroup("execpool", basic.PoolName);
                auto& poolInfo = cpuManager.PingInfoByPool[poolId];
                poolInfo.AvgPingCounter = poolGroup->GetCounter("SelfPingAvgUs", false);
                poolInfo.AvgPingCounterWithSmallWindow = poolGroup->GetCounter("SelfPingAvgUsIn1s", false);
                const TDuration maxAvgPing = GetSelfPingInterval(systemConfig) + TDuration::MicroSeconds(poolConfig.GetMaxAvgPingDeviation());
                poolInfo.MaxAvgPingUs = maxAvgPing.MicroSeconds();
            }
            basic.SpinThreshold = poolConfig.GetSpinThreshold();
            basic.RealtimePriority = poolConfig.GetRealtimePriority();
            basic.HasSharedThread = poolConfig.GetHasSharedThread();
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
            basic.Priority = poolConfig.GetPriority();
            if (poolConfig.HasMinLocalQueueSize()) {
                basic.MinLocalQueueSize = poolConfig.GetMinLocalQueueSize();
            }
            if (poolConfig.HasMaxLocalQueueSize()) {
                basic.MaxLocalQueueSize = poolConfig.GetMaxLocalQueueSize();
            }
            for (const auto& pool : poolConfig.GetAdjacentPools()) {
                basic.AdjacentPools.push_back(pool);
            }
            if (poolConfig.HasForcedForeignSlots()) {
                basic.ForcedForeignSlotCount = poolConfig.GetForcedForeignSlots();
            }
            cpuManager.Basic.emplace_back(std::move(basic));

            break;
        }

        case TExecutorConfig::IO: {
            Y_ABORT_UNLESS(!placementGroup);
            NActors::TIOExecutorPoolConfig io;
            io.PoolId = poolId;
            io.PoolName = poolName;
            io.Threads = poolConfig.GetThreads();
            if (!poolConfig.HasAffinity() && defaultAffinity) {
                Y_ABORT_UNLESS(defaultAffinity->CpuCount(),
                    "NUMA placement groups consume all CPUs; executor pool '%s' has no CPUs left and no explicit Affinity",
                    poolName.c_str());
                io.Affinity = *defaultAffinity;
            } else {
                io.Affinity = ParseAffinity(poolConfig.GetAffinity());
            }
            io.UseRingQueue = systemConfig.HasUseRingQueue() && systemConfig.GetUseRingQueue();
            cpuManager.IO.emplace_back(std::move(io));
            break;
        }

        default:
            Y_ABORT();
    }
}

}  // anonymous namespace

TVector<ui32> GetStoragePoolIds(const NKikimrConfig::TActorSystemConfig& systemConfig) {
    TVector<ui32> storagePoolIds;
    ui32 poolId = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        if (poolConfig.GetType() != TExecutorConfig::NUMA) {
            ++poolId;
            continue;
        }

        const ui32 placementGroups = poolConfig.GetPlacementGroups();
        Y_ABORT_UNLESS(placementGroups, "NUMA executor must have non-zero placement group count");
        for (ui32 group = 0; group < placementGroups; ++group) {
            storagePoolIds.push_back(poolId++);
        }
    }
    return storagePoolIds;
}

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters) {
    cpuManager.PingInfoByPool.resize(GetExecutorPoolCount(systemConfig));

    std::optional<TCpuTopology> cpuTopology;
    std::optional<TCpuMask> remainingCpus;
    TCpuMask usedPlacementCpus;
    ui32 placementGroupOffset = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        if (poolConfig.GetType() != TExecutorConfig::NUMA) {
            continue;
        }

        const ui32 placementGroups = poolConfig.GetPlacementGroups();
        Y_ABORT_UNLESS(placementGroups, "NUMA executor must have non-zero placement group count");
        Y_ABORT_UNLESS(!poolConfig.HasAffinity(), "NUMA executor must not define Affinity together with PlacementGroups");

        if (!cpuTopology) {
            auto parsedCpuTopology = ParseCpuTopology();
            Y_ABORT_UNLESS(parsedCpuTopology, "Failed to parse CPU topology for NUMA placement groups: %s", parsedCpuTopology.error().c_str());
            cpuTopology.emplace(std::move(*parsedCpuTopology));
        }

        Y_ABORT_UNLESS(placementGroupOffset + placementGroups <= cpuTopology->PlacementGroups.size(),
            "NUMA executors requested %" PRIu32 " placement groups, but CPU topology has only %zu placement groups",
            placementGroupOffset + placementGroups, cpuTopology->PlacementGroups.size());

        for (ui32 group = 0; group < placementGroups; ++group) {
            usedPlacementCpus = usedPlacementCpus | cpuTopology->PlacementGroups[placementGroupOffset + group].Cpus;
        }
        placementGroupOffset += placementGroups;
    }
    if (cpuTopology) {
        remainingCpus = cpuTopology->AllCpus - usedPlacementCpus;
    }

    ui32 poolId = 0;
    placementGroupOffset = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        if (poolConfig.GetType() != TExecutorConfig::NUMA) {
            const TString poolName = poolConfig.GetName();
            AddExecutorPool(cpuManager, poolConfig, systemConfig, poolId, poolName, counters,
                std::nullopt, remainingCpus);
            ++poolId;
            continue;
        }

        Y_ABORT_UNLESS(cpuTopology);

        const ui32 placementGroups = poolConfig.GetPlacementGroups();
        for (ui32 group = 0; group < placementGroups; ++group) {
            const TString poolName = GetStoragePoolName(poolConfig, group, placementGroups);
            AddExecutorPool(cpuManager, poolConfig, systemConfig, poolId, poolName, counters,
                cpuTopology->PlacementGroups[placementGroupOffset + group]);
            ++poolId;
        }
        placementGroupOffset += placementGroups;
    }
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
