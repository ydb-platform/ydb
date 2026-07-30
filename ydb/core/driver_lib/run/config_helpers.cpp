#include "config_helpers.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/cpu_topology.h>

#include <util/generic/hash_set.h>
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

TString GetPlacementExecutorPoolName(const TExecutorConfig& poolConfig, ui32 groupIndex, ui32 placementGroups) {
    const TString& baseName = poolConfig.GetName();
    return placementGroups == 1 || baseName.empty()
        ? baseName
        : TStringBuilder() << baseName << groupIndex;
}

ui32 GetExpandedExecutorPoolCount(const TExecutorConfig& poolConfig) {
    if (poolConfig.GetType() != TExecutorConfig::PLACEMENT) {
        return 1;
    }

    const ui32 placementGroups = poolConfig.GetPlacementGroups();
    Y_ABORT_UNLESS(placementGroups, "PLACEMENT executor must have non-zero placement group count");
    return placementGroups;
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

[[nodiscard]]
NActors::TIOExecutorPoolConfig BuildIoExecutorPoolConfig(
    const TString poolName,
    const ui32 poolId,
    const TExecutorConfig& poolConfig,
    const NKikimrConfig::TActorSystemConfig& systemConfig,
    const std::optional<TCpuMask>& defaultAffinity)
{
    NActors::TIOExecutorPoolConfig io;
    io.PoolId = poolId;
    io.PoolName = poolName;
    io.Threads = poolConfig.GetThreads();
    if (!poolConfig.HasAffinity() && defaultAffinity) {
        Y_ABORT_UNLESS(defaultAffinity->CpuCount(),
            "PLACEMENT executors consume all CPUs; executor pool '%s' has no CPUs left and no explicit Affinity",
            poolName.c_str());
        io.Affinity = *defaultAffinity;
    } else {
        io.Affinity = ParseAffinity(poolConfig.GetAffinity());
    }
    io.UseRingQueue = systemConfig.HasUseRingQueue() && systemConfig.GetUseRingQueue();
    return io;
}

[[nodiscard]]
NActors::TBasicExecutorPoolConfig BuildBasicExecutorPoolConfig(
    const TString poolName,
    const ui32 poolId,
    const TExecutorConfig& poolConfig,
    const NKikimrConfig::TActorSystemConfig& systemConfig,
    NActors::TCpuManagerConfig& cpuManager,
    NMonitoring::TDynamicCounterPtr counters,
    const TCpuMask& affinity,
    ui32 threads,
    ui32 minThreadCount,
    ui32 maxThreadCount,
    ui32 defaultThreadCount)
{
    NActors::TBasicExecutorPoolConfig basic;

    basic.PoolId = poolId;
    basic.PoolName = poolName;
    basic.UseRingQueue = systemConfig.HasUseRingQueue() && systemConfig.GetUseRingQueue();

    basic.Affinity = affinity;
    basic.Threads = threads;
    basic.MinThreadCount = minThreadCount;
    basic.MaxThreadCount = maxThreadCount;
    basic.DefaultThreadCount = defaultThreadCount;

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
    const ui32 harmonizerNeedyCpuWindowSeconds = poolConfig.GetHarmonizerNeedyCpuWindowSeconds();
    Y_ABORT_UNLESS(harmonizerNeedyCpuWindowSeconds >= 1 && harmonizerNeedyCpuWindowSeconds <= 32,
        "HarmonizerNeedyCpuWindowSeconds must be in range [1, 32], got %" PRIu32,
        harmonizerNeedyCpuWindowSeconds);
    basic.HarmonizerNeedyCpuWindowSeconds = static_cast<ui8>(harmonizerNeedyCpuWindowSeconds);
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
    if (poolConfig.HasAllThreadsAreShared()) {
        basic.AllThreadsAreShared = poolConfig.GetAllThreadsAreShared();
    }

    return basic;
}

ui32 GetExecutorPoolCount(const NKikimrConfig::TActorSystemConfig& systemConfig) {
    ui32 poolCount = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        poolCount += GetExpandedExecutorPoolCount(poolConfig);
    }
    return poolCount;
}

}  // anonymous namespace

ui32 GetExpandedExecutorPoolId(const NKikimrConfig::TActorSystemConfig& systemConfig, ui32 executorId) {
    ui32 poolId = 0;
    for (ui32 i = 0; i < static_cast<ui32>(systemConfig.ExecutorSize()); ++i) {
        if (i == executorId) {
            return poolId;
        }
        poolId += GetExpandedExecutorPoolCount(systemConfig.GetExecutor(i));
    }

    Y_ABORT("ExecutorId# %" PRIu32 " is out of range; executor count# %d",
        executorId, systemConfig.ExecutorSize());
}

TVector<ui32> GetBlobStorageExecutorPoolIds(const NKikimrConfig::TActorSystemConfig& systemConfig) {
    TVector<ui32> executorPoolIds;
    THashSet<ui32> seenExecutorIds;
    for (const ui32 executorId : systemConfig.GetBlobStorageExecutor()) {
        Y_ABORT_UNLESS(executorId < static_cast<ui32>(systemConfig.ExecutorSize()),
            "BlobStorageExecutor id %" PRIu32 " is out of range; executor count is %d",
            executorId, systemConfig.ExecutorSize());
        Y_ABORT_UNLESS(seenExecutorIds.insert(executorId).second,
            "BlobStorageExecutor id %" PRIu32 " is specified more than once", executorId);

        const auto& poolConfig = systemConfig.GetExecutor(executorId);
        Y_ABORT_UNLESS(poolConfig.GetType() == TExecutorConfig::PLACEMENT,
            "BlobStorageExecutor id %" PRIu32 " must reference a PLACEMENT executor", executorId);

        const ui32 firstPoolId = GetExpandedExecutorPoolId(systemConfig, executorId);
        const ui32 poolCount = GetExpandedExecutorPoolCount(poolConfig);
        for (ui32 offset = 0; offset < poolCount; ++offset) {
            executorPoolIds.push_back(firstPoolId + offset);
        }
    }
    return executorPoolIds;
}

namespace {

void AddExecutorPoolsImpl(NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters,
        const TCpuTopology* suppliedCpuTopology) {
    cpuManager.PingInfoByPool.resize(GetExecutorPoolCount(systemConfig));

    std::optional<TCpuTopology> parsedCpuTopology;
    const TCpuTopology* cpuTopology = suppliedCpuTopology;
    std::optional<TCpuMask> remainingCpus;
    TCpuMask usedPlacementCpus;
    ui32 placementGroupOffset = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        if (poolConfig.GetType() != TExecutorConfig::PLACEMENT) {
            continue;
        }

        const ui32 placementGroups = poolConfig.GetPlacementGroups();
        Y_ABORT_UNLESS(placementGroups, "PLACEMENT executor must have non-zero placement group count");
        Y_ABORT_UNLESS(!poolConfig.HasAffinity(), "PLACEMENT executor must not define Affinity together with PlacementGroups");

        if (!cpuTopology) {
            auto result = ParseCpuTopology();
            Y_ABORT_UNLESS(result, "Failed to parse CPU topology for PLACEMENT executors: %s", result.error().c_str());
            parsedCpuTopology.emplace(std::move(*result));
            cpuTopology = &*parsedCpuTopology;
        }

        Y_ABORT_UNLESS(placementGroupOffset + placementGroups <= cpuTopology->PlacementGroups.size(),
            "PLACEMENT executors requested %" PRIu32 " placement groups, but CPU topology has only %zu placement groups",
            placementGroupOffset + placementGroups, cpuTopology->PlacementGroups.size());

        for (ui32 group = 0; group < placementGroups; ++group) {
            usedPlacementCpus = usedPlacementCpus | cpuTopology->PlacementGroups[placementGroupOffset + group].Cpus;
        }
        placementGroupOffset += placementGroups;
    }
    if (placementGroupOffset) {
        remainingCpus = cpuTopology->AllCpus - usedPlacementCpus;
    }

    // Counters are grouped by pool name, so expanded PLACEMENT pool names must not collide
    // with each other or with other pools. Only enforced for configs that use PLACEMENT
    // executors to avoid breaking pre-existing configs.
    const bool hasPlacementExecutors = placementGroupOffset != 0;
    THashSet<TString> poolNames;
    auto checkPoolName = [&](const TString& poolName) {
        if (hasPlacementExecutors) {
            Y_ABORT_UNLESS(poolNames.insert(poolName).second,
                "duplicate executor pool name '%s'; pool names must be unique when PLACEMENT executors are used",
                poolName.c_str());
        }
    };

    ui32 poolId = 0;
    placementGroupOffset = 0;
    for (const auto& poolConfig : systemConfig.GetExecutor()) {
        Y_ABORT_UNLESS(!poolConfig.HasHarmonizerNeedyCpuWindowSeconds()
            || poolConfig.GetType() == TExecutorConfig::BASIC,
            "HarmonizerNeedyCpuWindowSeconds is supported only for BASIC executors");

        switch (poolConfig.GetType()) {
            case TExecutorConfig::BASIC: {
                const TString poolName = poolConfig.GetName();
                checkPoolName(poolName);
                ui32 threads = Max(poolConfig.GetThreads(), poolConfig.GetMaxThreads());
                ui32 minThreadCount = poolConfig.GetMinThreads();
                ui32 maxThreadCount = poolConfig.GetMaxThreads();
                ui32 defaultThreadCount = poolConfig.GetThreads();

                TCpuMask affinity;
                if (!poolConfig.HasAffinity() && remainingCpus) {
                    Y_ABORT_UNLESS(remainingCpus->CpuCount(),
                        "PLACEMENT executors consume all CPUs; executor pool '%s' has no CPUs left and no explicit Affinity",
                        poolName.c_str());
                    affinity = *remainingCpus;
                } else {
                    affinity = ParseAffinity(poolConfig.GetAffinity());
                }

                cpuManager.Basic.emplace_back(BuildBasicExecutorPoolConfig(poolName, poolId, poolConfig, systemConfig, cpuManager,
                    counters, affinity, threads, minThreadCount, maxThreadCount, defaultThreadCount));

                ++poolId;
                break;
            }

            case TExecutorConfig::IO: {
                const TString poolName = poolConfig.GetName();
                checkPoolName(poolName);
                cpuManager.IO.emplace_back(BuildIoExecutorPoolConfig(poolName, poolId, poolConfig, systemConfig, remainingCpus));
                ++poolId;
                break;
            }

            case TExecutorConfig::PLACEMENT: {
                Y_ABORT_UNLESS(cpuTopology);
                const ui32 placementGroups = poolConfig.GetPlacementGroups();
                for (ui32 group = 0; group < placementGroups; ++group) {
                    const TString poolName = GetPlacementExecutorPoolName(poolConfig, group, placementGroups);
                    checkPoolName(poolName);
                    const TCpuTopologyGroup& placementGroup = cpuTopology->PlacementGroups[placementGroupOffset + group];
                    Y_ABORT_UNLESS(!poolConfig.HasAffinity(), "PLACEMENT executor must not define Affinity");
                    Y_ABORT_UNLESS(placementGroup.Cpus.CpuCount(), "PLACEMENT executor placement group %" PRIu32 " has no CPUs", placementGroup.Id);
                    TCpuMask affinity = placementGroup.Cpus;
                    ui32 threads = poolConfig.HasPlacementGroupThreads()
                        ? poolConfig.GetPlacementGroupThreads()
                        : affinity.CpuCount();
                    Y_ABORT_UNLESS(threads, "PLACEMENT executor placement group %" PRIu32 " has zero threads", placementGroup.Id);
                    ui32 minThreadCount = threads;
                    ui32 maxThreadCount = threads;
                    ui32 defaultThreadCount = threads;

                    cpuManager.Basic.emplace_back(BuildBasicExecutorPoolConfig(poolName, poolId, poolConfig, systemConfig, cpuManager,
                        counters, affinity, threads, minThreadCount, maxThreadCount, defaultThreadCount));

                    ++poolId;
                }
                placementGroupOffset += placementGroups;
                break;
            }

            default:
                Y_ABORT();
        }
    }
}

} // anonymous namespace

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters) {
    AddExecutorPoolsImpl(cpuManager, systemConfig, std::move(counters), nullptr);
}

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager,
        const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters,
        const TCpuTopology& cpuTopology) {
    AddExecutorPoolsImpl(cpuManager, systemConfig, std::move(counters), &cpuTopology);
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
