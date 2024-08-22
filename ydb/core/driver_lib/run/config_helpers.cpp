#include "config_helpers.h"

#include <ydb/library/actors/util/affinity.h>


namespace NKikimr {

namespace NActorSystemConfigHelpers {

namespace {

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

}  // anonymous namespace

void AddExecutorPool(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig::TExecutor& poolConfig, const NKikimrConfig::TActorSystemConfig& systemConfig, ui32 poolId, NMonitoring::TDynamicCounterPtr counters) {
    switch (poolConfig.GetType()) {
        case NKikimrConfig::TActorSystemConfig::TExecutor::BASIC: {
            NActors::TBasicExecutorPoolConfig basic;
            basic.PoolId = poolId;
            basic.PoolName = poolConfig.GetName();
            if (poolConfig.HasMaxAvgPingDeviation() && counters) {
                auto poolGroup = counters->GetSubgroup("execpool", basic.PoolName);
                auto &poolInfo = cpuManager.PingInfoByPool[poolId];
                poolInfo.AvgPingCounter = poolGroup->GetCounter("SelfPingAvgUs", false);
                poolInfo.AvgPingCounterWithSmallWindow = poolGroup->GetCounter("SelfPingAvgUsIn1s", false);
                TDuration maxAvgPing = GetSelfPingInterval(systemConfig) + TDuration::MicroSeconds(poolConfig.GetMaxAvgPingDeviation());
                poolInfo.MaxAvgPingUs = maxAvgPing.MicroSeconds();
            }
            basic.Threads = Max(poolConfig.GetThreads(), poolConfig.GetMaxThreads());
            basic.SpinThreshold = poolConfig.GetSpinThreshold();
            basic.Affinity = ParseAffinity(poolConfig.GetAffinity());
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
            basic.MinThreadCount = poolConfig.GetMinThreads();
            basic.MaxThreadCount = poolConfig.GetMaxThreads();
            basic.DefaultThreadCount = poolConfig.GetThreads();
            basic.Priority = poolConfig.GetPriority();
            cpuManager.Basic.emplace_back(std::move(basic));
            break;
        }

        case NKikimrConfig::TActorSystemConfig::TExecutor::IO: {
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

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config) {
    const ui64 resolution = config.HasResolution() ? config.GetResolution() : 1024;
    Y_DEBUG_ABORT_UNLESS((resolution & (resolution - 1)) == 0);  // resolution must be power of 2
    const ui64 spinThreshold = config.HasSpinThreshold() ? config.GetSpinThreshold() : 0;
    const ui64 progressThreshold = config.HasProgressThreshold() ? config.GetProgressThreshold() : 10000;
    const bool useSchedulerActor = config.HasUseSchedulerActor() ? config.GetUseSchedulerActor() : false;

    return NActors::TSchedulerConfig(resolution, spinThreshold, progressThreshold, useSchedulerActor);
}

}  // namespace NActorSystemConfigHelpers

}  // namespace NKikimr
