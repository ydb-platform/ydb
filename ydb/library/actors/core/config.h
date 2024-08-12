#pragma once

#include "defs.h"
#include <ydb/library/actors/util/cpumask.h>
#include <ydb/library/actors/util/datetime.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors {

    struct TBasicExecutorPoolConfig {
        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX = TDuration::MilliSeconds(10);
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = 100;

        ui32 PoolId = 0;
        TString PoolName;
        ui32 Threads = 1;
        ui64 SpinThreshold = 100;
        TCpuMask Affinity; // Executor thread affinity
        TDuration TimePerMailbox = DEFAULT_TIME_PER_MAILBOX;
        ui32 EventsPerMailbox = DEFAULT_EVENTS_PER_MAILBOX;
        int RealtimePriority = 0;
        i16 MinThreadCount = 0;
        i16 MaxThreadCount = 0;
        i16 DefaultThreadCount = 0;
        i16 Priority = 0;
        i16 SharedExecutorsCount = 0;
        i16 SoftProcessingDurationTs = 0;
        EASProfile ActorSystemProfile = EASProfile::Default;
        bool HasSharedThread = false;
    };

    struct TSharedExecutorPoolConfig {
        ui32 Threads = 1;
        ui64 SpinThreshold = 100;
        TCpuMask Affinity; // Executor thread affinity
        TDuration TimePerMailbox = TBasicExecutorPoolConfig::DEFAULT_TIME_PER_MAILBOX;
        ui32 EventsPerMailbox = TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX;
        i16 SoftProcessingDurationTs = Us2Ts(500);
    };

    struct TIOExecutorPoolConfig {
        ui32 PoolId = 0;
        TString PoolName;
        ui32 Threads = 1;
        TCpuMask Affinity; // Executor thread affinity
    };

    struct TSelfPingInfo {
        NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounter;
        NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounterWithSmallWindow;
        ui32 MaxAvgPingUs;
    };

    struct TExecutorPoolJailConfig {
        ui32 MaxThreadsInJailCore = 0;
        TCpuMask JailAffinity;
    };

    struct TCpuManagerConfig {
        TVector<TBasicExecutorPoolConfig> Basic;
        TVector<TIOExecutorPoolConfig> IO;
        TVector<TSelfPingInfo> PingInfoByPool;
        TSharedExecutorPoolConfig Shared;
        std::optional<TExecutorPoolJailConfig> Jail;

        ui32 GetExecutorsCount() const {
            return Basic.size() + IO.size();
        }

        TString GetPoolName(ui32 poolId) const {
            for (const auto& p : Basic) {
                if (p.PoolId == poolId) {
                    return p.PoolName;
                }
            }
            for (const auto& p : IO) {
                if (p.PoolId == poolId) {
                    return p.PoolName;
                }
            }
            Y_ABORT("undefined pool id: %" PRIu32, (ui32)poolId);
        }

        std::optional<ui32> GetThreadsOptional(ui32 poolId) const {
            for (const auto& p : Basic) {
                if (p.PoolId == poolId) {
                    return p.DefaultThreadCount;
                }
            }
            for (const auto& p : IO) {
                if (p.PoolId == poolId) {
                    return p.Threads;
                }
            }
            return {};
        }

        ui32 GetThreads(ui32 poolId) const {
            auto result = GetThreadsOptional(poolId);
            Y_ABORT_UNLESS(result, "undefined pool id: %" PRIu32, (ui32)poolId);
            return *result;
        }
    };

    struct TSchedulerConfig {
        TSchedulerConfig(
                ui64 resolution = 1024,
                ui64 spinThreshold = 100,
                ui64 progress = 10000,
                bool useSchedulerActor = false)
            : ResolutionMicroseconds(resolution)
            , SpinThreshold(spinThreshold)
            , ProgressThreshold(progress)
            , UseSchedulerActor(useSchedulerActor)
        {}

        ui64 ResolutionMicroseconds = 1024;
        ui64 SpinThreshold = 100;
        ui64 ProgressThreshold = 10000;
        bool UseSchedulerActor = false; // False is default because tests use scheduler thread
        ui64 RelaxedSendPaceEventsPerSecond = 200000;
        ui64 RelaxedSendPaceEventsPerCycle = RelaxedSendPaceEventsPerSecond * ResolutionMicroseconds / 1000000;
        // For resolution >= 250000 microseconds threshold is SendPace
        // For resolution <= 250 microseconds threshold is 20 * SendPace
        ui64 RelaxedSendThresholdEventsPerSecond = RelaxedSendPaceEventsPerSecond *
            (20 - ((20 - 1) * ClampVal(ResolutionMicroseconds, ui64(250), ui64(250000)) - 250) / (250000 - 250));
        ui64 RelaxedSendThresholdEventsPerCycle = RelaxedSendThresholdEventsPerSecond * ResolutionMicroseconds / 1000000;

        // Optional subsection for scheduler counters (usually subsystem=utils)
        NMonitoring::TDynamicCounterPtr MonCounters = nullptr;
    };

    struct TCpuAllocation {
        struct TPoolAllocation {
            TPoolId PoolId;
            TPoolWeight Weight;

            TPoolAllocation(TPoolId poolId = 0, TPoolWeight weight = 0)
                : PoolId(poolId)
                , Weight(weight)
            {}
        };

        TCpuId CpuId;
        TVector<TPoolAllocation> AllowedPools;

        TPoolsMask GetPoolsMask() const {
            TPoolsMask mask = 0;
            for (const auto& pa : AllowedPools) {
                if (pa.PoolId < MaxPools) {
                    mask &= (1ull << pa.PoolId);
                }
            }
            return mask;
        }

        bool HasPool(TPoolId pool) const {
            for (const auto& pa : AllowedPools) {
                if (pa.PoolId == pool) {
                    return true;
                }
            }
            return false;
        }
    };

}
