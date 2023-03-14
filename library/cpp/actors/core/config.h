#pragma once

#include "defs.h"
#include <library/cpp/actors/util/cpumask.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors {

    struct TBalancingConfig {
        // Default cpu count (used during overload). Zero value disables this pool balancing
        // 1) Sum of `Cpus` on all pools cannot be changed without restart
        //    (changing cpu mode between Shared and Assigned is not implemented yet)
        // 2) This sum must be equal to TUnitedWorkersConfig::CpuCount,
        //    otherwise `CpuCount - SUM(Cpus)` cpus will be in Shared mode (i.e. actorsystem 2.0)
        ui32 Cpus = 0;

        ui32 MinCpus = 0; // Lower balancing bound, should be at least 1, and not greater than `Cpus`
        ui32 MaxCpus = 0; // Higher balancing bound, should be not lower than `Cpus`
        ui8 Priority = 0; // Priority of pool to obtain cpu due to balancing (higher is better)
        ui64 ToleratedLatencyUs = 0; // p100-latency threshold indicating that more cpus are required by pool
    };

    struct TBalancerConfig {
        ui64 PeriodUs = 15000000; // Time between balancer steps
    };

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
        ui32 MaxActivityType = 5;
        i16 MinThreadCount = 0;
        i16 MaxThreadCount = 0;
        i16 DefaultThreadCount = 0;
        i16 Priority = 0;
    };

    struct TIOExecutorPoolConfig {
        ui32 PoolId = 0;
        TString PoolName;
        ui32 Threads = 1;
        TCpuMask Affinity; // Executor thread affinity
        ui32 MaxActivityType = 5;
    };

    struct TUnitedExecutorPoolConfig {
        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX = TDuration::MilliSeconds(10);
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = 100;

        ui32 PoolId = 0;
        TString PoolName;

        // Resource sharing
        ui32 Concurrency = 0; // Limits simultaneously running mailboxes count if set to non-zero value (do not set if Balancing.Cpus != 0)
        TPoolWeight Weight = 0; // Weight in fair cpu-local pool scheduler
        TCpuMask Allowed; // Allowed CPUs for workers to run this pool on (ignored if balancer works, i.e. actorsystem 1.5)

        // Single mailbox execution limits
        TDuration TimePerMailbox = DEFAULT_TIME_PER_MAILBOX;
        ui32 EventsPerMailbox = DEFAULT_EVENTS_PER_MAILBOX;

        // Introspection
        ui32 MaxActivityType = 5;

        // Long-term balancing
        TBalancingConfig Balancing;
    };

    struct TUnitedWorkersConfig {
        ui32 CpuCount = 0; // Total CPUs running united workers (i.e. TBasicExecutorPoolConfig::Threads analog); set to zero to disable united workers
        ui64 SpinThresholdUs = 100; // Limit for active spinning in case all pools became idle
        ui64 PoolLimitUs = 500; // Soft limit on pool execution
        ui64 EventLimitUs = 100; // Hard limit on last event execution exceeding pool limit
        ui64 LimitPrecisionUs = 100; // Maximum delay of timer on limit excess (delay needed to avoid settimer syscall on every pool switch)
        ui64 FastWorkerPriority = 10; // Real-time priority of workers not exceeding hard limits
        ui64 IdleWorkerPriority = 20; // Real-time priority of standby workers waiting for hard preemption on timers (should be greater than FastWorkerPriority)
        TCpuMask Allowed; // Allowed CPUs for workers to run on (every worker has affinity for exactly one cpu)
        bool NoRealtime = false; // For environments w/o permissions for RT-threads
        bool NoAffinity = false; // For environments w/o permissions for cpu affinity
        TBalancerConfig Balancer;
    };

    struct TSelfPingInfo {
        NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounter;
        NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounterWithSmallWindow;
        ui32 MaxAvgPingUs;
    };

    struct TCpuManagerConfig {
        TUnitedWorkersConfig UnitedWorkers;
        TVector<TBasicExecutorPoolConfig> Basic;
        TVector<TIOExecutorPoolConfig> IO;
        TVector<TUnitedExecutorPoolConfig> United;
        TVector<TSelfPingInfo> PingInfoByPool;

        ui32 GetExecutorsCount() const {
            return Basic.size() + IO.size() + United.size();
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
            for (const auto& p : United) {
                if (p.PoolId == poolId) {
                    return p.PoolName;
                }
            }
            Y_FAIL("undefined pool id: %" PRIu32, (ui32)poolId);
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
            for (const auto& p : United) {
                if (p.PoolId == poolId) {
                    return p.Concurrency ? p.Concurrency : UnitedWorkers.CpuCount;
                }
            }
            return {};
        }

        ui32 GetThreads(ui32 poolId) const {
            auto result = GetThreadsOptional(poolId);
            Y_VERIFY(result, "undefined pool id: %" PRIu32, (ui32)poolId);
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

    struct TCpuAllocationConfig {
        TVector<TCpuAllocation> Items;

        TCpuAllocationConfig(const TCpuMask& available, const TCpuManagerConfig& cfg) {
            for (const TUnitedExecutorPoolConfig& pool : cfg.United) {
                Y_VERIFY(pool.PoolId < MaxPools, "wrong PoolId of united executor pool: %s(%d)",
                    pool.PoolName.c_str(), (pool.PoolId));
            }
            ui32 allocated[MaxPools] = {0};
            for (TCpuId cpu = 0; cpu < available.Size() && Items.size() < cfg.UnitedWorkers.CpuCount; cpu++) {
                if (available.IsSet(cpu)) {
                    TCpuAllocation item;
                    item.CpuId = cpu;
                    for (const TUnitedExecutorPoolConfig& pool : cfg.United) {
                        if (cfg.UnitedWorkers.Allowed.IsEmpty() || cfg.UnitedWorkers.Allowed.IsSet(cpu)) {
                            if (pool.Allowed.IsEmpty() || pool.Allowed.IsSet(cpu)) {
                                item.AllowedPools.emplace_back(pool.PoolId, pool.Weight);
                                allocated[pool.PoolId]++;
                            }
                        }
                    }
                    if (!item.AllowedPools.empty()) {
                        Items.push_back(item);
                    }
                }
            }
            for (const TUnitedExecutorPoolConfig& pool : cfg.United) {
                Y_VERIFY(allocated[pool.PoolId] > 0, "unable to allocate cpu for united executor pool: %s(%d)",
                    pool.PoolName.c_str(), (pool.PoolId));
            }
        }

        operator bool() const {
            return !Items.empty();
        }
    };

}
