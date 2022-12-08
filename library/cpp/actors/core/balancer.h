#pragma once

#include "defs.h"
#include "config.h"
#include "cpu_state.h"

namespace NActors {
    // Per-pool statistics used by balancer
    struct TBalancerStats {
        ui64 Ts = 0; // Measurement timestamp
        ui64 CpuUs = 0; // Total cpu microseconds consumed by pool on all cpus since start
        ui64 IdleUs = ui64(-1); // Total cpu microseconds in spinning or waiting on futex
        ui64 WorstActivationTimeUs = 0;
        ui64 ExpectedLatencyIncreaseUs = 0;
    };

    // Pool cpu balancer
    struct IBalancer {
        virtual ~IBalancer() {}
        virtual bool AddCpu(const TCpuAllocation& cpuAlloc, TCpuState* cpu) = 0;
        virtual bool TryLock(ui64 ts) = 0;
        virtual void SetPoolStats(TPoolId pool, const TBalancerStats& stats) = 0;
        virtual void Balance() = 0;
        virtual void Unlock() = 0;
        virtual ui64 GetPeriodUs() = 0;
        // TODO: add method for reconfiguration on fly
    };

    IBalancer* MakeBalancer(const TBalancerConfig& config, const TVector<TUnitedExecutorPoolConfig>& unitedPools, ui64 ts);
}
