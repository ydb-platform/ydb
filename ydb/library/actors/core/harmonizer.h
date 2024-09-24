#pragma once

#include "defs.h"
#include "executor_pool_shared.h"

namespace NActors {
    class IExecutorPool;
    class TSharedExecutorPool;
    struct TSelfPingInfo;

    template <typename T>
    struct TWaitingStats;

    struct TPoolHarmonizerStats {
        ui64 IncreasingThreadsByNeedyState = 0;
        ui64 IncreasingThreadsByExchange = 0;
        ui64 DecreasingThreadsByStarvedState = 0;
        ui64 DecreasingThreadsByHoggishState = 0;
        ui64 DecreasingThreadsByExchange = 0;
        float MaxElapsedCpu = 0.0;
        float MinElapsedCpu = 0.0;
        float AvgElapsedCpu = 0.0;
        float MaxCpu = 0.0;
        float MinCpu = 0.0;
        i16 PotentialMaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;
    
        TString ToString() const;
    };

    struct THarmonizerStats {
        i64 MaxElapsedCpu = 0.0;
        i64 MinElapsedCpu = 0.0;
        i64 MaxCpu = 0.0;
        i64 MinCpu = 0.0;

        double AvgAwakeningTimeUs = 0;
        double AvgWakingUpTimeUs = 0;

        TString ToString() const;
    };

    // Pool cpu harmonizer
    class IHarmonizer {
    public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo = nullptr) = 0;
        virtual void Enable(bool enable) = 0;
        virtual TPoolHarmonizerStats GetPoolStats(i16 poolId) const = 0;
        virtual THarmonizerStats GetStats() const = 0;
        virtual void SetSharedPool(TSharedExecutorPool* pool) = 0;
    };

    IHarmonizer* MakeHarmonizer(ui64 ts);
}
