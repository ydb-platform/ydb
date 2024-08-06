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
        float MaxConsumedCpu = 0.0;
        float MinConsumedCpu = 0.0;
        float AvgConsumedCpu = 0.0;
        float MaxBookedCpu = 0.0;
        float MinBookedCpu = 0.0;
        i16 PotentialMaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;
    };

    struct THarmonizerStats {
        i64 MaxConsumedCpu = 0.0;
        i64 MinConsumedCpu = 0.0;
        i64 MaxBookedCpu = 0.0;
        i64 MinBookedCpu = 0.0;

        double AvgAwakeningTimeUs = 0;
        double AvgWakingUpTimeUs = 0;
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
