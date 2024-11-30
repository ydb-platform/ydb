#pragma once

#include "defs.h"

namespace NActors {
    class IExecutorPool;
    class ISharedExecutorPool;
    struct TSelfPingInfo;

    template <typename T>
    struct TWaitingStats;

    struct TPoolHarmonizerStats {
        ui64 IncreasingThreadsByNeedyState = 0;
        ui64 IncreasingThreadsByExchange = 0;
        ui64 DecreasingThreadsByStarvedState = 0;
        ui64 DecreasingThreadsByHoggishState = 0;
        ui64 DecreasingThreadsByExchange = 0;

        ui64 ReceivedHalfThreadByNeedyState = 0;
        ui64 GivenHalfThreadByOtherStarvedState = 0;
        ui64 GivenHalfThreadByHoggishState = 0;
        ui64 GivenHalfThreadByOtherNeedyState = 0;
        ui64 ReturnedHalfThreadByStarvedState = 0;
        ui64 ReturnedHalfThreadByOtherHoggishState = 0;

        float MaxCpuUs = 0.0;
        float MinCpuUs = 0.0;
        float AvgCpuUs = 0.0;
        float MaxElapsedUs = 0.0;
        float MinElapsedUs = 0.0;
        float AvgElapsedUs = 0.0;
        i16 PotentialMaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;

        TString ToString() const;
    };

    struct THarmonizerStats {
        i64 MaxCpuUs = 0.0;
        i64 MinCpuUs = 0.0;
        i64 MaxElapsedUs = 0.0;
        i64 MinElapsedUs = 0.0;

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
        virtual void SetSharedPool(ISharedExecutorPool* pool) = 0;
    };

    IHarmonizer* MakeHarmonizer(ui64 ts);
}
