#pragma once

#include "defs.h"

namespace NActors {
    class IExecutorPool;
    class ISharedPool;
    struct TSelfPingInfo;
    struct THarmonizerIterationState;

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

        float MaxUsedCpu = 0.0;
        float MinUsedCpu = 0.0;
        float AvgUsedCpu = 0.0;
        float MaxElapsedCpu = 0.0;
        float MinElapsedCpu = 0.0;
        float AvgElapsedCpu = 0.0;
        float PotentialMaxThreadCount = 0.0;
        float SharedCpuQuota = 0.0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;

        TString ToString() const;
    };

    struct THarmonizerStats {
        float MaxUsedCpu = 0.0;
        float MinUsedCpu = 0.0;
        float MaxElapsedCpu = 0.0;
        float MinElapsedCpu = 0.0;

        float AvgAwakeningTimeUs = 0;
        float AvgWakingUpTimeUs = 0;

        float Budget = 0.0;
        float SharedFreeCpu = 0.0;

        TString ToString() const;
    };

    // Pool cpu harmonizer
    class IHarmonizer {
    public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo = nullptr, bool ignoreFullThreadQuota = false) = 0;
        virtual void Enable(bool enable) = 0;
        virtual TPoolHarmonizerStats GetPoolStats(i16 poolId) const = 0;
        virtual THarmonizerStats GetStats() const = 0;
        virtual void SetSharedPool(ISharedPool* pool) = 0;
    };

    std::unique_ptr<IHarmonizer> MakeHarmonizer(ui64 ts);
}
