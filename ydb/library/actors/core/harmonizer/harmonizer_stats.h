#pragma once

#include "defs.h"

namespace NActors {

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

} // namespace NActors
