#pragma once

#include "defs.h"
#include "config.h"

namespace NActors {
    class IExecutorPool;

    struct TPoolHarmonizedStats {
        ui64 IncreasingThreadsByNeedyState = 0;
        ui64 DecreasingThreadsByStarvedState = 0;
        ui64 DecreasingThreadsByHoggishState = 0;
        i16 PotentialMaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;
    };

    // Pool cpu harmonizer
    class IHarmonizer {
    public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo = nullptr) = 0;
        virtual void Enable(bool enable) = 0;
        virtual TPoolHarmonizedStats GetPoolStats(i16 poolId) const = 0;
    };

    IHarmonizer* MakeHarmonizer(ui64 ts);
}
