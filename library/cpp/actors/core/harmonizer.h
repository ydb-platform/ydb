#pragma once

#include "defs.h"
#include "config.h"

namespace NActors {
    class IExecutorPool;

    struct TPoolStateFlags {
        bool IsNeedy = false;
        bool IsStarved = false;
    };

    // Pool cpu harmonizer
    class IHarmonizer {
    public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo = nullptr) = 0;
        virtual void Enable(bool enable) = 0;
        virtual TPoolStateFlags GetPoolFlags(i16 poolId) const = 0;
    };

    IHarmonizer* MakeHarmonizer(ui64 ts);
}
