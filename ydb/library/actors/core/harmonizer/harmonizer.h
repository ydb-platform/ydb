#pragma once

#include "defs.h"
#include "harmonizer_stats.h"

namespace NActors {
    class IExecutorPool;
    class ISharedPool;
    struct TSelfPingInfo;
    struct THarmonizerIterationState;

    template <typename T>
    struct TWaitingStats;

    // Pool cpu harmonizer
    class IHarmonizer {
    public:
        virtual ~IHarmonizer() {}
        virtual void Harmonize(ui64 ts) = 0;
        virtual void DeclareEmergency(ui64 ts) = 0;
        virtual void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo = nullptr, bool ignoreFullThreadQuota = false) = 0;
        virtual void Enable(bool enable) = 0;
        virtual TPoolHarmonizerStats GetPoolStats(i16 poolId) const = 0;
        virtual void GetStats(THarmonizerStats &stats) const = 0;
        virtual void SetSharedPool(ISharedPool* pool) = 0;
    };

    std::unique_ptr<IHarmonizer> MakeHarmonizer(ui64 ts);
}
