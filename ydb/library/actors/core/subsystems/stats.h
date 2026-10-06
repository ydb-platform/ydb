#pragma once


#include "defs.h"

#include <ydb/library/actors/core/harmonizer/harmonizer_stats.h>
#include <ydb/library/actors/core/mon_stats.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/vector.h>

#include <memory>
#include <vector>


namespace NActors {
    class TActorSystem;
    class TCpuManager;

    class TActorSystemStatsSubSystem : public ISubSystem {
    public:
        virtual void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats,
            TVector<TExecutorThreadStats>& statsCopy) const = 0;
        virtual void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats,
            TVector<TExecutorThreadStats>& statsCopy,
            TVector<TExecutorThreadStats>& sharedStats) const = 0;
        virtual void GetExecutorPoolState(i16 poolId, TExecutorPoolState& state) const = 0;
        virtual void GetExecutorPoolStates(std::vector<TExecutorPoolState>& states) const = 0;
        virtual void GetHarmonizerStats(THarmonizerStats& stats) const = 0;
    };

    std::unique_ptr<TActorSystemStatsSubSystem> MakeActorSystemStatsSubSystem(TCpuManager *cpuManager);

    const TActorSystemStatsSubSystem& GetActorSystemStats(const TActorSystem& actorSystem);
    const TActorSystemStatsSubSystem& GetActorSystemStats();

} // namespace NActors
