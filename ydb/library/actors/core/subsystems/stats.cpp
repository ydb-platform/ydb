#include "stats.h"


#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/cpu_manager.h>
#include <ydb/library/actors/core/harmonizer/harmonizer.h>


namespace NActors {

    TActorSystemStatsSubSystem::TActorSystemStatsSubSystem(TCpuManager *cpuManager)
        : CpuManager(cpuManager)
    {
    }

    void TActorSystemStatsSubSystem::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        CpuManager->GetPoolStats(poolId, poolStats, statsCopy);
    }

    void TActorSystemStatsSubSystem::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy,
            TVector<TExecutorThreadStats>& sharedStats) const {
        CpuManager->GetPoolStats(poolId, poolStats, statsCopy, sharedStats);
    }

    void TActorSystemStatsSubSystem::GetExecutorPoolState(i16 poolId, TExecutorPoolState& state) const {
        CpuManager->GetExecutorPoolState(poolId, state);
    }

    void TActorSystemStatsSubSystem::GetExecutorPoolStates(std::vector<TExecutorPoolState>& states) const {
        CpuManager->GetExecutorPoolStates(states);
    }

    void TActorSystemStatsSubSystem::GetHarmonizerStats(THarmonizerStats &stats) const {
        return CpuManager->GetHarmonizerStats(stats);
    }


    std::unique_ptr<TActorSystemStatsSubSystem> MakeActorSystemStatsSubSystem(TCpuManager *cpuManager) {
        return std::make_unique<TActorSystemStatsSubSystem>(cpuManager);
    }

    const TActorSystemStatsSubSystem& GetActorSystemStats(const TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TActorSystemStatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "actor system stats subsystem is not registered");
        return *subSystem;
    }

    const TActorSystemStatsSubSystem& GetActorSystemStats() {
        TActorSystem *actorSystem = TActivationContext::ActorSystem();
        auto* subSystem = actorSystem->GetSubSystem<TActorSystemStatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "actor system stats subsystem is not registered");
        return *subSystem;
    }

} // namespace NActors
