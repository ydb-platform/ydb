#include "stats.h"


#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/cpu_manager.h>
#include <ydb/library/actors/core/harmonizer/harmonizer.h>


namespace NActors {
    namespace {
        class TActorSystemStatsSubSystemImpl final : public TActorSystemStatsSubSystem {
        public:
            explicit TActorSystemStatsSubSystemImpl(TCpuManager* cpuManager)
                : CpuManager(cpuManager)
            {
            }

            void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats,
                    TVector<TExecutorThreadStats>& statsCopy) const override {
                CpuManager->GetPoolStats(poolId, poolStats, statsCopy);
            }

            void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats,
                    TVector<TExecutorThreadStats>& statsCopy,
                    TVector<TExecutorThreadStats>& sharedStats) const override {
                CpuManager->GetPoolStats(poolId, poolStats, statsCopy, sharedStats);
            }

            void GetExecutorPoolState(i16 poolId, TExecutorPoolState& state) const override {
                CpuManager->GetExecutorPoolState(poolId, state);
            }

            void GetExecutorPoolStates(std::vector<TExecutorPoolState>& states) const override {
                CpuManager->GetExecutorPoolStates(states);
            }

            void GetHarmonizerStats(THarmonizerStats& stats) const override {
                CpuManager->GetHarmonizerStats(stats);
            }

        private:
            TCpuManager* const CpuManager;
        };
    }


    std::unique_ptr<TActorSystemStatsSubSystem> MakeActorSystemStatsSubSystem(TCpuManager *cpuManager) {
        return std::make_unique<TActorSystemStatsSubSystemImpl>(cpuManager);
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
