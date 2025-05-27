#pragma once

#include "config.h"
#include "executor_pool.h"
#include "mon_stats.h"
#include <ydb/library/actors/core/harmonizer/harmonizer.h>
#include <memory>

namespace NActors {
    struct TActorSystemSetup;
    class TExecutorPoolJail;
    class TSharedExecutorPool;

    class TCpuManager : public TNonCopyable {
        const ui32 ExecutorPoolCount;
        TArrayHolder<TAutoPtr<IExecutorPool>> Executors;
        std::unique_ptr<IHarmonizer> Harmonizer;
        std::unique_ptr<TSharedExecutorPool> Shared;
        std::unique_ptr<TExecutorPoolJail> Jail;
        TCpuManagerConfig Config;

    public:
        explicit TCpuManager(THolder<TActorSystemSetup>& setup);
        ~TCpuManager();

        void Setup();
        void SetupShared();
        void PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem);
        void Start();
        void PrepareStop();
        void Shutdown();
        void Cleanup();

        TVector<IExecutorPool*> GetBasicExecutorPools() const;

        ui32 GetExecutorsCount() const {
            return ExecutorPoolCount;
        }

        IExecutorPool* GetExecutorPool(ui32 poolId) {
            return Executors[poolId].Get();
        }

        void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
            if (poolId < ExecutorPoolCount) {
                Executors[poolId]->GetCurrentStats(poolStats, statsCopy);
            }
        }

        void GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy, TVector<TExecutorThreadStats>& sharedStatsCopy) const;
        void GetExecutorPoolState(i16 poolId, TExecutorPoolState &state) const;
        void GetExecutorPoolStates(std::vector<TExecutorPoolState> &states) const;

        THarmonizerStats GetHarmonizerStats() const {
            if (Harmonizer) {
                return Harmonizer->GetStats();
            }
            return {};
        }

    private:
        IExecutorPool* CreateExecutorPool(ui32 poolId);
    };
}
