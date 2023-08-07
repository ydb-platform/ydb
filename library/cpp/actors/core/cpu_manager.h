#pragma once

#include "harmonizer.h"
#include "executor_pool.h"
#include "executor_pool_united_workers.h"
#include "balancer.h"

namespace NActors {
    struct TActorSystemSetup;

    class TCpuManager : public TNonCopyable {
        const ui32 ExecutorPoolCount;
        TArrayHolder<TAutoPtr<IExecutorPool>> Executors;
        THolder<TUnitedWorkers> UnitedWorkers;
        THolder<IBalancer> Balancer;
        THolder<IHarmonizer> Harmonizer;
        TCpuManagerConfig Config;

    public:
        explicit TCpuManager(THolder<TActorSystemSetup>& setup);

        void Setup();
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
