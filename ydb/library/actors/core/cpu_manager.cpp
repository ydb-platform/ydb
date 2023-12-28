#include "cpu_manager.h"
#include "probes.h"

#include "executor_pool_basic.h"
#include "executor_pool_io.h"
#include "executor_pool_shared.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TCpuManager::TCpuManager(THolder<TActorSystemSetup>& setup)
        : ExecutorPoolCount(setup->GetExecutorsCount())
        , Config(setup->CpuManager)
    {
        if (setup->Executors) { // Explicit mode w/o united pools
            Executors.Reset(setup->Executors.Release());
        } else {
            Setup();
        }
    }

    void TCpuManager::Setup() {
        TAffinity available;
        available.Current();

        std::vector<i16> poolsWithSharedThreads;
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.HasSharedThread) {
                poolsWithSharedThreads.push_back(cfg.PoolId);
            }
        }

        SharedPool.Reset(new TSharedExecutorPool(Config.Shared, ExecutorPoolCount, poolsWithSharedThreads));
        auto shared = static_cast<TSharedExecutorPool*>(SharedPool.Get());

        ui64 ts = GetCycleCountFast();
        Harmonizer.Reset(MakeHarmonizer(ts, shared));

        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);

        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx].Reset(CreateExecutorPool(excIdx));
            bool hasSharedThread = shared->GetSharedThread(excIdx);
            auto *pingInfo = (excIdx < Config.PingInfoByPool.size() ? &Config.PingInfoByPool[excIdx] : nullptr);
            Harmonizer->AddPool(Executors[excIdx].Get(), pingInfo, hasSharedThread, shared);
        }
    }

    void TCpuManager::PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem) {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            NSchedulerQueue::TReader* readers;
            ui32 readersCount = 0;
            Executors[excIdx]->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        if (auto shared = static_cast<TSharedExecutorPool*>(SharedPool.Get())) {
            NSchedulerQueue::TReader* readers;
            ui32 readersCount = 0;
            shared->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
    }

    void TCpuManager::Start() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Start();
        }
        if (SharedPool) {
            SharedPool->Start();
        }
    }

    void TCpuManager::PrepareStop() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->PrepareStop();
        }
    }

    void TCpuManager::Shutdown() {
        if (SharedPool) {
            SharedPool->Shutdown();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Shutdown();
        }
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount && round < 3; ++round) {
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
    }

    void TCpuManager::Cleanup() {
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount; ++round) {
            Y_ABORT_UNLESS(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        Executors.Destroy();
    }

    IExecutorPool* TCpuManager::CreateExecutorPool(ui32 poolId) {
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.PoolId == poolId) {
                TBasicExecutorPool *pool = new TBasicExecutorPool(cfg, Harmonizer.Get());
                auto sharedPool = static_cast<TSharedExecutorPool*>(SharedPool.Get());
                pool->SetOwnSharedThread(sharedPool->GetSharedThread(poolId));
                return pool;
            }
        }
        for (TIOExecutorPoolConfig& cfg : Config.IO) {
            if (cfg.PoolId == poolId) {
                return new TIOExecutorPool(cfg);
            }
        }
        Y_ABORT("missing PoolId: %d", int(poolId));
    }

    TVector<IExecutorPool*> TCpuManager::GetBasicExecutorPools() const {
        TVector<IExecutorPool*> pools;
        for (ui32 idx = 0; idx < ExecutorPoolCount; ++idx) {
            if (auto basicPool = dynamic_cast<TBasicExecutorPool*>(Executors[idx].Get()); basicPool != nullptr) {
                pools.push_back(basicPool);
            }
        }
        return pools;
    }

    void TCpuManager::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy, TVector<TExecutorThreadStats> &sharedStatsCopy) const {
        if (poolId < ExecutorPoolCount) {
            Executors[poolId]->GetCurrentStats(poolStats, statsCopy);
            if (auto shared = static_cast<TSharedExecutorPool*>(SharedPool.Get())) {
                shared->GetSharedStats(poolId, sharedStatsCopy);
            }
        }
    }

}
