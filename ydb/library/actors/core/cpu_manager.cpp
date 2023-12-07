#include "cpu_manager.h"
#include "probes.h"

#include "executor_pool_basic.h"
#include "executor_pool_io.h"

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

        ui64 ts = GetCycleCountFast();
        Harmonizer.Reset(MakeHarmonizer(ts));

        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);

        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx].Reset(CreateExecutorPool(excIdx));
            if (excIdx < Config.PingInfoByPool.size()) {
                Harmonizer->AddPool(Executors[excIdx].Get(), &Config.PingInfoByPool[excIdx]);
            } else {
                Harmonizer->AddPool(Executors[excIdx].Get());
            }
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
    }

    void TCpuManager::Start() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Start();
        }
    }

    void TCpuManager::PrepareStop() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->PrepareStop();
        }
    }

    void TCpuManager::Shutdown() {
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
                return new TBasicExecutorPool(cfg, Harmonizer.Get());
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

}
