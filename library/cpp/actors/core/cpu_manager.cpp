#include "cpu_manager.h"
#include "probes.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void TCpuManager::Setup() {
        TAffinity available;
        available.Current();
        TCpuAllocationConfig allocation(available, Config);

        if (allocation) {
            if (!Balancer) {
                Balancer.Reset(MakeBalancer(Config.UnitedWorkers.Balancer, Config.United, GetCycleCountFast()));
            }
            UnitedWorkers.Reset(new TUnitedWorkers(Config.UnitedWorkers, Config.United, allocation, Balancer.Get()));
        }

        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);

        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx].Reset(CreateExecutorPool(excIdx));
        }
    }

    void TCpuManager::PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem) {
        if (UnitedWorkers) {
            UnitedWorkers->Prepare(actorSystem, scheduleReaders);
        }
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
        if (UnitedWorkers) {
            UnitedWorkers->Start();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Start();
        }
    }

    void TCpuManager::PrepareStop() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->PrepareStop();
        }
        if (UnitedWorkers) {
            UnitedWorkers->PrepareStop();
        }
    }

    void TCpuManager::Shutdown() {
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Shutdown();
        }
        if (UnitedWorkers) {
            UnitedWorkers->Shutdown();
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
            Y_VERIFY(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        Executors.Destroy();
        UnitedWorkers.Destroy();
    }

    IExecutorPool* TCpuManager::CreateExecutorPool(ui32 poolId) {
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.PoolId == poolId) {
                return new TBasicExecutorPool(cfg);
            }
        }
        for (TIOExecutorPoolConfig& cfg : Config.IO) {
            if (cfg.PoolId == poolId) {
                return new TIOExecutorPool(cfg);
            }
        }
        for (TUnitedExecutorPoolConfig& cfg : Config.United) {
            if (cfg.PoolId == poolId) {
                IExecutorPool* result = new TUnitedExecutorPool(cfg, UnitedWorkers.Get());
                return result;
            }
        }
        Y_FAIL("missing PoolId: %d", int(poolId));
    }
}
