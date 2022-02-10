#pragma once 
 
#include "actorsystem.h" 
#include "executor_pool_basic.h" 
#include "executor_pool_io.h" 
#include "executor_pool_united.h" 
 
namespace NActors { 
    class TCpuManager : public TNonCopyable { 
        const ui32 ExecutorPoolCount; 
        TArrayHolder<TAutoPtr<IExecutorPool>> Executors; 
        THolder<TUnitedWorkers> UnitedWorkers; 
        THolder<IBalancer> Balancer; 
        TCpuManagerConfig Config; 
    public: 
        explicit TCpuManager(THolder<TActorSystemSetup>& setup) 
            : ExecutorPoolCount(setup->GetExecutorsCount()) 
            , Balancer(setup->Balancer) 
            , Config(setup->CpuManager) 
        { 
            if (setup->Executors) { // Explicit mode w/o united pools 
                Executors.Reset(setup->Executors.Release()); 
                for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) { 
                    IExecutorPool* pool = Executors[excIdx].Get(); 
                    Y_VERIFY(dynamic_cast<TUnitedExecutorPool*>(pool) == nullptr, 
                        "united executor pool is prohibited in explicit mode of NActors::TCpuManager"); 
                } 
            } else { 
                Setup(); 
            } 
        } 
 
        void Setup(); 
        void PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem); 
        void Start(); 
        void PrepareStop(); 
        void Shutdown(); 
        void Cleanup(); 
 
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
 
    private: 
        IExecutorPool* CreateExecutorPool(ui32 poolId); 
    }; 
} 
