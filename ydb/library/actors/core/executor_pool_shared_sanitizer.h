#pragma once

#include "executor_pool_shared.h"
#include "debug.h"

#include <atomic>
#include <ctime>
#include <ydb/library/actors/util/thread.h>


namespace NActors {

class TSharedExecutorPoolSanitizer : public ISimpleThread {
public:
    TSharedExecutorPoolSanitizer(TSharedExecutorPool *pool)
        : Pool(pool)
    {}

    void CheckSemaphore() const {
        Y_UNUSED(Pool);
            /*
        bool found = false;
        for (ui32 i = 0; i != Pool->PoolManager.PoolInfos.size(); ++i) {
            auto *pool = Pool->Pools[i];
            if (pool == nullptr) {
                continue;
            }
            ui64 semaphore = Pool->Pools[i]->GetSemaphore().OldSemaphore;
            Y_UNUSED(semaphore);
            ui64 leases = ForeignThreadSlots[i].load(std::memory_order_acquire);
            ui64 localNotifications = Pool->Pools[i]->LocalNotifications.load(std::memory_order_acquire);

            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "Iteration_", Iteration, " Pseudo_", i, " TSharedExecutorPoolSanitizer::CheckSemaphore: semaphore == ", semaphore, " leases == ", leases, " localThreads == ", localThreads, " defaultThreads == ", Pool->Pools[i]->Config.DefaultThreadsCount, " localNotifications == ", localNotifications);
            if (semaphore > 0 && leases == Pool->Pools[i]->Config.Leases && localThreads == 0 && localNotifications == 0) {
                ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "Iteration_", Iteration, " Pseudo_", i, " TSharedExecutorPoolSanitizer::CheckSemaphore: error state found: semaphore > 0 && leases == Pool->Pools[i]->Config.Leases && localThreads == 0 && localNotifications == 0");
                found = true;
            }
        }
        if (!found) {
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "Iteration_", Iteration, " TSharedExecutorPoolSanitizer::CheckSemaphore: no error states found");
        }
        */
    }

    void* ThreadProc() override {
        while (!StopFlag.load(std::memory_order_acquire)) {
            CheckSemaphore();
            NanoSleep(1000000000);
            Iteration++;
        }
        return nullptr;
    }

    void Stop() {
        StopFlag.store(true, std::memory_order_release);
    }

private:
    TSharedExecutorPool *Pool;
    std::atomic_bool StopFlag = false;
    ui64 Iteration = 0;
};

} // namespace NActors
