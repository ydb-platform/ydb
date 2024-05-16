#pragma once

#include "executor_pool_basic.h"

#include <atomic>
#include <ctime>
#include <ydb/library/actors/util/thread.h>


namespace NActors {

class TBasicExecutorPoolSanitizer : public ISimpleThread {
public:
    TBasicExecutorPoolSanitizer(TBasicExecutorPool *pool)
        : Pool(pool)
    {}

    void CheckSemaphore() const {
        auto x = AtomicGet(Pool->Semaphore);
        auto semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(x);
        Y_ABORT_UNLESS(semaphore.OldSemaphore <= 0 || semaphore.CurrentThreadCount != semaphore.CurrentSleepThreadCount || Pool->StopFlag.load());
    }

    void* ThreadProc() override {
        while (!StopFlag.load(std::memory_order_acquire)) {
            CheckSemaphore();
            NanoSleep(1000);
        }
        return nullptr;
    }

    void Stop() {
        StopFlag.store(true, std::memory_order_release);
    }

private:
    TBasicExecutorPool *Pool;
    std::atomic_bool StopFlag = false;
};

} // namespace NActors
