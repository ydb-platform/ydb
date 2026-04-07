#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace NKvVolumeStress {

class TActionPool {
public:
    using TTask = std::function<void()>;

    explicit TActionPool(ui32 poolSize);
    ~TActionPool();

    void Start();
    void Stop();

    void Enqueue(TTask task);
    bool WaitForIdle(std::chrono::steady_clock::duration timeout);

private:
    struct alignas(64) TQueue {
        std::mutex Mutex;
        std::condition_variable Cv;
        alignas(64) std::deque<TTask> Pending;
    };

    void WorkerLoop(ui32 queueIndex);

private:
    const ui32 PoolSize_ = 1;

    // Lifecycle contract:
    // 1) Start() must be called before Enqueue().
    // 2) Enqueue() must not race with or happen after Stop().
    // 3) Stop() is a hard stop: pending tasks may be dropped.
    // 4) Queues_ is expected to be non-empty while the pool is running.
    alignas(64) std::atomic<bool> StopRequested_ = false;
    alignas(64) std::atomic<ui64> NextQueueIndex_ = 0;
    alignas(64) std::atomic<ui64> ActiveTasks_ = 0;

    alignas(64) std::mutex ActiveTasksMutex_;
    std::condition_variable ActiveTasksCv_;

    alignas(64) TVector<std::unique_ptr<TQueue>> Queues_;
    TVector<std::thread> Workers_;
};

} // namespace NKvVolumeStress
