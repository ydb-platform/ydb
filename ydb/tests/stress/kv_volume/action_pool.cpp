#include "action_pool.h"

#include <algorithm>

namespace NKvVolumeStress {

TActionPool::TActionPool(ui32 poolSize)
    : PoolSize_(std::max<ui32>(1, poolSize))
{
}

TActionPool::~TActionPool() {
    Stop();
}

void TActionPool::Start() {
    StopRequested_.store(false, std::memory_order_release);
    NextQueueIndex_.store(0, std::memory_order_release);
    ActiveTasks_.store(0, std::memory_order_release);

    Queues_.clear();
    Queues_.reserve(PoolSize_);
    Workers_.clear();
    Workers_.reserve(PoolSize_);

    try {
        for (ui32 i = 0; i < PoolSize_; ++i) {
            Queues_.push_back(std::make_unique<TQueue>());
            Workers_.emplace_back([this, i] {
                WorkerLoop(i);
            });
        }
    } catch (...) {
        StopRequested_.store(true, std::memory_order_release);
        for (const auto& queue : Queues_) {
            if (queue) {
                queue->Cv.notify_all();
            }
        }
        for (auto& worker : Workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        Workers_.clear();
        Queues_.clear();
        throw;
    }
}

void TActionPool::Stop() {
    StopRequested_.store(true, std::memory_order_release);
    for (const auto& queue : Queues_) {
        if (queue) {
            queue->Cv.notify_all();
        }
    }

    for (auto& worker : Workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    Workers_.clear();
    Queues_.clear();
}

void TActionPool::Enqueue(TTask task) {
    auto queueNumSeq = NextQueueIndex_.fetch_add(1, std::memory_order_acq_rel);
    const ui32 queueIndex = static_cast<ui32>(queueNumSeq) % Queues_.size();
    TQueue* queue = Queues_[queueIndex].get();
    {
        std::lock_guard lock(queue->Mutex);
        queue->Pending.push_back(std::move(task));
        ActiveTasks_.fetch_add(1, std::memory_order_acq_rel);
    }

    queue->Cv.notify_one();
}

bool TActionPool::WaitForIdle(std::chrono::steady_clock::duration timeout) {
    std::unique_lock lock(ActiveTasksMutex_);
    return ActiveTasksCv_.wait_for(lock, timeout, [this] {
        return ActiveTasks_.load(std::memory_order_acquire) == 0;
    });
}

void TActionPool::WorkerLoop(ui32 queueIndex) {
    if (queueIndex >= Queues_.size() || !Queues_[queueIndex]) {
        return;
    }
    TQueue& queue = *Queues_[queueIndex];

    while (!StopRequested_.load(std::memory_order_acquire)) {
        TTask task;
        {
            std::unique_lock lock(queue.Mutex);
            while (!StopRequested_.load(std::memory_order_acquire) && queue.Pending.empty()) {
                queue.Cv.wait(lock);
            }

            if (queue.Pending.empty()) {
                continue;
            }

            task = std::move(queue.Pending.front());
            queue.Pending.pop_front();
        }

        try {
            task();
        } catch (...) {
            // skip
        }

        if (ActiveTasks_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard lock(ActiveTasksMutex_);
            ActiveTasksCv_.notify_all();
        }
    }
}

} // namespace NKvVolumeStress
