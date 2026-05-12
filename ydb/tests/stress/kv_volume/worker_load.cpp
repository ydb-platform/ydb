#include "worker_load.h"

#include <algorithm>

namespace NKvVolumeStress {

TWorkerLoadTracker::TWorkerLoadTracker(ui32 workersCount)
    : WorkersCount_(workersCount)
    , ActiveByWorker_(std::make_unique<std::atomic<ui32>[]>(workersCount))
    , CapacityByWorker_(std::make_unique<std::atomic<ui32>[]>(workersCount))
{
    for (ui32 i = 0; i < workersCount; ++i) {
        ActiveByWorker_[i].store(0, std::memory_order_relaxed);
        CapacityByWorker_[i].store(0, std::memory_order_relaxed);
    }
}

void TWorkerLoadTracker::SetWorkerCapacity(ui32 workerId, ui32 capacity) {
    if (workerId >= WorkersCount_) {
        return;
    }
    CapacityByWorker_[workerId].store(capacity, std::memory_order_relaxed);
}

void TWorkerLoadTracker::AddActive(ui32 workerId, i32 delta) {
    if (workerId >= WorkersCount_ || delta == 0) {
        return;
    }

    if (delta > 0) {
        ActiveByWorker_[workerId].fetch_add(static_cast<ui32>(delta), std::memory_order_relaxed);
        return;
    }

    const ui32 decrement = static_cast<ui32>(-delta);
    std::atomic<ui32>& active = ActiveByWorker_[workerId];
    ui32 current = active.load(std::memory_order_relaxed);
    while (true) {
        const ui32 next = current > decrement ? current - decrement : 0;
        if (active.compare_exchange_weak(current, next, std::memory_order_relaxed)) {
            break;
        }
    }
}

TWorkerLoadSnapshot TWorkerLoadTracker::Snapshot() const {
    TWorkerLoadSnapshot snapshot;
    snapshot.WorkersTotal = WorkersCount_;
    snapshot.Workers.reserve(WorkersCount_);

    for (ui32 workerId = 0; workerId < WorkersCount_; ++workerId) {
        const ui32 active = ActiveByWorker_[workerId].load(std::memory_order_relaxed);
        const ui32 capacity = CapacityByWorker_[workerId].load(std::memory_order_relaxed);

        if (active > 0) {
            ++snapshot.BusyWorkers;
        }

        snapshot.ActiveActionsTotal += active;
        snapshot.CapacityTotal += capacity;

        TWorkerLoadSnapshot::TWorkerRow row;
        row.WorkerId = workerId;
        row.ActiveActions = active;
        row.Capacity = capacity;
        row.UtilizationPercent = capacity > 0
            ? std::min(100.0, static_cast<double>(active) * 100.0 / capacity)
            : 0.0;
        snapshot.Workers.push_back(std::move(row));
    }

    snapshot.UtilizationPercent = snapshot.CapacityTotal > 0
        ? std::min(100.0, static_cast<double>(snapshot.ActiveActionsTotal) * 100.0 / snapshot.CapacityTotal)
        : 0.0;
    snapshot.BusyPercent = snapshot.WorkersTotal > 0
        ? std::min(100.0, static_cast<double>(snapshot.BusyWorkers) * 100.0 / snapshot.WorkersTotal)
        : 0.0;

    return snapshot;
}

} // namespace NKvVolumeStress
