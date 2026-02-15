#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>

#include <atomic>
#include <memory>

namespace NKvVolumeStress {

struct TWorkerLoadSnapshot {
    struct TWorkerRow {
        ui32 WorkerId = 0;
        ui32 ActiveActions = 0;
        ui32 Capacity = 0;
        double UtilizationPercent = 0.0;
    };

    ui32 WorkersTotal = 0;
    ui32 BusyWorkers = 0;
    ui64 ActiveActionsTotal = 0;
    ui64 CapacityTotal = 0;
    double UtilizationPercent = 0.0;
    double BusyPercent = 0.0;
    TVector<TWorkerRow> Workers;
};

class TWorkerLoadTracker {
public:
    explicit TWorkerLoadTracker(ui32 workersCount);

    void SetWorkerCapacity(ui32 workerId, ui32 capacity);
    void AddActive(ui32 workerId, i32 delta);
    TWorkerLoadSnapshot Snapshot() const;

private:
    const ui32 WorkersCount_ = 0;
    std::unique_ptr<std::atomic<ui32>[]> ActiveByWorker_;
    std::unique_ptr<std::atomic<ui32>[]> CapacityByWorker_;
};

} // namespace NKvVolumeStress
