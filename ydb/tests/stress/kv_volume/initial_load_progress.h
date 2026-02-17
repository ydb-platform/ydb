#pragma once

#include <util/system/types.h>

#include <atomic>

namespace NKvVolumeStress {

struct TInitialLoadProgressSnapshot {
    ui64 TotalBytes = 0;
    ui64 TotalCommands = 0;

    ui64 ProcessedBytes = 0;
    ui64 SuccessfulBytes = 0;
    ui64 ProcessedCommands = 0;
    ui64 FailedCommands = 0;
};

class TInitialLoadProgress {
public:
    TInitialLoadProgress(ui64 totalBytes, ui64 totalCommands);

    void OnCommandFinished(ui64 bytes, bool success);
    TInitialLoadProgressSnapshot Snapshot() const;

private:
    const ui64 TotalBytes_ = 0;
    const ui64 TotalCommands_ = 0;

    std::atomic<ui64> ProcessedBytes_ = 0;
    std::atomic<ui64> SuccessfulBytes_ = 0;
    std::atomic<ui64> ProcessedCommands_ = 0;
    std::atomic<ui64> FailedCommands_ = 0;
};

} // namespace NKvVolumeStress
