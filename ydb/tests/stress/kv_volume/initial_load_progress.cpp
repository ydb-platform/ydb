#include "initial_load_progress.h"

namespace NKvVolumeStress {

TInitialLoadProgress::TInitialLoadProgress(ui64 totalBytes, ui64 totalCommands)
    : TotalBytes_(totalBytes)
    , TotalCommands_(totalCommands)
{
}

void TInitialLoadProgress::OnCommandFinished(ui64 bytes, bool success) {
    ProcessedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    ProcessedCommands_.fetch_add(1, std::memory_order_relaxed);

    if (success) {
        SuccessfulBytes_.fetch_add(bytes, std::memory_order_relaxed);
    } else {
        FailedCommands_.fetch_add(1, std::memory_order_relaxed);
    }
}

TInitialLoadProgressSnapshot TInitialLoadProgress::Snapshot() const {
    TInitialLoadProgressSnapshot snapshot;
    snapshot.TotalBytes = TotalBytes_;
    snapshot.TotalCommands = TotalCommands_;
    snapshot.ProcessedBytes = ProcessedBytes_.load(std::memory_order_relaxed);
    snapshot.SuccessfulBytes = SuccessfulBytes_.load(std::memory_order_relaxed);
    snapshot.ProcessedCommands = ProcessedCommands_.load(std::memory_order_relaxed);
    snapshot.FailedCommands = FailedCommands_.load(std::memory_order_relaxed);
    return snapshot;
}

} // namespace NKvVolumeStress
