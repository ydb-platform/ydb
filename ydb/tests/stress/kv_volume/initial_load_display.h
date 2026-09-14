#pragma once

#include "initial_load_progress.h"
#include "run_stats.h"

#include <util/system/types.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

namespace NKvVolumeStress {

struct TInitialLoadDisplayData {
    ui64 TotalBytes = 0;
    ui64 ProcessedBytes = 0;
    ui64 SuccessfulBytes = 0;
    ui64 RemainingBytes = 0;

    ui64 TotalCommands = 0;
    ui64 ProcessedCommands = 0;
    ui64 FailedCommands = 0;

    ui64 TotalErrors = 0;

    double ProgressPercent = 0.0;
    double ProcessedBytesPerSecond = 0.0;
    double SuccessfulBytesPerSecond = 0.0;
    double ElapsedSeconds = 0.0;
};

class TInitialLoadDisplayController {
public:
    TInitialLoadDisplayController(
        const TInitialLoadProgress& progress,
        const TRunStats& stats,
        bool noTui,
        bool verbose);
    ~TInitialLoadDisplayController();

    void Start();
    void Stop();

private:
    class TTui;

    enum class EDisplayMode {
        None = 0,
        Text,
        Tui,
    };

    EDisplayMode DetectMode() const;
    void Loop();
    std::shared_ptr<TInitialLoadDisplayData> BuildDisplayData(std::chrono::steady_clock::time_point now);
    void PrintText(const TInitialLoadDisplayData& data) const;

private:
    const TInitialLoadProgress& Progress_;
    const TRunStats& Stats_;
    const bool NoTui_;
    const bool Verbose_;

    EDisplayMode Mode_ = EDisplayMode::None;
    std::atomic<bool> StopRequested_ = false;
    std::thread Thread_;

    std::chrono::steady_clock::time_point StartTs_;
    std::chrono::steady_clock::time_point PrevTs_;
    TInitialLoadProgressSnapshot PrevSnapshot_;
    bool HasPrevSnapshot_ = false;

    std::unique_ptr<TTui> Tui_;
};

} // namespace NKvVolumeStress
