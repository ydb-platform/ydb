#pragma once

#include "run_stats.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <chrono>
#include <atomic>
#include <deque>
#include <memory>
#include <thread>

namespace NKvVolumeStress {

enum class EDisplayMode {
    None = 0,
    Text,
    Tui,
};

struct TRunDisplayData {
    struct TActionRow {
        TString Name;
        ui64 TotalRuns = 0;
        double RunsPerSecond = 0.0;
        TLatencyPercentiles Latency;
    };

    struct TErrorRow {
        TString Name;
        ui64 Total = 0;
    };

    ui32 DurationSeconds = 0;
    ui32 ElapsedSeconds = 0;
    ui32 RemainingSeconds = 0;
    double ProgressPercent = 0.0;

    ui64 TotalActions = 0;
    ui64 TotalErrors = 0;
    double ActionsPerSecond = 0.0;
    double AverageActionsPerSecond = 0.0;
    TLatencyPercentiles Latency;

    TVector<TActionRow> Actions;
    TVector<TErrorRow> Errors;
    TVector<TString> SampleErrors;
};

class TRunTui;

class TRunDisplayController {
public:
    TRunDisplayController(TRunStats& stats, ui32 durationSeconds, bool noTui, bool verbose);
    ~TRunDisplayController();

    void Start();
    void Stop();

private:
    EDisplayMode DetectMode() const;
    void Loop();
    std::shared_ptr<TRunDisplayData> BuildDisplayData(std::chrono::steady_clock::time_point now);
    void PrintText(const TRunDisplayData& data) const;

private:
    TRunStats& Stats_;
    const ui32 DurationSeconds_;
    const bool NoTui_;
    const bool Verbose_;

    EDisplayMode Mode_ = EDisplayMode::None;
    std::atomic<bool> StopRequested_ = false;
    std::thread Thread_;

    std::chrono::steady_clock::time_point StartTs_;
    std::chrono::steady_clock::time_point PrevTs_;
    TRunStatsSnapshot PrevSnapshot_;
    bool HasPrevSnapshot_ = false;
    std::deque<std::pair<std::chrono::steady_clock::time_point, ui64>> TotalActionsHistory_;
    THashMap<TString, std::deque<std::pair<std::chrono::steady_clock::time_point, ui64>>> ActionRunsHistory_;

    std::unique_ptr<TRunTui> Tui_;
};

} // namespace NKvVolumeStress
