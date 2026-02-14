#include "run_display.h"

#include "run_tui.h"

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <cstdio>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#endif

namespace NKvVolumeStress {

namespace {

constexpr auto DisplayUpdateInterval = std::chrono::seconds(1);
constexpr auto CurrentOpsRateWindow = std::chrono::seconds(5);

bool IsInteractiveTerminal() {
#ifdef _win_
    return _isatty(_fileno(stdout)) != 0 && _isatty(_fileno(stderr)) != 0;
#else
    return isatty(STDOUT_FILENO) != 0 && isatty(STDERR_FILENO) != 0;
#endif
}

double CalcRate(ui64 current, ui64 previous, double elapsedSeconds) {
    if (elapsedSeconds <= 0.0 || current < previous) {
        return 0.0;
    }
    return static_cast<double>(current - previous) / elapsedSeconds;
}

} // namespace

TRunDisplayController::TRunDisplayController(TRunStats& stats, ui32 durationSeconds, bool noTui, bool verbose)
    : Stats_(stats)
    , DurationSeconds_(durationSeconds)
    , NoTui_(noTui)
    , Verbose_(verbose)
{
}

TRunDisplayController::~TRunDisplayController() {
    Stop();
}

EDisplayMode TRunDisplayController::DetectMode() const {
    if (NoTui_ || Verbose_) {
        return EDisplayMode::Text;
    }

    if (IsInteractiveTerminal()) {
        return EDisplayMode::Tui;
    }

    return EDisplayMode::Text;
}

void TRunDisplayController::Start() {
    if (Thread_.joinable()) {
        return;
    }

    StartTs_ = std::chrono::steady_clock::now();
    PrevTs_ = StartTs_;
    PrevSnapshot_ = Stats_.Snapshot();
    HasPrevSnapshot_ = true;
    TotalActionsHistory_.clear();

    Mode_ = DetectMode();
    if (Mode_ == EDisplayMode::None) {
        return;
    }

    if (Mode_ == EDisplayMode::Tui) {
        Tui_ = std::make_unique<TRunTui>(BuildDisplayData(StartTs_));
    }

    StopRequested_.store(false, std::memory_order_relaxed);
    Thread_ = std::thread([this] {
        Loop();
    });
}

void TRunDisplayController::Stop() {
    StopRequested_.store(true, std::memory_order_relaxed);
    if (Thread_.joinable()) {
        Thread_.join();
    }
    Tui_.reset();
}

void TRunDisplayController::Loop() {
    while (!StopRequested_.load(std::memory_order_relaxed)) {
        const auto now = std::chrono::steady_clock::now();
        std::shared_ptr<TRunDisplayData> data = BuildDisplayData(now);

        switch (Mode_) {
            case EDisplayMode::Text:
                PrintText(*data);
                break;
            case EDisplayMode::Tui:
                if (Tui_) {
                    Tui_->Update(std::move(data));
                }
                break;
            case EDisplayMode::None:
                break;
        }

        for (int i = 0; i < 10 && !StopRequested_.load(std::memory_order_relaxed); ++i) {
            std::this_thread::sleep_for(DisplayUpdateInterval / 10);
        }
    }
}

std::shared_ptr<TRunDisplayData> TRunDisplayController::BuildDisplayData(std::chrono::steady_clock::time_point now) {
    const TRunStatsSnapshot snapshot = Stats_.Snapshot();
    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - StartTs_);
    const double elapsedSecondsFloat = std::chrono::duration_cast<std::chrono::duration<double>>(now - StartTs_).count();
    const ui32 elapsedSeconds = static_cast<ui32>(std::max<i64>(0, elapsed.count()));
    const ui32 boundedElapsed = std::min(elapsedSeconds, DurationSeconds_);
    const ui32 remaining = DurationSeconds_ > boundedElapsed ? DurationSeconds_ - boundedElapsed : 0;

    ui64 totalActions = 0;
    for (const auto& [_, count] : snapshot.ActionRuns) {
        totalActions += count;
    }

    ui64 prevTotalActions = 0;
    for (const auto& [_, count] : PrevSnapshot_.ActionRuns) {
        prevTotalActions += count;
    }

    const double intervalSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(now - PrevTs_).count();
    const double instantRate = HasPrevSnapshot_ ? CalcRate(totalActions, prevTotalActions, intervalSeconds) : 0.0;
    const double avgRate = elapsedSecondsFloat > 0.0 ? static_cast<double>(totalActions) / elapsedSecondsFloat : 0.0;

    TotalActionsHistory_.emplace_back(now, totalActions);
    while (!TotalActionsHistory_.empty() && now - TotalActionsHistory_.front().first > CurrentOpsRateWindow) {
        TotalActionsHistory_.pop_front();
    }

    double recentRate = instantRate;
    if (TotalActionsHistory_.size() >= 2) {
        const auto& [windowStartTs, windowStartActions] = TotalActionsHistory_.front();
        const double windowSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(now - windowStartTs).count();
        recentRate = CalcRate(totalActions, windowStartActions, windowSeconds);
    }

    auto data = std::make_shared<TRunDisplayData>();
    data->DurationSeconds = DurationSeconds_;
    data->ElapsedSeconds = boundedElapsed;
    data->RemainingSeconds = remaining;
    data->ProgressPercent = DurationSeconds_ > 0
        ? std::min(100.0, static_cast<double>(boundedElapsed) * 100.0 / DurationSeconds_)
        : 100.0;
    data->TotalActions = totalActions;
    data->TotalErrors = snapshot.TotalErrors;
    data->ActionsPerSecond = recentRate;
    data->AverageActionsPerSecond = avgRate;
    data->SampleErrors = snapshot.SampleErrors;

    data->Actions.reserve(snapshot.ActionRuns.size());
    for (const auto& [name, count] : snapshot.ActionRuns) {
        ui64 prevCount = 0;
        const auto it = PrevSnapshot_.ActionRuns.find(name);
        if (it != PrevSnapshot_.ActionRuns.end()) {
            prevCount = it->second;
        }

        TRunDisplayData::TActionRow row;
        row.Name = name;
        row.TotalRuns = count;
        row.RunsPerSecond = HasPrevSnapshot_ ? CalcRate(count, prevCount, intervalSeconds) : 0.0;
        data->Actions.push_back(std::move(row));
    }
    std::sort(data->Actions.begin(), data->Actions.end(), [](const auto& l, const auto& r) {
        return l.Name < r.Name;
    });

    data->Errors.reserve(snapshot.ErrorsByKind.size());
    for (const auto& [name, count] : snapshot.ErrorsByKind) {
        data->Errors.push_back({name, count});
    }
    std::sort(data->Errors.begin(), data->Errors.end(), [](const auto& l, const auto& r) {
        return l.Name < r.Name;
    });

    PrevSnapshot_ = snapshot;
    PrevTs_ = now;
    HasPrevSnapshot_ = true;

    return data;
}

void TRunDisplayController::PrintText(const TRunDisplayData& data) const {
    std::stringstream line;
    line << "[kv_volume] "
         << data.ElapsedSeconds / 60 << ":" << std::setfill('0') << std::setw(2) << (data.ElapsedSeconds % 60)
         << " elapsed, "
         << data.RemainingSeconds / 60 << ":" << std::setfill('0') << std::setw(2) << (data.RemainingSeconds % 60)
         << " remaining, "
         << std::fixed << std::setprecision(1) << data.ProgressPercent << "%";

    line << " | total_ops=" << data.TotalActions
         << " ops/s(cur)=" << std::fixed << std::setprecision(2) << data.ActionsPerSecond
         << " ops/s(avg)=" << std::fixed << std::setprecision(1) << data.AverageActionsPerSecond
         << " errors=" << data.TotalErrors;

    if (!data.Actions.empty()) {
        line << " | actions: ";
        bool first = true;
        for (const auto& action : data.Actions) {
            if (!first) {
                line << ", ";
            }
            first = false;
            line << action.Name << "=" << std::fixed << std::setprecision(1) << action.RunsPerSecond << "/s";
        }
    }

    Cerr << line.str() << Endl;
}

} // namespace NKvVolumeStress
