#include "cgroup_oom_trend.h"

#include <algorithm>
#include <cmath>

namespace NActors::NDetail {

    namespace {

        double RelativeBytes(ui64 value, ui64 base) {
            return value >= base
                ? static_cast<double>(value - base)
                : -static_cast<double>(base - value);
        }

    } // namespace

    TCGroupOomTrendCalculator::TCGroupOomTrendCalculator(
            TCGroupOomTrendWindowsConfig windows) {
        if (windows.ShortWindow) {
            MaxWindow = std::max(MaxWindow, windows.ShortWindow->Duration);
            Windows[CGroupOomTrendWindowIndex(
                ECGroupOomTrendWindow::ShortWindow)].Config =
                    std::move(windows.ShortWindow);
        }
        if (windows.LongWindow) {
            MaxWindow = std::max(MaxWindow, windows.LongWindow->Duration);
            Windows[CGroupOomTrendWindowIndex(
                ECGroupOomTrendWindow::LongWindow)].Config =
                    std::move(windows.LongWindow);
        }
    }

    TRecalculatedCGroupOomTrendWindows TCGroupOomTrendCalculator::AddSample(
            TMonotonic timestamp,
            TCGroupMemoryStatsPtr stats) {
        TRecalculatedCGroupOomTrendWindows recalculated;
        if (MaxWindow == TDuration::Zero()) {
            return recalculated;
        }

        if (!Samples.empty() && timestamp <= Samples.back().Timestamp) {
            Reset();
        }

        if (!HistoryStartedAt) {
            HistoryStartedAt = timestamp;
        }
        Samples.push_back({
            .Timestamp = timestamp,
            .CurrentBytes = stats->CurrentBytes,
        });
        Prune(timestamp);

        for (ECGroupOomTrendWindow window : CGroupOomTrendWindows) {
            auto& state = Windows[CGroupOomTrendWindowIndex(window)];
            if (!state.Config) {
                continue;
            }
            if (!state.LastCalculatedAt ||
                    timestamp - *state.LastCalculatedAt >=
                        state.Config->RecalculationPeriod) {
                state.HasResult =
                    timestamp - *HistoryStartedAt >= state.Config->Duration;
                state.Trend =
                    CalculateTrend(window, state, timestamp, stats);
                state.LastCalculatedAt = timestamp;
                recalculated.Add(window);
            }
        }
        return recalculated;
    }

    bool TCGroupOomTrendCalculator::HasResult(ECGroupOomTrendWindow window) const {
        const auto& state = Windows[CGroupOomTrendWindowIndex(window)];
        return state.Config && state.HasResult;
    }

    const TCGroupOomTrend* TCGroupOomTrendCalculator::FindTrend(
            ECGroupOomTrendWindow window) const {
        const auto& state = Windows[CGroupOomTrendWindowIndex(window)];
        return state.Trend
            ? &*state.Trend
            : nullptr;
    }

    void TCGroupOomTrendCalculator::Reset() {
        Samples.clear();
        HistoryStartedAt.reset();
        for (auto& state : Windows) {
            state.Trend.reset();
            state.LastCalculatedAt.reset();
            state.HasResult = false;
        }
    }

    void TCGroupOomTrendCalculator::Prune(TMonotonic timestamp) {
        const TMonotonic cutoff = timestamp - MaxWindow;
        while (Samples.size() > 1 && Samples[1].Timestamp <= cutoff) {
            Samples.pop_front();
        }
    }

    std::optional<TCGroupOomTrend> TCGroupOomTrendCalculator::CalculateTrend(
            ECGroupOomTrendWindow window,
            const TWindowState& state,
            TMonotonic timestamp,
            const TCGroupMemoryStatsPtr& stats) const {
        const auto& config = *state.Config;
        if (!HistoryStartedAt || timestamp - *HistoryStartedAt < config.Duration ||
                !stats->MaxBytes || stats->MaxBytes->Unlimited ||
                !stats->MaxBytes->Value) {
            return std::nullopt;
        }

        const TMonotonic cutoff = timestamp - config.Duration;
        size_t first = 0;
        while (first + 1 < Samples.size() && Samples[first + 1].Timestamp <= cutoff) {
            ++first;
        }

        const size_t sampleCount = Samples.size() - first;
        // Two points always form a perfect line and do not provide a useful
        // linearity check.
        if (sampleCount < 3) {
            return std::nullopt;
        }

        const TMonotonic baseTimestamp = Samples[first].Timestamp;
        const ui64 baseBytes = Samples[first].CurrentBytes;
        double sumX = 0;
        double sumY = 0;
        for (size_t index = first; index < Samples.size(); ++index) {
            sumX += (Samples[index].Timestamp - baseTimestamp).SecondsFloat();
            sumY += RelativeBytes(Samples[index].CurrentBytes, baseBytes);
        }

        const double meanX = sumX / sampleCount;
        const double meanY = sumY / sampleCount;
        double sumXX = 0;
        double sumXY = 0;
        double sumYY = 0;
        for (size_t index = first; index < Samples.size(); ++index) {
            const double x =
                (Samples[index].Timestamp - baseTimestamp).SecondsFloat() - meanX;
            const double y =
                RelativeBytes(Samples[index].CurrentBytes, baseBytes) - meanY;
            sumXX += x * x;
            sumXY += x * y;
            sumYY += y * y;
        }

        if (sumXX <= 0 || sumYY <= 0) {
            return std::nullopt;
        }

        const double growthBytesPerSecond = sumXY / sumXX;
        const double rSquared = std::clamp(
            sumXY * sumXY / (sumXX * sumYY),
            0.0,
            1.0);
        if (!std::isfinite(growthBytesPerSecond) ||
                !std::isfinite(rSquared) ||
                growthBytesPerSecond <= 0 ||
                rSquared < config.MinimumRSquared) {
            return std::nullopt;
        }

        const ui64 maxBytes = stats->MaxBytes->Value;
        const double remainingBytes = stats->CurrentBytes < maxBytes
            ? static_cast<double>(maxBytes - stats->CurrentBytes)
            : 0.0;

        return TCGroupOomTrend{
            .Stats = stats,
            .Window = window,
            .WindowDuration = config.Duration,
            .GrowthBytesPerSecond = growthBytesPerSecond,
            .RSquared = rSquared,
            .TimeToOom = TDuration::Seconds(remainingBytes / growthBytesPerSecond),
            .SampleCount = sampleCount,
        };
    }

} // namespace NActors::NDetail
