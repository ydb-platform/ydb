#pragma once

#include "cgroup_oom.h"

#include <ydb/library/actors/core/monotonic.h>

#include <util/generic/deque.h>

#include <array>
#include <optional>

namespace NActors::NDetail {

    inline constexpr std::array<ECGroupOomTrendWindow, 2> CGroupOomTrendWindows = {
        ECGroupOomTrendWindow::ShortWindow,
        ECGroupOomTrendWindow::LongWindow,
    };

    constexpr size_t CGroupOomTrendWindowIndex(ECGroupOomTrendWindow window) {
        return static_cast<size_t>(window);
    }

    class TRecalculatedCGroupOomTrendWindows {
    public:
        void Add(ECGroupOomTrendWindow window) {
            Recalculated[CGroupOomTrendWindowIndex(window)] = true;
        }

        bool Contains(ECGroupOomTrendWindow window) const {
            return Recalculated[CGroupOomTrendWindowIndex(window)];
        }

    private:
        std::array<bool, CGroupOomTrendWindows.size()> Recalculated = {};
    };

    class TCGroupOomTrendCalculator {
    public:
        explicit TCGroupOomTrendCalculator(
            TCGroupOomTrendWindowsConfig windows);

        // Stores every sample and reports only windows whose regression was
        // recalculated for this sample.
        TRecalculatedCGroupOomTrendWindows AddSample(
            TMonotonic timestamp,
            TCGroupMemoryStatsPtr stats);
        bool HasResult(ECGroupOomTrendWindow window) const;
        const TCGroupOomTrend* FindTrend(ECGroupOomTrendWindow window) const;

    private:
        struct TSample {
            TMonotonic Timestamp;
            ui64 CurrentBytes = 0;
        };

        struct TWindowState {
            std::optional<TCGroupOomTrendWindowConfig> Config;
            std::optional<TCGroupOomTrend> Trend;
            std::optional<TMonotonic> LastCalculatedAt;
            bool HasResult = false;
        };

        void Reset();
        void Prune(TMonotonic timestamp);
        std::optional<TCGroupOomTrend> CalculateTrend(
            ECGroupOomTrendWindow window,
            const TWindowState& state,
            TMonotonic timestamp,
            const TCGroupMemoryStatsPtr& stats) const;

    private:
        std::array<TWindowState, CGroupOomTrendWindows.size()> Windows;
        TDeque<TSample> Samples;
        TDuration MaxWindow;
        std::optional<TMonotonic> HistoryStartedAt;
        std::optional<ECGroupVersion> Version;
        TString CGroupPath;
    };

} // namespace NActors::NDetail
