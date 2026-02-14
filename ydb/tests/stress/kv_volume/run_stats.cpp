#include "run_stats.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace NKvVolumeStress {

void TRunStats::RecordAction(const TString& actionName) {
    std::lock_guard lock(Mutex_);
    ++ActionRuns_[actionName];
}

void TRunStats::RecordError(const TString& kind, const TString& message) {
    std::lock_guard lock(Mutex_);
    ++ErrorsByKind_[kind];
    ++TotalErrors_;

    if (SampleErrors_.size() < 20) {
        SampleErrors_.push_back(TStringBuilder() << kind << ": " << message);
    }
}

ui64 TRunStats::GetTotalErrors() const {
    std::lock_guard lock(Mutex_);
    return TotalErrors_;
}

TRunStatsSnapshot TRunStats::Snapshot() const {
    std::lock_guard lock(Mutex_);

    TRunStatsSnapshot snapshot;
    snapshot.ActionRuns = ActionRuns_;
    snapshot.ErrorsByKind = ErrorsByKind_;
    snapshot.SampleErrors = SampleErrors_;
    snapshot.TotalErrors = TotalErrors_;
    return snapshot;
}

void TRunStats::PrintSummary(double elapsedSeconds) const {
    const TRunStatsSnapshot snapshot = Snapshot();

    TVector<std::pair<TString, ui64>> sortedActions(snapshot.ActionRuns.begin(), snapshot.ActionRuns.end());
    std::sort(sortedActions.begin(), sortedActions.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, ui64>> sortedErrors(snapshot.ErrorsByKind.begin(), snapshot.ErrorsByKind.end());
    std::sort(sortedErrors.begin(), sortedErrors.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    ui64 totalActions = 0;
    for (const auto& [_, count] : sortedActions) {
        totalActions += count;
    }

    const double safeElapsedSeconds = elapsedSeconds > 0.0 ? elapsedSeconds : 0.0;
    const double totalOpsPerSecond = safeElapsedSeconds > 0.0
        ? static_cast<double>(totalActions) / safeElapsedSeconds
        : 0.0;

    Cout << "==== kv_volume summary ====" << Endl;
    {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << safeElapsedSeconds;
        Cout << "Elapsed seconds: " << ss.str() << Endl;
    }
    Cout << "Total actions: " << totalActions << Endl;
    {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << totalOpsPerSecond;
        Cout << "Average ops/s: " << ss.str() << Endl;
    }
    Cout << "Action runs:" << Endl;
    for (const auto& [name, count] : sortedActions) {
        const double actionOpsPerSecond = safeElapsedSeconds > 0.0
            ? static_cast<double>(count) / safeElapsedSeconds
            : 0.0;
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << actionOpsPerSecond;

        Cout << "  " << name << ": " << count
             << " (" << ss.str() << " ops/s)" << Endl;
    }

    Cout << "Errors by kind:" << Endl;
    if (sortedErrors.empty()) {
        Cout << "  none" << Endl;
    } else {
        for (const auto& [name, count] : sortedErrors) {
            Cout << "  " << name << ": " << count << Endl;
        }
    }

    Cout << "Total errors: " << snapshot.TotalErrors << Endl;

    if (!snapshot.SampleErrors.empty()) {
        Cout << "Sample errors:" << Endl;
        for (const auto& error : snapshot.SampleErrors) {
            Cout << "  " << error << Endl;
        }
    }
}

} // namespace NKvVolumeStress
