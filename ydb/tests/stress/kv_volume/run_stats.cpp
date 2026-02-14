#include "run_stats.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace NKvVolumeStress {

namespace {

constexpr std::array<ui64, 48> LatencyBucketUpperBoundsMs = {
    1, 2, 3,
    4, 5, 6,
    8, 10, 12,
    16, 20, 24,
    32, 40, 48,
    64, 80, 96,
    128, 160, 192,
    256, 320, 384,
    512, 640, 768,
    1000, 1200, 1500,
    2000, 2500, 3000,
    4000, 5000, 6000,
    8000, 10000, 12000,
    16000, 20000, 24000,
    32000, 40000, 48000,
    64000, 80000, 96000
};

} // namespace

TRunStats::TRunStats(TVector<TString> actionNames)
    : ActionNames_(std::move(actionNames))
{
    ActionStats_.resize(ActionNames_.size());
    TVector<TString> sortedActionNames = ActionNames_;
    std::sort(sortedActionNames.begin(), sortedActionNames.end());
    for (size_t i = 1; i < sortedActionNames.size(); ++i) {
        if (sortedActionNames[i - 1] == sortedActionNames[i]) {
            throw std::runtime_error(TStringBuilder() << "duplicate action name in stats registry: " << sortedActionNames[i]);
        }
    }
}

size_t TRunStats::FindLatencyBucket(ui64 latencyMs) {
    for (size_t i = 0; i < LatencyBucketUpperBoundsMs.size(); ++i) {
        if (latencyMs <= LatencyBucketUpperBoundsMs[i]) {
            return i;
        }
    }
    return LatencyBucketCount - 1;
}

void TRunStats::RecordLatencySample(TLatencyHistogram& histogram, ui64 latencyMs) {
    const size_t bucket = FindLatencyBucket(latencyMs);
    ++histogram.Buckets[bucket];
    ++histogram.Samples;
    histogram.MaxMs = std::max(histogram.MaxMs, latencyMs);
}

TLatencyPercentiles TRunStats::BuildPercentiles(const TLatencyHistogram& histogram) {
    TLatencyPercentiles percentiles;
    percentiles.Samples = histogram.Samples;
    if (histogram.Samples == 0) {
        return percentiles;
    }

    const ui64 p50Rank = std::max<ui64>(1, static_cast<ui64>(std::ceil(histogram.Samples * 0.50)));
    const ui64 p90Rank = std::max<ui64>(1, static_cast<ui64>(std::ceil(histogram.Samples * 0.90)));
    const ui64 p99Rank = std::max<ui64>(1, static_cast<ui64>(std::ceil(histogram.Samples * 0.99)));

    const auto valueAtRank = [&histogram](ui64 rank) -> ui64 {
        ui64 cumulative = 0;
        for (size_t i = 0; i < histogram.Buckets.size(); ++i) {
            const ui64 bucketSamples = histogram.Buckets[i];
            if (bucketSamples == 0) {
                continue;
            }

            const ui64 cumulativeBefore = cumulative;
            cumulative += bucketSamples;
            if (cumulative < rank) {
                continue;
            }

            const double lowerBoundMs = i == 0
                ? 0.0
                : static_cast<double>(LatencyBucketUpperBoundsMs[i - 1]);
            const double bucketUpperBoundMs = i < LatencyBucketUpperBoundsMs.size()
                ? static_cast<double>(LatencyBucketUpperBoundsMs[i])
                : static_cast<double>(histogram.MaxMs);
            const double upperBoundMs = std::min(bucketUpperBoundMs, static_cast<double>(histogram.MaxMs));

            if (upperBoundMs <= lowerBoundMs) {
                return std::min<ui64>(static_cast<ui64>(upperBoundMs), histogram.MaxMs);
            }

            const double rankInBucket = static_cast<double>(rank - cumulativeBefore);
            const double bucketFraction = std::clamp(
                rankInBucket / static_cast<double>(bucketSamples),
                0.0,
                1.0);
            const double interpolatedMs = lowerBoundMs + (upperBoundMs - lowerBoundMs) * bucketFraction;
            const ui64 estimatedMs = static_cast<ui64>(std::llround(interpolatedMs));
            return std::min(estimatedMs, histogram.MaxMs);
        }
        return histogram.MaxMs;
    };

    percentiles.P50Ms = valueAtRank(p50Rank);
    percentiles.P90Ms = valueAtRank(p90Rank);
    percentiles.P99Ms = valueAtRank(p99Rank);
    percentiles.P100Ms = histogram.MaxMs;
    return percentiles;
}

void TRunStats::IncrementNamedCounter(TVector<TNamedCounter>& counters, const TString& name) {
    for (auto& counter : counters) {
        if (counter.Name == name) {
            ++counter.Total;
            return;
        }
    }
    counters.push_back(TNamedCounter{name, 1});
}

bool TRunStats::IsValidActionIndex(ui32 actionIndex) const {
    return actionIndex < ActionStats_.size();
}

void TRunStats::RecordAction(ui32 actionIndex) {
    std::lock_guard lock(Mutex_);
    if (IsValidActionIndex(actionIndex)) {
        ++ActionStats_[actionIndex].Runs;
    }
}

void TRunStats::RecordReadBytes(ui32 actionIndex, ui64 bytes) {
    std::lock_guard lock(Mutex_);
    if (IsValidActionIndex(actionIndex)) {
        ActionStats_[actionIndex].ReadBytes += bytes;
    }
}

void TRunStats::RecordWriteBytes(ui32 actionIndex, ui64 bytes) {
    std::lock_guard lock(Mutex_);
    if (IsValidActionIndex(actionIndex)) {
        ActionStats_[actionIndex].WriteBytes += bytes;
    }
}

void TRunStats::RecordLatency(ui32 actionIndex, ui64 latencyMs) {
    std::lock_guard lock(Mutex_);
    RecordLatencySample(TotalLatency_, latencyMs);
    if (IsValidActionIndex(actionIndex)) {
        RecordLatencySample(ActionStats_[actionIndex].Latency, latencyMs);
    }
}

void TRunStats::RecordError(const TString& kind, const TString& message, std::optional<ui32> actionIndex) {
    std::lock_guard lock(Mutex_);
    IncrementNamedCounter(ErrorsByKind_, kind);
    ++TotalErrors_;
    if (actionIndex && IsValidActionIndex(*actionIndex)) {
        ++ActionStats_[*actionIndex].Errors;
    }

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
    snapshot.ActionNames = ActionNames_;
    snapshot.ActionRuns.resize(ActionStats_.size());
    snapshot.ErrorsByAction.resize(ActionStats_.size());
    snapshot.ReadBytesByAction.resize(ActionStats_.size());
    snapshot.WriteBytesByAction.resize(ActionStats_.size());
    snapshot.LatencyByAction.resize(ActionStats_.size());
    for (size_t i = 0; i < ActionStats_.size(); ++i) {
        const TActionStats& actionStats = ActionStats_[i];
        snapshot.ActionRuns[i] = actionStats.Runs;
        snapshot.ErrorsByAction[i] = actionStats.Errors;
        snapshot.ReadBytesByAction[i] = actionStats.ReadBytes;
        snapshot.WriteBytesByAction[i] = actionStats.WriteBytes;
        snapshot.LatencyByAction[i] = BuildPercentiles(actionStats.Latency);
    }
    snapshot.ErrorsByKind = ErrorsByKind_;
    snapshot.TotalLatency = BuildPercentiles(TotalLatency_);
    snapshot.SampleErrors = SampleErrors_;
    snapshot.TotalErrors = TotalErrors_;
    return snapshot;
}

void TRunStats::PrintSummary(double elapsedSeconds) const {
    const TRunStatsSnapshot snapshot = Snapshot();

    TVector<std::pair<TString, ui64>> sortedActions;
    sortedActions.reserve(snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedActions.push_back({snapshot.ActionNames[i], i < snapshot.ActionRuns.size() ? snapshot.ActionRuns[i] : 0});
    }
    std::sort(sortedActions.begin(), sortedActions.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, ui64>> sortedErrors;
    sortedErrors.reserve(snapshot.ErrorsByKind.size());
    for (const auto& error : snapshot.ErrorsByKind) {
        sortedErrors.push_back({error.Name, error.Total});
    }
    std::sort(sortedErrors.begin(), sortedErrors.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, ui64>> sortedActionErrors;
    sortedActionErrors.reserve(snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedActionErrors.push_back(
            {snapshot.ActionNames[i], i < snapshot.ErrorsByAction.size() ? snapshot.ErrorsByAction[i] : 0});
    }
    std::sort(sortedActionErrors.begin(), sortedActionErrors.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, TLatencyPercentiles>> sortedLatencies(
        snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedLatencies[i] = {
            snapshot.ActionNames[i],
            i < snapshot.LatencyByAction.size() ? snapshot.LatencyByAction[i] : TLatencyPercentiles{}
        };
    }
    std::sort(sortedLatencies.begin(), sortedLatencies.end(), [](const auto& l, const auto& r) {
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

    Cout << "Latency (ms):" << Endl;
    if (snapshot.TotalLatency.Samples == 0) {
        Cout << "  none" << Endl;
    } else {
        Cout << "  total:"
             << " p50=" << snapshot.TotalLatency.P50Ms
             << " p90=" << snapshot.TotalLatency.P90Ms
             << " p99=" << snapshot.TotalLatency.P99Ms
             << " p100=" << snapshot.TotalLatency.P100Ms
             << " samples=" << snapshot.TotalLatency.Samples
             << Endl;
    }

    if (!sortedLatencies.empty()) {
        Cout << "Latency by action (ms):" << Endl;
        for (const auto& [kind, lat] : sortedLatencies) {
            if (lat.Samples == 0) {
                continue;
            }
            Cout << "  " << kind << ":"
                 << " p50=" << lat.P50Ms
                 << " p90=" << lat.P90Ms
                 << " p99=" << lat.P99Ms
                 << " p100=" << lat.P100Ms
                 << " samples=" << lat.Samples
                 << Endl;
        }
    }

    bool hasActionErrors = false;
    for (const auto& [_, count] : sortedActionErrors) {
        if (count > 0) {
            hasActionErrors = true;
            break;
        }
    }

    if (hasActionErrors) {
        Cout << "Errors by action:" << Endl;
        for (const auto& [action, count] : sortedActionErrors) {
            if (count == 0) {
                continue;
            }
            Cout << "  " << action << ": " << count << Endl;
        }
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
