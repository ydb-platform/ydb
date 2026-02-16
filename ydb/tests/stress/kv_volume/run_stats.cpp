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

std::string FormatBytes(ui64 bytes) {
    static constexpr std::array<const char*, 6> units = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};

    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < units.size()) {
        value /= 1024.0;
        ++unit;
    }

    std::stringstream ss;
    if (unit == 0) {
        ss << static_cast<ui64>(value) << " " << units[unit];
    } else {
        ss << std::fixed << std::setprecision(value >= 100.0 ? 0 : 1) << value << " " << units[unit];
    }
    return ss.str();
}

std::string FormatRate(double bytesPerSecond) {
    if (bytesPerSecond <= 0.0) {
        return "0 B/s";
    }
    return FormatBytes(static_cast<ui64>(bytesPerSecond)) + "/s";
}

} // namespace

TRunStats::TPaddedAtomicUi64::TPaddedAtomicUi64() {
    Value.store(0, std::memory_order_relaxed);
}

TRunStats::TAtomicLatencyHistogram::TAtomicLatencyHistogram() {
    // initialized by TPaddedAtomicUi64 default constructors
}

TRunStats::TAtomicActionStats::TAtomicActionStats() {
    // initialized by TPaddedAtomicUi64 default constructors
}

TRunStats::TRunStats(TVector<TString> actionNames)
    : ActionCount_(static_cast<ui32>(actionNames.size()))
    , ActionNames_(std::move(actionNames))
{
    if (ActionCount_ > 0) {
        ActionStats_ = std::make_unique<TAtomicActionStats[]>(ActionCount_);
    }

    TVector<TString> sortedActionNames = ActionNames_;
    std::sort(sortedActionNames.begin(), sortedActionNames.end());
    for (size_t i = 1; i < sortedActionNames.size(); ++i) {
        if (sortedActionNames[i - 1] == sortedActionNames[i]) {
            throw std::runtime_error(TStringBuilder() << "duplicate action name in stats registry: " << sortedActionNames[i]);
        }
    }
}

size_t TRunStats::FindLatencyBucket(ui64 latencyMs) {
    if (latencyMs <= 1) {
        return 0;
    }
    if (latencyMs <= 5) {
        return latencyMs - 1;
    }
    for (size_t i = 5; i < LatencyBucketUpperBoundsMs.size(); ++i) {
        if (latencyMs <= LatencyBucketUpperBoundsMs[i]) {
            return i;
        }
    }
    return LatencyBucketCount - 1;
}

void TRunStats::UpdateMax(TPaddedAtomicUi64& maxValue, ui64 candidate) {
    ui64 current = maxValue.Value.load(std::memory_order_acquire);
    while (current < candidate
        && !maxValue.Value.compare_exchange_weak(
            current,
            candidate,
            std::memory_order_acq_rel,
            std::memory_order_acquire))
    {
    }
}

void TRunStats::RecordLatencySample(TAtomicLatencyHistogram& histogram, ui64 latencyMs) {
    const size_t bucket = FindLatencyBucket(latencyMs);
    histogram.Buckets[bucket].Value.fetch_add(1, std::memory_order_relaxed);
    UpdateMax(histogram.MaxMs, latencyMs);
}

TRunStats::TLatencyHistogram TRunStats::BuildHistogramSnapshot(const TAtomicLatencyHistogram& histogram) {
    TLatencyHistogram result;
    for (size_t i = 0; i < result.Buckets.size(); ++i) {
        const ui64 count = histogram.Buckets[i].Value.load(std::memory_order_relaxed);
        result.Buckets[i] = count;
        result.Samples += count;
    }
    result.MaxMs = histogram.MaxMs.Value.load(std::memory_order_acquire);
    return result;
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

void TRunStats::RecordAction(ui32 actionIndex) {
    if (IsValidActionIndex(actionIndex)) {
        ActionStats_[actionIndex].Runs.Value.fetch_add(1, std::memory_order_relaxed);
    }
}

void TRunStats::RecordReadBytes(ui32 actionIndex, ui64 bytes) {
    if (IsValidActionIndex(actionIndex)) {
        ActionStats_[actionIndex].ReadBytes.Value.fetch_add(bytes, std::memory_order_relaxed);
    }
}

void TRunStats::RecordWriteBytes(ui32 actionIndex, ui64 bytes) {
    if (IsValidActionIndex(actionIndex)) {
        ActionStats_[actionIndex].WriteBytes.Value.fetch_add(bytes, std::memory_order_relaxed);
    }
}

void TRunStats::RecordLatency(ui32 actionIndex, ui64 latencyMs) {
    RecordLatencySample(TotalLatency_, latencyMs);
    if (IsValidActionIndex(actionIndex)) {
        RecordLatencySample(ActionStats_[actionIndex].Latency, latencyMs);
    }
}

void TRunStats::RecordError(const TString& kind, const TString& message, std::optional<ui32> actionIndex) {
    (void)kind;
    (void)message;
    TotalErrors_.Value.fetch_add(1, std::memory_order_relaxed);
    if (actionIndex && IsValidActionIndex(*actionIndex)) {
        ActionStats_[*actionIndex].Errors.Value.fetch_add(1, std::memory_order_relaxed);
    }
}

ui64 TRunStats::GetTotalErrors() const {
    return TotalErrors_.Value.load(std::memory_order_relaxed);
}

TRunStatsSnapshot TRunStats::Snapshot() const {
    TRunStatsSnapshot snapshot;
    snapshot.ActionNames = ActionNames_;
    snapshot.ActionRuns.resize(ActionCount_);
    snapshot.ErrorsByAction.resize(ActionCount_);
    snapshot.ReadBytesByAction.resize(ActionCount_);
    snapshot.WriteBytesByAction.resize(ActionCount_);
    snapshot.LatencyByAction.resize(ActionCount_);
    for (ui32 i = 0; i < ActionCount_; ++i) {
        const TAtomicActionStats& actionStats = ActionStats_[i];
        snapshot.ActionRuns[i] = actionStats.Runs.Value.load(std::memory_order_relaxed);
        snapshot.ErrorsByAction[i] = actionStats.Errors.Value.load(std::memory_order_relaxed);
        snapshot.ReadBytesByAction[i] = actionStats.ReadBytes.Value.load(std::memory_order_relaxed);
        snapshot.WriteBytesByAction[i] = actionStats.WriteBytes.Value.load(std::memory_order_relaxed);
        snapshot.LatencyByAction[i] = BuildPercentiles(BuildHistogramSnapshot(actionStats.Latency));
    }
    snapshot.TotalLatency = BuildPercentiles(BuildHistogramSnapshot(TotalLatency_));
    snapshot.TotalErrors = TotalErrors_.Value.load(std::memory_order_relaxed);
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

    TVector<std::pair<TString, ui64>> sortedActionErrors;
    sortedActionErrors.reserve(snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedActionErrors.push_back(
            {snapshot.ActionNames[i], i < snapshot.ErrorsByAction.size() ? snapshot.ErrorsByAction[i] : 0});
    }
    std::sort(sortedActionErrors.begin(), sortedActionErrors.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, ui64>> sortedActionReadBytes;
    sortedActionReadBytes.reserve(snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedActionReadBytes.push_back(
            {snapshot.ActionNames[i], i < snapshot.ReadBytesByAction.size() ? snapshot.ReadBytesByAction[i] : 0});
    }
    std::sort(sortedActionReadBytes.begin(), sortedActionReadBytes.end(), [](const auto& l, const auto& r) {
        return l.first < r.first;
    });

    TVector<std::pair<TString, ui64>> sortedActionWriteBytes;
    sortedActionWriteBytes.reserve(snapshot.ActionNames.size());
    for (size_t i = 0; i < snapshot.ActionNames.size(); ++i) {
        sortedActionWriteBytes.push_back(
            {snapshot.ActionNames[i], i < snapshot.WriteBytesByAction.size() ? snapshot.WriteBytesByAction[i] : 0});
    }
    std::sort(sortedActionWriteBytes.begin(), sortedActionWriteBytes.end(), [](const auto& l, const auto& r) {
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
    ui64 totalReadBytes = 0;
    for (const auto& [_, bytes] : sortedActionReadBytes) {
        totalReadBytes += bytes;
    }
    ui64 totalWriteBytes = 0;
    for (const auto& [_, bytes] : sortedActionWriteBytes) {
        totalWriteBytes += bytes;
    }

    const double safeElapsedSeconds = elapsedSeconds > 0.0 ? elapsedSeconds : 0.0;
    const double totalOpsPerSecond = safeElapsedSeconds > 0.0
        ? static_cast<double>(totalActions) / safeElapsedSeconds
        : 0.0;
    const double totalReadBandwidth = safeElapsedSeconds > 0.0
        ? static_cast<double>(totalReadBytes) / safeElapsedSeconds
        : 0.0;
    const double totalWriteBandwidth = safeElapsedSeconds > 0.0
        ? static_cast<double>(totalWriteBytes) / safeElapsedSeconds
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
    Cout << "Average bandwidth: "
         << "read=" << FormatRate(totalReadBandwidth)
         << ", write=" << FormatRate(totalWriteBandwidth)
         << ", total=" << FormatRate(totalReadBandwidth + totalWriteBandwidth)
         << Endl;
    Cout << "Action runs:" << Endl;
    for (size_t i = 0; i < sortedActions.size(); ++i) {
        const auto& [name, count] = sortedActions[i];
        const ui64 readBytes = i < sortedActionReadBytes.size() ? sortedActionReadBytes[i].second : 0;
        const ui64 writeBytes = i < sortedActionWriteBytes.size() ? sortedActionWriteBytes[i].second : 0;
        const double actionOpsPerSecond = safeElapsedSeconds > 0.0
            ? static_cast<double>(count) / safeElapsedSeconds
            : 0.0;
        const double actionReadBandwidth = safeElapsedSeconds > 0.0
            ? static_cast<double>(readBytes) / safeElapsedSeconds
            : 0.0;
        const double actionWriteBandwidth = safeElapsedSeconds > 0.0
            ? static_cast<double>(writeBytes) / safeElapsedSeconds
            : 0.0;
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << actionOpsPerSecond;

        Cout << "  " << name << ": " << count
             << " (" << ss.str() << " ops/s"
             << ", read=" << FormatRate(actionReadBandwidth)
             << ", write=" << FormatRate(actionWriteBandwidth)
             << ")" << Endl;
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

    Cout << "Total errors: " << snapshot.TotalErrors << Endl;
}

} // namespace NKvVolumeStress
