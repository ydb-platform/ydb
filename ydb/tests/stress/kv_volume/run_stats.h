#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <util/generic/vector.h>

namespace NKvVolumeStress {

struct TLatencyPercentiles {
    ui64 Samples = 0;
    ui64 P50Ms = 0;
    ui64 P90Ms = 0;
    ui64 P99Ms = 0;
    ui64 P100Ms = 0;
};

struct TNamedCounter {
    TString Name;
    ui64 Total = 0;
};

struct TRunStatsSnapshot {
    TVector<TString> ActionNames;
    TVector<ui64> ActionRuns;
    TVector<ui64> ErrorsByAction;
    TVector<ui64> ReadBytesByAction;
    TVector<ui64> WriteBytesByAction;
    TVector<TLatencyPercentiles> LatencyByAction;
    TVector<TNamedCounter> ErrorsByKind;
    TLatencyPercentiles TotalLatency;
    TVector<TString> SampleErrors;
    ui64 TotalErrors = 0;
};

class TRunStats {
public:
    explicit TRunStats(TVector<TString> actionNames = {});

    void RecordAction(ui32 actionIndex);
    void RecordReadBytes(ui32 actionIndex, ui64 bytes);
    void RecordWriteBytes(ui32 actionIndex, ui64 bytes);
    void RecordLatency(ui32 actionIndex, ui64 latencyMs);
    void RecordError(const TString& kind, const TString& message, std::optional<ui32> actionIndex = std::nullopt);
    ui64 GetTotalErrors() const;
    TRunStatsSnapshot Snapshot() const;
    void PrintSummary(double elapsedSeconds) const;

private:
    static constexpr size_t LatencyBucketCount = 49; // last bucket is +inf

    struct TLatencyHistogram {
        std::array<ui64, LatencyBucketCount> Buckets = {};
        ui64 Samples = 0;
        ui64 MaxMs = 0;
    };

    struct TAtomicLatencyHistogram {
        TAtomicLatencyHistogram();

        std::array<std::atomic<ui64>, LatencyBucketCount> Buckets;
        std::atomic<ui64> MaxMs = 0;
    };

    struct TAtomicActionStats {
        TAtomicActionStats();

        std::atomic<ui64> Runs = 0;
        std::atomic<ui64> ReadBytes = 0;
        std::atomic<ui64> WriteBytes = 0;
        TAtomicLatencyHistogram Latency;
    };

    static size_t FindLatencyBucket(ui64 latencyMs);
    static void RecordLatencySample(TAtomicLatencyHistogram& histogram, ui64 latencyMs);
    static TLatencyHistogram BuildHistogramSnapshot(const TAtomicLatencyHistogram& histogram);
    static TLatencyPercentiles BuildPercentiles(const TLatencyHistogram& histogram);
    static void IncrementNamedCounter(TVector<TNamedCounter>& counters, const TString& name);
    static void UpdateMax(std::atomic<ui64>& maxValue, ui64 candidate);

    bool IsValidActionIndex(ui32 actionIndex) const {
        return actionIndex < ActionCount_;
    }

private:
    const ui32 ActionCount_ = 0;
    TVector<TString> ActionNames_;

    std::unique_ptr<TAtomicActionStats[]> ActionStats_;
    TAtomicLatencyHistogram TotalLatency_;

    mutable std::mutex ErrorMutex_;
    TVector<TNamedCounter> ErrorsByKind_;
    TVector<ui64> ActionErrors_;
    TVector<TString> SampleErrors_;
    ui64 TotalErrors_ = 0;
};

} // namespace NKvVolumeStress
