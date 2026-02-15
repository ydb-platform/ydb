#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>
#include <atomic>
#include <memory>
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

struct TRunStatsSnapshot {
    TVector<TString> ActionNames;
    TVector<ui64> ActionRuns;
    TVector<ui64> ErrorsByAction;
    TVector<ui64> ReadBytesByAction;
    TVector<ui64> WriteBytesByAction;
    TVector<TLatencyPercentiles> LatencyByAction;
    TLatencyPercentiles TotalLatency;
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
    static constexpr size_t CacheLineSize = 64;

    struct TLatencyHistogram {
        std::array<ui64, LatencyBucketCount> Buckets = {};
        ui64 Samples = 0;
        ui64 MaxMs = 0;
    };

    struct alignas(CacheLineSize) TPaddedAtomicUi64 {
        TPaddedAtomicUi64();

        std::atomic<ui64> Value = 0;
    };
    static_assert(alignof(TPaddedAtomicUi64) >= CacheLineSize);
    static_assert(sizeof(TPaddedAtomicUi64) % CacheLineSize == 0);

    struct TAtomicLatencyHistogram {
        TAtomicLatencyHistogram();

        std::array<TPaddedAtomicUi64, LatencyBucketCount> Buckets;
        TPaddedAtomicUi64 MaxMs;
    };

    struct alignas(CacheLineSize) TAtomicActionStats {
        TAtomicActionStats();

        TPaddedAtomicUi64 Runs;
        TPaddedAtomicUi64 Errors;
        TPaddedAtomicUi64 ReadBytes;
        TPaddedAtomicUi64 WriteBytes;
        TAtomicLatencyHistogram Latency;
    };
    static_assert(alignof(TAtomicActionStats) >= CacheLineSize);

    static size_t FindLatencyBucket(ui64 latencyMs);
    static void RecordLatencySample(TAtomicLatencyHistogram& histogram, ui64 latencyMs);
    static TLatencyHistogram BuildHistogramSnapshot(const TAtomicLatencyHistogram& histogram);
    static TLatencyPercentiles BuildPercentiles(const TLatencyHistogram& histogram);
    static void UpdateMax(TPaddedAtomicUi64& maxValue, ui64 candidate);

    bool IsValidActionIndex(ui32 actionIndex) const {
        return actionIndex < ActionCount_;
    }

private:
    const ui32 ActionCount_ = 0;
    TVector<TString> ActionNames_;

    std::unique_ptr<TAtomicActionStats[]> ActionStats_;
    TAtomicLatencyHistogram TotalLatency_;
    TPaddedAtomicUi64 TotalErrors_;
};

} // namespace NKvVolumeStress
