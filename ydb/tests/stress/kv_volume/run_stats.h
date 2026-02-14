#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <mutex>

namespace NKvVolumeStress {

struct TLatencyPercentiles {
    ui64 Samples = 0;
    ui64 P50Ms = 0;
    ui64 P90Ms = 0;
    ui64 P99Ms = 0;
    ui64 P100Ms = 0;
};

struct TRunStatsSnapshot {
    THashMap<TString, ui64> ActionRuns;
    THashMap<TString, ui64> ErrorsByKind;
    THashMap<TString, ui64> ErrorsByAction;
    THashMap<TString, ui64> ReadBytesByAction;
    THashMap<TString, ui64> WriteBytesByAction;
    THashMap<TString, TLatencyPercentiles> LatencyByKind;
    TLatencyPercentiles TotalLatency;
    TVector<TString> SampleErrors;
    ui64 TotalErrors = 0;
};

class TRunStats {
public:
    TRunStats();

    void RecordAction(const TString& actionName);
    void RecordReadBytes(const TString& actionName, ui64 bytes);
    void RecordWriteBytes(const TString& actionName, ui64 bytes);
    void RecordLatency(const TString& kind, ui64 latencyMs);
    void RecordError(const TString& kind, const TString& message, const TString& actionName = TString());
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

    static size_t FindLatencyBucket(ui64 latencyMs);
    static void RecordLatencySample(TLatencyHistogram& histogram, ui64 latencyMs);
    static TLatencyPercentiles BuildPercentiles(const TLatencyHistogram& histogram);

private:
    mutable std::mutex Mutex_;
    THashMap<TString, ui64> ActionRuns_;
    THashMap<TString, ui64> ErrorsByKind_;
    THashMap<TString, ui64> ErrorsByAction_;
    THashMap<TString, ui64> ReadBytesByAction_;
    THashMap<TString, ui64> WriteBytesByAction_;
    THashMap<TString, TLatencyHistogram> LatencyByKind_;
    TLatencyHistogram TotalLatency_;
    TVector<TString> SampleErrors_;
    ui64 TotalErrors_ = 0;
};

} // namespace NKvVolumeStress
