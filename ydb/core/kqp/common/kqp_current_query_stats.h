#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

constexpr TDuration CurrentQueryStatsReportInterval = TDuration::Seconds(30);

// CPU includes compute and reported storage CPU; memory is compute quota without channel quota.
// Table/source bytes may overlap and do not distinguish local storage from S3.
struct TCurrentExecStats {
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
    ui64 ComputeMemoryBytes = 0;
    ui64 TableReadBytes = 0;
    ui64 ReadIngressBytes = 0;
    ui64 ObservedPeakComputeMemoryBytes = 0;
};

struct TCurrentExecStatsReport {
    TCurrentExecStats Stats;
    ui64 SequenceNo = 0;
};

class TCurrentQueryStats {
public:
    struct TSnapshot {
        ui64 CpuTimeUs = 0;
        ui64 ComputeMemoryBytes = 0;
        ui64 TableReadBytes = 0;
        ui64 ReadIngressBytes = 0;
        std::optional<ui64> ReadIngressBytesRate;
        ui64 ObservedPeakComputeMemoryBytes = 0;
    };

    struct TSourceState {
        TCurrentExecStats Previous;
        ui64 SequenceNo = 0;
    };

    static TCurrentExecStats ToExecutionStats(const TSnapshot& snapshot) {
        return {
            .CpuTimeUs = snapshot.CpuTimeUs,
            .ComputeMemoryBytes = snapshot.ComputeMemoryBytes,
            .TableReadBytes = snapshot.TableReadBytes,
            .ReadIngressBytes = snapshot.ReadIngressBytes,
            .ObservedPeakComputeMemoryBytes = snapshot.ObservedPeakComputeMemoryBytes,
        };
    }

    bool Update(TSourceState& source, const TCurrentExecStatsReport& report) {
        if (report.SequenceNo <= source.SequenceNo) {
            return false;
        }
        source.SequenceNo = report.SequenceNo;
        UpdateSnapshot(source.Previous, report.Stats);
        return true;
    }

    bool Finish(TSourceState& source) {
        if (!source.SequenceNo) {
            return false;
        }
        if (source.Previous.ComputeMemoryBytes) {
            auto final = source.Previous;
            final.ComputeMemoryBytes = 0;
            UpdateSnapshot(source.Previous, final);
        }
        source = {};
        return true;
    }

    std::optional<TSnapshot> Get() const {
        return HasReports ? std::make_optional(Total) : std::nullopt;
    }

private:
    void UpdateSnapshot(TCurrentExecStats& previous, TCurrentExecStats current) {
        current.CpuTimeUs = std::max(current.CpuTimeUs, previous.CpuTimeUs);
        current.TableReadBytes = std::max(current.TableReadBytes, previous.TableReadBytes);
        current.ReadIngressBytes = std::max(current.ReadIngressBytes, previous.ReadIngressBytes);
        Total.CpuTimeUs += current.CpuTimeUs - previous.CpuTimeUs;
        Total.TableReadBytes += current.TableReadBytes - previous.TableReadBytes;
        Total.ReadIngressBytes += current.ReadIngressBytes - previous.ReadIngressBytes;
        Total.ComputeMemoryBytes -= previous.ComputeMemoryBytes;
        Total.ComputeMemoryBytes += current.ComputeMemoryBytes;
        Total.ObservedPeakComputeMemoryBytes = std::max({Total.ObservedPeakComputeMemoryBytes,
            Total.ComputeMemoryBytes, current.ObservedPeakComputeMemoryBytes});
        previous = current;
        HasReports = true;
    }

    TSnapshot Total;
    bool HasReports = false;
};

class TCurrentQueryStatsWindow {
public:
    struct TPublishResult {
        TCurrentQueryStats::TSnapshot Snapshot;
        bool ScheduleStaleCheck = false;
    };

    explicit TCurrentQueryStatsWindow(TMonotonic publishedAt)
        : LastPublishedAt(publishedAt)
    {}

    void MarkUpdated() {
        UpdatedSincePublish = true;
    }

    TPublishResult Publish(TCurrentQueryStats::TSnapshot snapshot, TMonotonic now) {
        const bool hasNewStats = UpdatedSincePublish;
        if (hasNewStats) {
            const auto elapsed = now - LastPublishedAt;
            if (elapsed != TDuration::Zero()) {
                snapshot.ReadIngressBytesRate = (snapshot.ReadIngressBytes - LastPublishedReadIngressBytes)
                    * TDuration::Seconds(1).MicroSeconds() / elapsed.MicroSeconds();
            }
        } else {
            snapshot.ReadIngressBytesRate.reset();
        }

        LastPublishedReadIngressBytes = snapshot.ReadIngressBytes;
        LastPublishedAt = now;
        UpdatedSincePublish = false;
        return {std::move(snapshot), hasNewStats};
    }

private:
    ui64 LastPublishedReadIngressBytes = 0;
    TMonotonic LastPublishedAt;
    bool UpdatedSincePublish = false;
};

} // namespace NKikimr::NKqp
