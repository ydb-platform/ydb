#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

constexpr TDuration CurrentQueryStatsReportInterval = TDuration::Seconds(30);

struct TCurrentQueryResources {
    ui64 CpuTimeUs = 0;
    // Current compute-task quota; not RSS or peak memory.
    ui64 ComputeMemoryBytes = 0;
    // Bytes received by DQ compute tasks from all input sources.
    ui64 ReadIngressBytes = 0;
    // Highest compute-task quota observed during this execution.
    ui64 ObservedPeakComputeMemoryBytes = 0;
};

struct TCurrentExecStatsReport {
    TCurrentQueryResources Stats;
    ui64 SequenceNo = 0;
};

class TCurrentQueryStats {
public:
    struct TPublishedSnapshot : TCurrentQueryResources {
        // Average ingress throughput since the previous published snapshot.
        // Empty when there was no fresh execution report in that interval.
        std::optional<ui64> ReadIngressBytesRate;
    };

    struct TSourceState {
        TCurrentQueryResources Previous;
        ui64 SequenceNo = 0;
    };

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

    std::optional<TCurrentQueryResources> Get() const {
        return HasReports ? std::make_optional(Total) : std::nullopt;
    }

private:
    void UpdateSnapshot(TCurrentQueryResources& previous, TCurrentQueryResources current) {
        current.CpuTimeUs = std::max(current.CpuTimeUs, previous.CpuTimeUs);
        current.ReadIngressBytes = std::max(current.ReadIngressBytes, previous.ReadIngressBytes);
        Total.CpuTimeUs += current.CpuTimeUs - previous.CpuTimeUs;
        Total.ReadIngressBytes += current.ReadIngressBytes - previous.ReadIngressBytes;
        Total.ComputeMemoryBytes -= previous.ComputeMemoryBytes;
        Total.ComputeMemoryBytes += current.ComputeMemoryBytes;
        Total.ObservedPeakComputeMemoryBytes = std::max({Total.ObservedPeakComputeMemoryBytes,
            Total.ComputeMemoryBytes, current.ObservedPeakComputeMemoryBytes});
        previous = current;
        HasReports = true;
    }

    TCurrentQueryResources Total;
    bool HasReports = false;
};

class TCurrentQueryStatsPublisher {
public:
    struct TPublication {
        TCurrentQueryStats::TPublishedSnapshot Stats;
        ui64 SequenceNo = 0;
        bool ScheduleNextPublish = false;
    };

    TCurrentQueryStatsPublisher(TMonotonic startedAt, TDuration interval)
        : LastPublishedAt(startedAt)
        , Interval(interval)
    {}

    bool Update(const TCurrentExecStatsReport& report) {
        if (!Stats.Update(Source, report)) {
            return false;
        }
        UpdatedSincePublish = true;
        return true;
    }

    bool Finish() {
        if (!Stats.Finish(Source)) {
            return false;
        }
        UpdatedSincePublish = true;
        return true;
    }

    bool SchedulePublish() {
        if (PublishScheduled) {
            return false;
        }
        PublishScheduled = true;
        return true;
    }

    std::optional<TPublication> Publish(TMonotonic now) {
        PublishScheduled = false;
        auto current = Stats.Get();
        if (!current) {
            return std::nullopt;
        }

        TCurrentQueryStats::TPublishedSnapshot published;
        static_cast<TCurrentQueryResources&>(published) = *current;
        if (UpdatedSincePublish) {
            const auto elapsed = now - LastPublishedAt;
            if (elapsed != TDuration::Zero()) {
                published.ReadIngressBytesRate = (current->ReadIngressBytes - LastPublishedReadIngressBytes)
                    * TDuration::Seconds(1).MicroSeconds() / elapsed.MicroSeconds();
            }
        }

        LastPublishedReadIngressBytes = current->ReadIngressBytes;
        LastPublishedAt = now;
        PublishScheduled = UpdatedSincePublish;
        UpdatedSincePublish = false;
        return TPublication{std::move(published), ++SequenceNo, PublishScheduled};
    }

    TDuration GetInterval() const {
        return Interval;
    }

private:
    TCurrentQueryStats Stats;
    TCurrentQueryStats::TSourceState Source;
    ui64 LastPublishedReadIngressBytes = 0;
    TMonotonic LastPublishedAt;
    bool UpdatedSincePublish = false;
    TDuration Interval;
    ui64 SequenceNo = 0;
    bool PublishScheduled = false;
};

} // namespace NKikimr::NKqp
