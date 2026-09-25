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
    using TSnapshot = TCurrentQueryResources;

    struct TPublishedSnapshot : TSnapshot {
        // Average ingress throughput since the previous published snapshot.
        // Empty when there was no fresh execution report in that interval.
        std::optional<ui64> ReadIngressBytesRate;
    };

    struct TSourceState {
        TSnapshot Previous;
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

    std::optional<TSnapshot> Get() const {
        return HasReports ? std::make_optional(Total) : std::nullopt;
    }

private:
    void UpdateSnapshot(TSnapshot& previous, TSnapshot current) {
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

    TSnapshot Total;
    bool HasReports = false;
};

class TCurrentQueryStatsWindow {
public:
    struct TPublishResult {
        TCurrentQueryStats::TPublishedSnapshot Snapshot;
        bool ScheduleStaleCheck = false;
    };

    explicit TCurrentQueryStatsWindow(TMonotonic publishedAt)
        : LastPublishedAt(publishedAt)
    {}

    void MarkUpdated() {
        UpdatedSincePublish = true;
    }

    TPublishResult Publish(TCurrentQueryStats::TSnapshot snapshot, TMonotonic now) {
        TCurrentQueryStats::TPublishedSnapshot published;
        static_cast<TCurrentQueryStats::TSnapshot&>(published) = snapshot;
        const bool hasNewStats = UpdatedSincePublish;
        if (hasNewStats) {
            const auto elapsed = now - LastPublishedAt;
            if (elapsed != TDuration::Zero()) {
                published.ReadIngressBytesRate = (snapshot.ReadIngressBytes - LastPublishedReadIngressBytes)
                    * TDuration::Seconds(1).MicroSeconds() / elapsed.MicroSeconds();
            }
        }

        LastPublishedReadIngressBytes = snapshot.ReadIngressBytes;
        LastPublishedAt = now;
        UpdatedSincePublish = false;
        return {std::move(published), hasNewStats};
    }

private:
    ui64 LastPublishedReadIngressBytes = 0;
    TMonotonic LastPublishedAt;
    bool UpdatedSincePublish = false;
};

class TCurrentQueryStatsPublisher {
public:
    struct TPublication {
        TCurrentQueryStats::TPublishedSnapshot Stats;
        ui64 SequenceNo = 0;
        bool ScheduleStaleCheck = false;
    };

    TCurrentQueryStatsPublisher(TMonotonic startedAt, TDuration interval)
        : Window(startedAt)
        , Interval(interval)
    {}

    bool Update(const TCurrentExecStatsReport& report) {
        if (!Stats.Update(Source, report)) {
            return false;
        }
        Window.MarkUpdated();
        return true;
    }

    bool Finish() {
        if (!Stats.Finish(Source)) {
            return false;
        }
        Window.MarkUpdated();
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

        auto published = Window.Publish(*current, now);
        PublishScheduled = published.ScheduleStaleCheck;
        return TPublication{std::move(published.Snapshot), ++SequenceNo, published.ScheduleStaleCheck};
    }

    TDuration GetInterval() const {
        return Interval;
    }

private:
    TCurrentQueryStats Stats;
    TCurrentQueryStats::TSourceState Source;
    TCurrentQueryStatsWindow Window;
    TDuration Interval;
    ui64 SequenceNo = 0;
    bool PublishScheduled = false;
};

} // namespace NKikimr::NKqp
