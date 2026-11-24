#pragma once

#include "workload.h"

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/json/writer/json_value.h>

#include <map>
#include <string>

namespace NDataShardReadLoad {

struct TStats {
    using TJson = NJson::TJsonValue;

    uint64_t Scheduled = 0;
    uint64_t Launched = 0;
    uint64_t SkippedScheduler = 0;
    uint64_t SkippedCapacity = 0;
    uint64_t Completed = 0;
    uint64_t SuccessfulRows = 0;
    uint64_t RowMismatches = 0;
    uint64_t SessionFailures = 0;
    uint64_t SessionReplacements = 0;
    std::map<std::string, uint64_t> Statuses;
    NHdr::THistogram Latency{1000000000000LL, 3};
    NHdr::THistogram SuccessLatency{1000000000000LL, 3};
    NHdr::THistogram SchedulerDelay{1000000000000LL, 3};

    void Add(const TStats& other) {
        Scheduled += other.Scheduled;
        Launched += other.Launched;
        SkippedScheduler += other.SkippedScheduler;
        SkippedCapacity += other.SkippedCapacity;
        Completed += other.Completed;
        SuccessfulRows += other.SuccessfulRows;
        RowMismatches += other.RowMismatches;
        SessionFailures += other.SessionFailures;
        SessionReplacements += other.SessionReplacements;
        for (const auto& [status, count] : other.Statuses) {
            Statuses[status] += count;
        }
        Latency.Add(other.Latency);
        SuccessLatency.Add(other.SuccessLatency);
        SchedulerDelay.Add(other.SchedulerDelay);
    }

    static TJson Histogram(const NHdr::THistogram& histogram) {
        TJson json(NJson::JSON_MAP);
        json["p50"] = histogram.GetValueAtPercentile(50);
        json["p95"] = histogram.GetValueAtPercentile(95);
        json["p99"] = histogram.GetValueAtPercentile(99);
        json["max"] = histogram.GetMax();
        return json;
    }

    TJson Json(bool cumulative) const {
        TJson json(NJson::JSON_MAP);
        json["scheduled"] = Scheduled;
        json["launched"] = Launched;
        json["skipped_scheduler"] = SkippedScheduler;
        json["skipped_capacity"] = SkippedCapacity;
        json["completed"] = Completed;
        json["successful_rows"] = SuccessfulRows;
        json["row_mismatches"] = RowMismatches;
        json["session_failures"] = SessionFailures;
        json["session_replacements"] = SessionReplacements;
        if (cumulative) {
            json["outstanding"] = Launched - Completed;
        }
        json["statuses"].SetType(NJson::JSON_MAP);
        for (const auto& [status, count] : Statuses) {
            json["statuses"][status] = count;
        }
        json["latency_us"] = Histogram(Latency);
        json["success_latency_us"] = Histogram(SuccessLatency);
        json["scheduler_delay_us"] = Histogram(SchedulerDelay);
        return json;
    }
};

using TPopulationStats = std::array<TStats, PopulationCount>;


} // namespace NDataShardReadLoad
