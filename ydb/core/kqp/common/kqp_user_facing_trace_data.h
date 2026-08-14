#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/string/cast.h>
#include <util/system/types.h>

#include <array>
#include <unordered_map>
#include <vector>

namespace NKikimr::NKqp {

// Data recorded by the executer and rendered by the session after query completion.

constexpr size_t MaxUserFacingShardReadsPerTask = 64;

// Avoid retaining the full compute actor stats proto for every sampled task.
struct TUserFacingTaskSnapshot {
    ui64 TaskId = 0;
    ui32 NodeId = 0;
    ui64 CreateTimeMs = 0; // absolute epoch ms, as reported by the compute actor
    ui64 StartTimeMs = 0;
    ui64 FinishTimeMs = 0;
    ui64 ComputeCpuTimeUs = 0;
    ui64 BuildCpuTimeUs = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 WaitInputTimeUs = 0;
    ui64 WaitOutputTimeUs = 0;
    ui64 SpilledBytes = 0;
    ui32 ReadRetries = 0;
    std::vector<NKqpProto::TKqpShardReadStats> ShardReads;
    ui32 ShardReadsTruncated = 0;

    ui64 DurationMs() const {
        return StartTimeMs && FinishTimeMs >= StartTimeMs ? FinishTimeMs - StartTimeMs : 0;
    }
};

inline TUserFacingTaskSnapshot MakeUserFacingTaskSnapshot(const NYql::NDqProto::TDqTaskStats& task) {
    TUserFacingTaskSnapshot s;
    s.TaskId = task.GetTaskId();
    s.NodeId = task.GetNodeId();
    s.CreateTimeMs = task.GetCreateTimeMs();
    s.StartTimeMs = task.GetStartTimeMs();
    s.FinishTimeMs = task.GetFinishTimeMs();
    s.ComputeCpuTimeUs = task.GetComputeCpuTimeUs();
    s.BuildCpuTimeUs = task.GetBuildCpuTimeUs();
    s.InputRows = task.GetInputRows();
    s.OutputRows = task.GetOutputRows();
    s.WaitInputTimeUs = task.GetWaitInputTimeUs();
    s.WaitOutputTimeUs = task.GetWaitOutputTimeUs();
    s.SpilledBytes = task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();
    if (task.HasExtra()) {
        NKqpProto::TKqpTaskExtraStats extra;
        if (task.GetExtra().UnpackTo(&extra)) {
            s.ReadRetries = extra.GetReadRetriesCount() + extra.GetScanTaskExtraStats().GetRetriesCount();
            const size_t count = Min<size_t>(extra.GetShardReads().size(), MaxUserFacingShardReadsPerTask);
            s.ShardReads.assign(extra.GetShardReads().begin(), extra.GetShardReads().begin() + count);
            s.ShardReadsTruncated = extra.GetShardReadsTruncated()
                + extra.GetShardReads().size() - count;
        }
    }
    for (const auto& source : task.GetSources()) {
        for (const auto& partition : source.GetExternalPartitions()) {
            ui64 shardId = 0;
            if (!TryFromString(partition.GetPartitionId(), shardId)) {
                continue;
            }
            if (s.ShardReads.size() >= MaxUserFacingShardReadsPerTask) {
                ++s.ShardReadsTruncated;
                continue;
            }
            auto& shard = s.ShardReads.emplace_back();
            shard.SetShardId(shardId);
            shard.SetStartTimeMs(partition.GetFirstMessageMs());
            shard.SetFinishTimeMs(partition.GetLastMessageMs());
            shard.SetRows(partition.GetExternalRows());
            shard.SetTiming(NKqpProto::TKqpShardReadStats::FIRST_TO_LAST_MESSAGE);
        }
    }
    return s;
}

// Kept separate from exported stats so tracing does not change the query plan.
using TUserFacingTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, TUserFacingTaskSnapshot>>;

constexpr size_t MaxUserFacingTraceTasksPerStage = 128;

class TUserFacingShardReadCollector {
public:
    void OnStart(ui64 shardId) {
        auto it = Reads.find(shardId);
        if (it == Reads.end()) {
            if (Reads.size() >= MaxUserFacingShardReadsPerTask) {
                ++Dropped;
                return;
            }
            it = Reads.emplace(shardId, NKqpProto::TKqpShardReadStats{}).first;
        }
        auto& shard = it->second;
        shard.SetShardId(shardId);
        if (!shard.GetStartTimeMs()) {
            shard.SetStartTimeMs(TInstant::Now().MilliSeconds());
        }
    }

    void OnFinish(ui64 shardId, ui64 rows, ui32 retries, ui32 nodeId = 0,
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            bool finished = true) {
        auto it = Reads.find(shardId);
        if (it == Reads.end()) {
            return;
        }
        auto& shard = it->second;
        shard.SetShardId(shardId);
        shard.SetFinishTimeMs(TInstant::Now().MilliSeconds());
        shard.SetRows(shard.GetRows() + rows);
        shard.SetRetries(Max(shard.GetRetries(), retries));
        shard.SetStatus(status);
        shard.SetFinished(finished || status != Ydb::StatusIds::SUCCESS);
        if (nodeId) {
            shard.SetNodeId(nodeId);
        }
    }

    bool Empty() const {
        return Reads.empty();
    }

    void OnError(Ydb::StatusIds::StatusCode status) {
        const ui64 nowMs = TInstant::Now().MilliSeconds();
        for (auto& [shardId, shard] : Reads) {
            Y_UNUSED(shardId);
            if (!shard.GetFinished()) {
                shard.SetFinishTimeMs(nowMs);
                shard.SetStatus(status);
                shard.SetFinished(true);
            }
        }
    }

    void Export(NKqpProto::TKqpTaskExtraStats& extraStats, ui32 totalRetries) const {
        if (totalRetries) {
            extraStats.SetReadRetriesCount(extraStats.GetReadRetriesCount() + totalRetries);
        }
        size_t exported = 0;
        for (const auto& [shardId, shard] : Reads) {
            if (static_cast<size_t>(extraStats.ShardReadsSize()) >= MaxUserFacingShardReadsPerTask) {
                break;
            }
            *extraStats.AddShardReads() = shard;
            ++exported;
        }
        if (exported < Reads.size()) {
            extraStats.SetShardReadsTruncated(
                extraStats.GetShardReadsTruncated() + Reads.size() - exported);
        }
        if (Dropped > 0) {
            extraStats.SetShardReadsTruncated(extraStats.GetShardReadsTruncated() + Dropped);
        }
    }

private:
    std::unordered_map<ui64, NKqpProto::TKqpShardReadStats> Reads;
    size_t Dropped = 0;
};

// Tasks compete globally by duration for the budget left after phases and stages.
constexpr size_t MaxUserFacingSpansPerQuery = 5000;
constexpr size_t MaxUserFacingCommitShards = 64;

enum class EUserFacingTracePhase : size_t {
    ResolveTables = 0,
    ResolveShards,
    Snapshot,
    RunTasks,
    Commit,
    // Resolve windows may overlap because their requests run concurrently.
    ResolveMetadata,
    ResolvePartitioning,
    CommitPrepareShards,
    CommitCoordinator,
    CommitApplyShards,
    Count,
};

struct TUserFacingTraceTimeline {
    struct TWindow {
        TInstant Start;
        TInstant End;

        explicit operator bool() const {
            return Start != TInstant::Zero() && End > Start;
        }
    };

    TWindow Execute;
    std::array<TWindow, static_cast<size_t>(EUserFacingTracePhase::Count)> Phases;

    TWindow& Phase(EUserFacingTracePhase phase) {
        return Phases[static_cast<size_t>(phase)];
    }
    const TWindow& Phase(EUserFacingTracePhase phase) const {
        return Phases[static_cast<size_t>(phase)];
    }
};

inline TUserFacingTraceTimeline::TWindow FitUserFacingRemoteWindow(
        TUserFacingTraceTimeline::TWindow window,
        const TUserFacingTraceTimeline::TWindow& parent) {
    if (window.Start == TInstant::Zero() || window.End < window.Start) {
        return {};
    }
    if (window.End == window.Start) {
        window.End += TDuration::MicroSeconds(1);
    }
    if (!parent) {
        return window;
    }
    const TDuration duration = window.End - window.Start;
    const TDuration parentDuration = parent.End - parent.Start;
    if (duration >= parentDuration) {
        return parent;
    }
    if (window.Start < parent.Start) {
        window.Start = parent.Start;
        window.End = window.Start + duration;
    } else if (window.End > parent.End) {
        window.End = parent.End;
        window.Start = window.End - duration;
    }
    return window;
}

struct TUserFacingShardCommitAck {
    ui64 ShardId = 0;
    TInstant PreparedAt;
    TInstant CommittedAt;
};

struct TUserFacingStageAgg {
    std::unordered_map<ui32, ui32> TasksByNode;
    ui64 MinDurationMs = Max<ui64>();
    ui32 MinDurationNode = 0;
    ui64 MaxDurationMs = 0;
    ui32 MaxDurationNode = 0;
};

struct TUserFacingBufferLookupStats {
    std::vector<NKqpProto::TKqpShardReadStats> ShardReads;
    ui32 ShardReadsTruncated = 0;
};

// Sink-write stage stats do not contain a table path, so the executer captures it separately.
struct TUserFacingStageHint {
    TString TablePath;
    bool IsWrite = false;
};

struct TUserFacingTraceExecutionData {
    TString ExecuterActorType;
    TString ComputeActorType;
    TUserFacingTraceTimeline Timeline;
    TUserFacingTraceTaskStats TaskStats;
    std::unordered_map<ui32, TUserFacingStageHint> StageHints;
    std::unordered_map<ui32, TUserFacingStageAgg> StageAggs;
    TUserFacingBufferLookupStats BufferLookup;
    std::vector<TUserFacingShardCommitAck> ShardCommitAcks;
    size_t ShardCommitAcksTruncated = 0;
    // Unlike response stats, this snapshot is exported at the tracing collection depth.
    NYql::NDqProto::TDqExecutionStats ExecStats;
};

} // namespace NKikimr::NKqp
