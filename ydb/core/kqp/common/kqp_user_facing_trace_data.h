#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/system/types.h>

#include <array>
#include <unordered_map>
#include <vector>

namespace NKikimr::NKqp {

// Data recorded by the executer and rendered by the session after query completion.

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
            s.ShardReads.assign(extra.GetShardReads().begin(), extra.GetShardReads().end());
            s.ShardReadsTruncated = extra.GetShardReadsTruncated();
        }
    }
    return s;
}

// Kept separate from exported stats so tracing does not change the query plan.
using TUserFacingTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, TUserFacingTaskSnapshot>>;

constexpr size_t MaxUserFacingTraceTasksPerStage = 128;

constexpr size_t MaxUserFacingShardReadsPerTask = 64;

class TUserFacingShardReadCollector {
public:
    void OnStart(ui64 shardId) {
        auto& shard = Reads[shardId];
        shard.SetShardId(shardId);
        if (!shard.GetStartTimeMs()) {
            shard.SetStartTimeMs(TInstant::Now().MilliSeconds());
        }
    }

    void OnFinish(ui64 shardId, ui64 rows, ui32 retries, ui32 nodeId = 0) {
        auto& shard = Reads[shardId];
        shard.SetShardId(shardId);
        shard.SetFinishTimeMs(TInstant::Now().MilliSeconds());
        shard.SetRows(shard.GetRows() + rows);
        shard.SetRetries(Max(shard.GetRetries(), retries));
        if (nodeId) {
            shard.SetNodeId(nodeId);
        }
    }

    bool Empty() const {
        return Reads.empty();
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
    }

private:
    std::unordered_map<ui64, NKqpProto::TKqpShardReadStats> Reads;
};

// Tasks compete globally by duration for the budget left after phases and stages.
constexpr size_t MaxUserFacingSpansPerQuery = 5000;

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

// Sink-write stage stats do not contain a table path, so the executer captures it separately.
struct TUserFacingStageHint {
    TString TablePath;
    bool IsWrite = false;
};

struct TUserFacingTraceExecutionData {
    TUserFacingTraceTimeline Timeline;
    TUserFacingTraceTaskStats TaskStats;
    std::unordered_map<ui32, TUserFacingStageHint> StageHints;
    std::unordered_map<ui32, TUserFacingStageAgg> StageAggs;
    std::vector<TUserFacingShardCommitAck> ShardCommitAcks;
    // Unlike response stats, this snapshot is exported at the tracing collection depth.
    NYql::NDqProto::TDqExecutionStats ExecStats;
};

} // namespace NKikimr::NKqp
