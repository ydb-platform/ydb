#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <unordered_map>

namespace NKikimr::NKqp {

// Source data for the user-facing trace of one execution. The executer never creates user-facing
// spans: it stamps plain timestamps here (and retains per-task stats), ships the struct to the
// session in the local TEvTxResponse, and the single renderer in kqp_user_facing_tracing.cpp
// builds the whole span tree at reply time.

// Compact per-task snapshot retained for the user-facing trace: only the fields the renderer
// reads (~100 bytes), NOT the full compute actor stats proto — at profile level that proto
// carries history samples and channels and costs KBs per task, which at 128 tasks x many
// stages would add up to tens of MB per sampled query in the executer.
struct TUserFacingTaskSnapshot {
    ui64 TaskId = 0;
    ui32 NodeId = 0;
    ui64 CreateTimeMs = 0; // absolute epoch ms, as reported by the compute actor
    ui64 StartTimeMs = 0;
    ui64 FinishTimeMs = 0;
    ui64 CpuTimeUs = 0;
    ui64 ComputeCpuTimeUs = 0;
    ui64 BuildCpuTimeUs = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 WaitInputTimeUs = 0;
    ui64 WaitOutputTimeUs = 0;
    ui64 SpilledBytes = 0;
    ui32 ReadRetries = 0;
    TVector<NKqpProto::TKqpShardReadStats> ShardReads; // profile level only; capped at source
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
    s.CpuTimeUs = task.GetCpuTimeUs();
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

// stageId -> taskId -> task snapshot. Deliberately separate from the exported stats proto —
// a trace-sampled query must produce the same plan as an unsampled one.
using TUserFacingTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, TUserFacingTaskSnapshot>>;

// Cap on retained tasks per stage (top-N by duration once full — stragglers matter most);
// bounds executer memory on wide OLAP stages, the stage span gets ydb.tasks_truncated when hit.
constexpr size_t MaxUserFacingTraceTasksPerStage = 128;

// Cap on per-shard read entries a task exports at profile level (first-come); the task span
// gets ydb.shards_truncated when hit.
constexpr size_t MaxUserFacingShardReadsPerTask = 64;

// Operational phases the executer passes through. The renderer decides presentation: which are
// grouped under "Prepare", and their user-facing names.
enum class EUserFacingTracePhase : size_t {
    ResolveTables = 0,
    ResolveShards,
    Snapshot,
    RunTasks,
    Commit, // effects commit/flush via the buffer actor (covers the coordinator round-trip)
    // Sub-windows below are reported by other actors and assigned directly, outside the
    // executer's Begin/End chain. The two resolve ones run concurrently and may overlap.
    ResolveMetadata,     // scheme cache navigate (table metadata); child of ResolveTables
    ResolvePartitioning, // scheme cache key-range resolve; child of ResolveTables
    CommitPrepareShards, // children of Commit; empty for immediate (single-shard) commit
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

// Per-shard commit acknowledgements of a distributed commit (profile level only).
struct TUserFacingShardCommitAck {
    ui64 ShardId = 0;
    TInstant PreparedAt;
    TInstant CommittedAt;
};

struct TUserFacingTraceExecutionData {
    TUserFacingTraceTimeline Timeline;
    TUserFacingTraceTaskStats TaskStats;
    TVector<TUserFacingShardCommitAck> ShardCommitAcks;
    // Stats exported at collection depth for the trace; the response's stats stay at the
    // client-requested mode and must not be used for rendering.
    NYql::NDqProto::TDqExecutionStats ExecStats;
};

} // namespace NKikimr::NKqp
