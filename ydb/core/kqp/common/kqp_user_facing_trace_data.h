#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
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
    ui64 ComputeCpuTimeUs = 0;
    ui64 BuildCpuTimeUs = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 WaitInputTimeUs = 0;
    ui64 WaitOutputTimeUs = 0;
    ui64 SpilledBytes = 0;
    ui32 ReadRetries = 0;
    TVector<NKqpProto::TKqpShardReadStats> ShardReads; // full-detail tier; capped at source
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

// stageId -> taskId -> task snapshot. Deliberately separate from the exported stats proto —
// a trace-sampled query must produce the same plan as an unsampled one.
using TUserFacingTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, TUserFacingTaskSnapshot>>;

// Cap on retained tasks per stage (top-N by duration once full — stragglers matter most);
// bounds executer memory on wide OLAP stages, the stage span gets ydb.tasks_truncated when hit.
constexpr size_t MaxUserFacingTraceTasksPerStage = 128;

// Cap on per-shard read entries a task exports at full stats level (first-come); the task span
// gets ydb.shards_truncated when hit.
constexpr size_t MaxUserFacingShardReadsPerTask = 64;

// Global budget of spans emitted per query: per-container caps alone still multiply into
// hundreds of thousands of spans on a huge OLAP query, which kills the uploader and trace UIs.
// Phases and stages always render (naturally small); tasks are admitted globally by duration
// (top-K across all stages of all executions), shard children consume what remains. The root
// span gets ydb.spans_truncated with the dropped count.
constexpr size_t MaxUserFacingSpansPerQuery = 5000;

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

// Per-shard commit acknowledgements of a distributed commit (full-detail tier only).
struct TUserFacingShardCommitAck {
    ui64 ShardId = 0;
    TInstant PreparedAt;
    TInstant CommittedAt;
};

// Per-stage parallelism aggregates for the user-facing trace, accumulated by the executer over
// final task reports: task placement across nodes and the nodes of the extreme-duration tasks
// (the fastest task is not retained by the top-N snapshot cap, so its node is recorded here).
struct TUserFacingStageAgg {
    THashMap<ui32, ui32> TasksByNode; // nodeId -> finished task count
    ui64 MinDurationMs = Max<ui64>();
    ui32 MinDurationNode = 0;
    ui64 MaxDurationMs = 0;
    ui32 MaxDurationNode = 0;
};

// Presentation hint for one stage, captured by the executer from the tasks graph: exported
// stage stats carry no table info for sink writes, so without it the renderer can only name
// such stages "Step N".
struct TUserFacingStageHint {
    TString TablePath;
    bool IsWrite = false;
};

struct TUserFacingTraceExecutionData {
    TUserFacingTraceTimeline Timeline;
    TUserFacingTraceTaskStats TaskStats;
    THashMap<ui32, TUserFacingStageHint> StageHints; // by exported stage id
    THashMap<ui32, TUserFacingStageAgg> StageAggs;   // by exported stage id
    TVector<TUserFacingShardCommitAck> ShardCommitAcks;
    // Stats exported at collection depth for the trace; the response's stats stay at the
    // client-requested mode and must not be used for rendering.
    NYql::NDqProto::TDqExecutionStats ExecStats;
};

} // namespace NKikimr::NKqp
