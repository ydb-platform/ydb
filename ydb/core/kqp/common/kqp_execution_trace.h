#pragma once

#include "kqp_runtime_diagnostics.h"

#include <util/generic/string.h>

#include <array>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {

constexpr size_t MaxInterestingTasksPerStage = 5;
constexpr size_t MaxInterestingShardsPerTask = 5;
constexpr size_t MaxStageNodeDiagnostics = 32;
constexpr size_t MaxExecutionTraceSnapshots = 16;
constexpr size_t MaxStageTraceSnapshotsPerQuery = 128;
constexpr size_t MaxTaskTraceSnapshotsPerQuery = 160;
constexpr size_t MaxShardTraceSnapshotsPerQuery = 160;
constexpr size_t MaxStageNodeDiagnosticsPerQuery = 128;
constexpr size_t MaxBufferLookupDiagnosticsPerQuery = 32;
constexpr size_t MaxCommitShardDiagnosticsPerQuery = 32;

struct TExecutionDiagnosticsPolicy {
    bool CollectTimeline = false;
    bool CollectStageAggregates = false;
    bool CollectTaskSamples = false;
    bool CollectShardSamples = false;
    bool CollectBufferLookup = false;
    bool CollectCommitTimeline = false;
    size_t MaxExecutions = MaxExecutionTraceSnapshots;

    explicit operator bool() const {
        return CollectTimeline || CollectStageAggregates || CollectTaskSamples
            || CollectShardSamples || CollectBufferLookup || CollectCommitTimeline;
    }
};

enum class EExecutionPhase : size_t {
    ResolveTables = 0,
    ResolveShards,
    Snapshot,
    RunTasks,
    Commit,
    ResolveMetadata,
    ResolvePartitioning,
    Count,
};

struct TExecutionTimeline {
    TTimeWindow Execute;
    std::array<TTimeWindow, static_cast<size_t>(EExecutionPhase::Count)> Phases;

    TTimeWindow& Phase(EExecutionPhase phase) {
        return Phases[static_cast<size_t>(phase)];
    }

    const TTimeWindow& Phase(EExecutionPhase phase) const {
        return Phases[static_cast<size_t>(phase)];
    }
};

enum class EStageOperation : ui8 {
    Compute,
    Read,
    Write,
    Join,
    Aggregate,
    Filter,
};

struct TTaskTraceSnapshot {
    ui64 TaskId = 0;
    ui32 NodeId = 0;
    TTimeWindow Window;
    ui64 QueueDelayUs = 0;
    ui64 ComputeCpuUs = 0;
    ui64 BuildCpuUs = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    ui32 ReadRetries = 0;
    bool Failed = false;
    std::vector<NKqpProto::TKqpShardReadStats> Shards;
    ui32 ShardsTruncated = 0;

    ui64 DurationUs() const {
        return Window ? (Window.End - Window.Start).MicroSeconds() : 0;
    }

    bool HasAnomaly() const {
        return Failed || ReadRetries > 0 || SpilledBytes > 0
            || (DurationUs() > 0 && WaitUs * 2 >= DurationUs());
    }
};

struct TTaskDurationSummary {
    ui64 MinUs = 0;
    ui64 MaxUs = 0;
    ui64 SumUs = 0;
    ui64 Count = 0;
};

struct TStageTraceSnapshot {
    ui32 StageId = 0;
    TString TablePath;
    EStageOperation Operation = EStageOperation::Compute;
    TTimeWindow Window;
    ui64 Tasks = 0;
    ui64 FailedTasks = 0;
    ui64 CpuUs = 0;
    ui64 InputRows = 0;
    ui64 OutputRows = 0;
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    TTaskDurationSummary Durations;
    std::vector<std::pair<ui32, ui32>> TasksByNode;
    ui32 SlowestTaskNode = 0;
    ui32 FastestTaskNode = 0;
    size_t NodesTruncated = 0;
    std::vector<TTaskTraceSnapshot> InterestingTasks;
};

struct TBufferLookupDiagnostics {
    std::vector<NKqpProto::TKqpShardReadStats> Shards;
    ui32 ShardsTruncated = 0;
};

struct TExecutionTraceSnapshot {
    TString ExecuterActorType;
    TString ComputeActorType;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TExecutionTimeline Timeline;
    ui64 CpuUs = 0;
    std::vector<TStageTraceSnapshot> Stages;
    size_t StagesTruncated = 0;
    TBufferLookupDiagnostics BufferLookup;
    TCommitDiagnostics Commit;
};

void TrimExecutionTraceSnapshot(TExecutionTraceSnapshot& snapshot);

void TrimExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& snapshots);

void AppendExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& target,
    size_t& dropped, std::vector<TExecutionTraceSnapshot>& source,
    size_t sourceDropped = 0, size_t limit = MaxExecutionTraceSnapshots);

} // namespace NKikimr::NKqp
