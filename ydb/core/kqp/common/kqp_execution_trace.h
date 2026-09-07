#pragma once

#include "kqp_runtime_diagnostics.h"

#include <util/generic/string.h>

#include <array>
#include <optional>
#include <utility>
#include <vector>

namespace NYql::NDqProto {
class TDqTaskStats;
}

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
    bool CollectStages = false;
    bool CollectShardSamples = false;
    bool CollectBufferLookup = false;
    bool CollectCommitTimeline = false;
    size_t MaxExecutions = MaxExecutionTraceSnapshots;

    explicit operator bool() const {
        return CollectTimeline || CollectStages
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
    FlushEffects,
    Rollback,
    Count,
};

struct TExecutionTimeline {
    TTimeWindow Execute;
    std::array<TPhaseDiagnostic, static_cast<size_t>(EExecutionPhase::Count)> Phases;

    TPhaseDiagnostic& Phase(EExecutionPhase phase) {
        return Phases[static_cast<size_t>(phase)];
    }

    const TPhaseDiagnostic& Phase(EExecutionPhase phase) const {
        return Phases[static_cast<size_t>(phase)];
    }
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

inline bool TaskDiagnosticsLess(const TTaskTraceSnapshot& lhs, const TTaskTraceSnapshot& rhs) {
    const auto rank = [](const TTaskTraceSnapshot& task) {
        return std::tuple(task.Failed, task.HasAnomaly(), task.SpilledBytes > 0,
            task.ReadRetries > 0, task.DurationUs());
    };
    return rank(lhs) < rank(rhs);
}

TTaskTraceSnapshot MakeTaskTraceSnapshot(const NYql::NDqProto::TDqTaskStats& task);

struct TTaskDurationSummary {
    ui64 MinUs = 0;
    ui64 MaxUs = 0;
    ui64 SumUs = 0;
    ui64 Count = 0;
};

struct TStageTraceSnapshot {
    ui32 StageId = 0;
    TString TablePath;
    bool HasJoins = false;
    bool HasAggregations = false;
    bool HasFilters = false;
    bool HasReads = false;
    bool HasWrites = false;
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

void KeepInterestingTask(TStageTraceSnapshot& stage, TTaskTraceSnapshot&& task);

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
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    double MaxTaskSkew = 0.0;
    std::vector<TStageTraceSnapshot> Stages;
    size_t StagesTruncated = 0;
    TBufferLookupDiagnostics BufferLookup;
    TCommitDiagnostics Commit;
};

// Exact query-wide scalars are accumulated before bounded diagnostic snapshots are retained.
struct TExecutionTraceTotals {
    ui64 CpuUs = 0;
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    double MaxTaskSkew = 0.0;
};

void AccumulateExecutionTraceTotals(TExecutionTraceTotals& totals,
    const TExecutionTraceSnapshot& snapshot);

void AccumulateExecutionTraceTotals(TExecutionTraceTotals& totals,
    const TExecutionTraceTotals& source);

// Owns optional execution-phase timing and assembles the terminal diagnostics snapshot.
class TExecutionDiagnosticsCapture {
public:
    TExecutionDiagnosticsCapture(TString executerActorType, TString computeActorType);

    void OnPhaseStarted(EExecutionPhase phase);

    void OnTableResolverFinished(const TTimeWindow& navigateWindow,
        const TTimeWindow& resolveKeysWindow, Ydb::StatusIds::StatusCode status);

    void SetCommitDiagnostics(TCommitDiagnostics diagnostics);

    TExecutionTraceSnapshot Finish(Ydb::StatusIds::StatusCode status);

private:
    void EndCurrentPhase(TInstant finishAt, Ydb::StatusIds::StatusCode status);

private:
    TExecutionTraceSnapshot Snapshot;
    EExecutionPhase CurrentPhase = EExecutionPhase::Count;
};

// Captures SchemeCache request envelopes without exposing timestamp bookkeeping to the resolver.
struct TTableResolverDiagnostics {
    TTimeWindow Navigate;
    TTimeWindow ResolveKeys;
};

class TTableResolverDiagnosticsCapture {
public:
    void OnNavigateStarted();
    void OnNavigateFinished();
    void OnResolveKeysStarted();
    void OnResolveKeysFinished();

    TTableResolverDiagnostics Finish();

private:
    TTableResolverDiagnostics Snapshot;
};

// Owns commit phase transitions and bounded shard acknowledgements for one commit attempt.
class TCommitDiagnosticsCapture {
public:
    TCommitDiagnosticsCapture(bool collectTimeline, bool collectShards);

    void OnPrepareStarted(TInstant at);
    void OnImmediateCommitStarted(TInstant at);
    void OnDistributedCommitStarted(TInstant at);
    void OnCoordinatorPlanned();
    void OnShardPrepared(ui64 shardId);
    void OnShardCommitted(ui64 shardId);

    TCommitDiagnostics Finish(Ydb::StatusIds::StatusCode status);

private:
    bool CollectTimeline = false;
    bool CollectShards = false;
    TShardAckDiagnosticsCollector PreparedShards;
    TShardAckDiagnosticsCollector CommittedShards;
    TCommitDiagnostics Snapshot;
};

void TrimExecutionTraceSnapshot(TExecutionTraceSnapshot& snapshot);

void TrimExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& snapshots);

void AppendExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& target,
    size_t& dropped, std::vector<TExecutionTraceSnapshot>& source,
    size_t sourceDropped = 0, size_t limit = MaxExecutionTraceSnapshots);

} // namespace NKikimr::NKqp
