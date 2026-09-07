#include "kqp_execution_trace.h"

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/string/cast.h>

#include <algorithm>
#include <tuple>

namespace NKikimr::NKqp {
namespace {

void KeepInterestingShard(TTaskTraceSnapshot& snapshot, NKqpProto::TKqpShardReadStats&& candidate) {
    if (snapshot.Shards.size() < MaxShardReadDiagnostics) {
        snapshot.Shards.push_back(std::move(candidate));
        return;
    }

    ++snapshot.ShardsTruncated;
    const auto leastInteresting = std::min_element(snapshot.Shards.begin(), snapshot.Shards.end(),
        [](const auto& lhs, const auto& rhs) {
            return ShardReadDiagnosticsRank(lhs) < ShardReadDiagnosticsRank(rhs);
        });
    if (leastInteresting != snapshot.Shards.end()
            && ShardReadDiagnosticsRank(*leastInteresting) < ShardReadDiagnosticsRank(candidate)) {
        *leastInteresting = std::move(candidate);
    }
}


bool Failed(Ydb::StatusIds::StatusCode status) {
    return status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
        && status != Ydb::StatusIds::SUCCESS;
}

auto StageRank(const TStageTraceSnapshot& stage) {
    const bool anomalous = std::any_of(stage.InterestingTasks.begin(),
        stage.InterestingTasks.end(), [](const auto& task) { return task.HasAnomaly(); });
    return std::tuple(stage.FailedTasks > 0, anomalous, stage.SpilledBytes > 0, stage.Durations.MaxUs,
        stage.WaitUs, stage.CpuUs);
}

auto ExecutionRank(const TExecutionTraceSnapshot& trace) {
    const bool anomalous = std::any_of(trace.Stages.begin(), trace.Stages.end(),
        [](const auto& stage) { return std::get<1>(StageRank(stage)); });
    const ui64 durationUs = trace.Timeline.Execute
        ? (trace.Timeline.Execute.End - trace.Timeline.Execute.Start).MicroSeconds() : 0;
    return std::tuple(Failed(trace.Status), anomalous, durationUs);
}

void FinishWindow(TTimeWindow& window, TInstant at) {
    if (window.Start != TInstant::Zero()) {
        window.End = Max(at, window.Start + TDuration::MicroSeconds(1));
    }
}

void CloseOpenWindow(TTimeWindow& window, TInstant at) {
    if (window.End == TInstant::Zero()) {
        FinishWindow(window, at);
    }
}

void FinishPhase(TPhaseDiagnostic& phase, TInstant at, Ydb::StatusIds::StatusCode status) {
    if (phase.Window.Start != TInstant::Zero() && phase.Window.End == TInstant::Zero()) {
        FinishWindow(phase.Window, at);
        phase.Status = status;
    }
}

// Online shard collection, compile dependency collection, and final query-wide top-N intentionally
// stay separate: they update entries differently, protect different in-flight state, and apply
// distinct eviction rules; one generic container would obscure these invariants.
template <class T, class TBetter>
void RetainBest(std::vector<T>& items, size_t limit, TBetter better) {
    if (items.size() <= limit) {
        return;
    }
    if (limit == 0) {
        items.clear();
        return;
    }
    std::nth_element(items.begin(), items.begin() + limit, items.end(), better);
    items.resize(limit);
}

struct TOwnedStage {
    size_t Execution;
    TStageTraceSnapshot Value;
};

struct TOwnedTask {
    size_t Execution;
    size_t Stage;
    TTaskTraceSnapshot Value;
};

struct TOwnedShard {
    size_t Execution;
    size_t Stage;
    size_t Task;
    NKqpProto::TKqpShardReadStats Value;
};

struct TOwnedNode {
    size_t Execution;
    size_t Stage;
    std::pair<ui32, ui32> Value;
};

struct TOwnedBufferShard {
    size_t Execution;
    NKqpProto::TKqpShardReadStats Value;
};

struct TOwnedCommitShard {
    size_t Execution;
    bool Prepared;
    ui64 DurationUs;
    TShardAckDiagnostic Value;
};

} // namespace

TTaskTraceSnapshot MakeTaskTraceSnapshot(const NYql::NDqProto::TDqTaskStats& task) {
    TTaskTraceSnapshot snapshot;
    snapshot.TaskId = task.GetTaskId();
    snapshot.NodeId = task.GetNodeId();
    const ui64 startMs = task.GetStartTimeMs() ? task.GetStartTimeMs() : task.GetCreateTimeMs();
    const ui64 finishMs = task.GetFinishTimeMs() ? task.GetFinishTimeMs() : task.GetUpdateTimeMs();
    if (startMs && finishMs >= startMs) {
        snapshot.Window = {
            TInstant::MilliSeconds(startMs),
            finishMs == startMs
                ? TInstant::MilliSeconds(finishMs) + TDuration::MicroSeconds(1)
                : TInstant::MilliSeconds(finishMs),
        };
    }
    if (task.GetCreateTimeMs() && task.GetStartTimeMs() > task.GetCreateTimeMs()) {
        snapshot.QueueDelayUs = (task.GetStartTimeMs() - task.GetCreateTimeMs()) * 1000;
    }
    snapshot.ComputeCpuUs = task.GetComputeCpuTimeUs();
    snapshot.BuildCpuUs = task.GetBuildCpuTimeUs();
    snapshot.InputRows = task.GetInputRows();
    snapshot.OutputRows = task.GetOutputRows();
    snapshot.WaitUs = task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs();
    snapshot.SpilledBytes = task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();

    if (task.HasExtra()) {
        NKqpProto::TKqpTaskExtraStats extra;
        if (task.GetExtra().UnpackTo(&extra)) {
            snapshot.ReadRetries = extra.GetReadRetriesCount() + extra.GetScanTaskExtraStats().GetRetriesCount();
            snapshot.ShardsTruncated = extra.GetShardReadsDroppedCount();
            for (const auto& shard : extra.GetShardReads()) {
                KeepInterestingShard(snapshot, NKqpProto::TKqpShardReadStats(shard));
            }
        }
    }
    for (const auto& source : task.GetSources()) {
        for (const auto& partition : source.GetExternalPartitions()) {
            ui64 shardId = 0;
            if (!TryFromString(partition.GetPartitionId(), shardId)) {
                continue;
            }
            NKqpProto::TKqpShardReadStats shard;
            shard.SetShardId(shardId);
            shard.SetStartTimeMs(partition.GetFirstMessageMs());
            shard.SetFinishTimeMs(partition.GetLastMessageMs());
            shard.SetRowCount(partition.GetExternalRows());
            shard.SetTimingBoundary(
                NKqpProto::TKqpShardReadStats::FIRST_MESSAGE_TO_LAST_MESSAGE);
            KeepInterestingShard(snapshot, std::move(shard));
        }
    }

    for (const auto& shard : snapshot.Shards) {
        if (!shard.GetStartTimeMs() || shard.GetFinishTimeMs() < shard.GetStartTimeMs()) {
            continue;
        }
        const TInstant shardStart = TInstant::MilliSeconds(shard.GetStartTimeMs());
        const TInstant shardEnd = TInstant::MilliSeconds(shard.GetFinishTimeMs());
        snapshot.Window.Start = snapshot.Window.Start == TInstant::Zero()
            ? shardStart : Min(snapshot.Window.Start, shardStart);
        snapshot.Window.End = Max(snapshot.Window.End, shardEnd);
    }
    std::sort(snapshot.Shards.begin(), snapshot.Shards.end(), [&](const auto& lhs, const auto& rhs) {
        return ShardReadDiagnosticsRank(lhs) > ShardReadDiagnosticsRank(rhs);
    });
    if (snapshot.Shards.size() > MaxInterestingShardsPerTask) {
        snapshot.ShardsTruncated += snapshot.Shards.size() - MaxInterestingShardsPerTask;
        snapshot.Shards.resize(MaxInterestingShardsPerTask);
    }
    return snapshot;
}

void KeepInterestingTask(TStageTraceSnapshot& stage, TTaskTraceSnapshot&& task) {
    auto& tasks = stage.InterestingTasks;
    if (auto it = std::find_if(tasks.begin(), tasks.end(), [&](const auto& item) {
            return item.TaskId == task.TaskId;
        }); it != tasks.end()) {
        *it = std::move(task);
        return;
    }
    if (tasks.size() < MaxInterestingTasksPerStage) {
        tasks.push_back(std::move(task));
        return;
    }
    auto least = std::min_element(tasks.begin(), tasks.end(), TaskDiagnosticsLess);
    if (least != tasks.end() && TaskDiagnosticsLess(*least, task)) {
        *least = std::move(task);
    }
}

void AccumulateExecutionTraceTotals(TExecutionTraceTotals& totals,
        const TExecutionTraceSnapshot& snapshot) {
    totals.CpuUs += snapshot.CpuUs;
    totals.WaitUs += snapshot.WaitUs;
    totals.SpilledBytes += snapshot.SpilledBytes;
    totals.MaxTaskSkew = std::max(totals.MaxTaskSkew, snapshot.MaxTaskSkew);
}

void AccumulateExecutionTraceTotals(TExecutionTraceTotals& totals,
        const TExecutionTraceTotals& source) {
    totals.CpuUs += source.CpuUs;
    totals.WaitUs += source.WaitUs;
    totals.SpilledBytes += source.SpilledBytes;
    totals.MaxTaskSkew = std::max(totals.MaxTaskSkew, source.MaxTaskSkew);
}

TExecutionDiagnosticsCapture::TExecutionDiagnosticsCapture(TString executerActorType,
        TString computeActorType) {
    Snapshot.ExecuterActorType = std::move(executerActorType);
    Snapshot.ComputeActorType = std::move(computeActorType);
    Snapshot.Timeline.Execute.Start = TInstant::Now();
}

void TExecutionDiagnosticsCapture::OnPhaseStarted(EExecutionPhase phase) {
    const TInstant transitionAt = TInstant::Now();
    EndCurrentPhase(transitionAt, Ydb::StatusIds::SUCCESS);
    CurrentPhase = phase;
    Snapshot.Timeline.Phase(phase) = {{transitionAt, {}}, Ydb::StatusIds::STATUS_CODE_UNSPECIFIED};
}

void TExecutionDiagnosticsCapture::OnTableResolverFinished(
        const TTimeWindow& navigateWindow, const TTimeWindow& resolveKeysWindow,
        Ydb::StatusIds::StatusCode status) {
    // Resolver envelopes may overlap and do not carry individual results. A
    // resolver failure cannot be attributed to either envelope from timing alone.
    const auto childStatus = status == Ydb::StatusIds::SUCCESS
        ? status : Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    Snapshot.Timeline.Phase(EExecutionPhase::ResolveMetadata) = {navigateWindow, childStatus};
    Snapshot.Timeline.Phase(EExecutionPhase::ResolvePartitioning) = {resolveKeysWindow, childStatus};
}

void TExecutionDiagnosticsCapture::SetCommitDiagnostics(TCommitDiagnostics diagnostics) {
    Snapshot.Commit = std::move(diagnostics);
}

TExecutionTraceSnapshot TExecutionDiagnosticsCapture::Finish(
        Ydb::StatusIds::StatusCode status) {
    const TInstant finishAt = TInstant::Now();
    Snapshot.Status = status;
    EndCurrentPhase(finishAt, status);
    FinishWindow(Snapshot.Timeline.Execute, finishAt);
    return std::move(Snapshot);
}

void TExecutionDiagnosticsCapture::EndCurrentPhase(TInstant finishAt,
        Ydb::StatusIds::StatusCode status) {
    if (CurrentPhase != EExecutionPhase::Count) {
        FinishPhase(Snapshot.Timeline.Phase(CurrentPhase), finishAt, status);
        CurrentPhase = EExecutionPhase::Count;
    }
}

void TTableResolverDiagnosticsCapture::OnNavigateStarted() {
    if (Snapshot.Navigate.Start == TInstant::Zero()) {
        Snapshot.Navigate.Start = TInstant::Now();
    }
}

void TTableResolverDiagnosticsCapture::OnNavigateFinished() {
    FinishWindow(Snapshot.Navigate, TInstant::Now());
}

void TTableResolverDiagnosticsCapture::OnResolveKeysStarted() {
    Snapshot.ResolveKeys.Start = TInstant::Now();
}

void TTableResolverDiagnosticsCapture::OnResolveKeysFinished() {
    FinishWindow(Snapshot.ResolveKeys, TInstant::Now());
}

TTableResolverDiagnostics TTableResolverDiagnosticsCapture::Finish() {
    const TInstant finishedAt = TInstant::Now();
    CloseOpenWindow(Snapshot.Navigate, finishedAt);
    CloseOpenWindow(Snapshot.ResolveKeys, finishedAt);
    return std::move(Snapshot);
}

TCommitDiagnosticsCapture::TCommitDiagnosticsCapture(bool collectTimeline, bool collectShards)
    : CollectTimeline(collectTimeline)
    , CollectShards(collectShards)
{}

void TCommitDiagnosticsCapture::OnPrepareStarted(TInstant at) {
    if (CollectTimeline) {
        Snapshot.PrepareShards.Window.Start = at;
    }
}

void TCommitDiagnosticsCapture::OnImmediateCommitStarted(TInstant at) {
    if (CollectTimeline) {
        Snapshot.ApplyShards.Window.Start = at;
    }
}

void TCommitDiagnosticsCapture::OnDistributedCommitStarted(TInstant at) {
    if (CollectTimeline) {
        FinishPhase(Snapshot.PrepareShards, at, Ydb::StatusIds::SUCCESS);
        Snapshot.Coordinator.Window.Start = at;
    }
}

void TCommitDiagnosticsCapture::OnCoordinatorPlanned() {
    if (CollectTimeline) {
        FinishPhase(Snapshot.Coordinator, TInstant::Now(), Ydb::StatusIds::SUCCESS);
        Snapshot.ApplyShards.Window.Start = Snapshot.Coordinator.Window.End;
    }
}

void TCommitDiagnosticsCapture::OnShardPrepared(ui64 shardId) {
    if (CollectShards && shardId) {
        PreparedShards.OnAck(shardId);
    }
}

void TCommitDiagnosticsCapture::OnShardCommitted(ui64 shardId) {
    if (CollectShards && shardId) {
        CommittedShards.OnAck(shardId);
    }
}

TCommitDiagnostics TCommitDiagnosticsCapture::Finish(Ydb::StatusIds::StatusCode status) {
    if (CollectTimeline) {
        const TInstant finishedAt = TInstant::Now();
        FinishPhase(Snapshot.PrepareShards, finishedAt, status);
        FinishPhase(Snapshot.Coordinator, finishedAt, status);
        FinishPhase(Snapshot.ApplyShards, finishedAt, status);
    }
    if (CollectShards) {
        Snapshot.PreparedShards = PreparedShards.Shards();
        Snapshot.CommittedShards = CommittedShards.Shards();
        Snapshot.PreparedShardsTruncated = PreparedShards.Dropped();
        Snapshot.CommittedShardsTruncated = CommittedShards.Dropped();
    }
    return std::move(Snapshot);
}

void TrimExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& snapshots) {
    for (auto& trace : snapshots) {
        auto& buffer = trace.BufferLookup;
        std::sort(buffer.Shards.begin(), buffer.Shards.end(), [](const auto& lhs, const auto& rhs) {
            return ShardReadDiagnosticsRank(lhs) > ShardReadDiagnosticsRank(rhs);
        });
        if (buffer.Shards.size() > MaxInterestingShardsPerTask) {
            buffer.ShardsTruncated += buffer.Shards.size() - MaxInterestingShardsPerTask;
            buffer.Shards.resize(MaxInterestingShardsPerTask);
        }
    }
    std::vector<TOwnedStage> stages;
    std::vector<size_t> originalStages(snapshots.size());
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        auto& source = snapshots[execution].Stages;
        originalStages[execution] = source.size();
        for (auto& stage : source) {
            stages.push_back({execution, std::move(stage)});
        }
        source.clear();
    }
    RetainBest(stages, MaxStageTraceSnapshotsPerQuery, [](const auto& lhs, const auto& rhs) {
        return StageRank(lhs.Value) > StageRank(rhs.Value);
    });
    for (auto& stage : stages) {
        snapshots[stage.Execution].Stages.push_back(std::move(stage.Value));
    }
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        auto& trace = snapshots[execution];
        trace.StagesTruncated += originalStages[execution] - trace.Stages.size();
        std::sort(trace.Stages.begin(), trace.Stages.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.StageId < rhs.StageId;
        });
    }

    std::vector<TOwnedTask> tasks;
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        for (size_t stage = 0; stage < snapshots[execution].Stages.size(); ++stage) {
            auto& source = snapshots[execution].Stages[stage].InterestingTasks;
            for (auto& task : source) {
                tasks.push_back({execution, stage, std::move(task)});
            }
            source.clear();
        }
    }
    RetainBest(tasks, MaxTaskTraceSnapshotsPerQuery, [](const auto& lhs, const auto& rhs) {
        return TaskDiagnosticsLess(rhs.Value, lhs.Value);
    });
    for (auto& task : tasks) {
        snapshots[task.Execution].Stages[task.Stage].InterestingTasks.push_back(std::move(task.Value));
    }
    for (auto& trace : snapshots) {
        for (auto& stage : trace.Stages) {
            std::sort(stage.InterestingTasks.begin(), stage.InterestingTasks.end(),
                [](const auto& lhs, const auto& rhs) { return TaskDiagnosticsLess(rhs, lhs); });
        }
    }

    std::vector<TOwnedShard> shards;
    std::vector<std::vector<std::vector<size_t>>> originalShards(snapshots.size());
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        originalShards[execution].resize(snapshots[execution].Stages.size());
        for (size_t stage = 0; stage < snapshots[execution].Stages.size(); ++stage) {
            auto& tasksInStage = snapshots[execution].Stages[stage].InterestingTasks;
            originalShards[execution][stage].resize(tasksInStage.size());
            for (size_t task = 0; task < tasksInStage.size(); ++task) {
                auto& source = tasksInStage[task].Shards;
                originalShards[execution][stage][task] = source.size();
                for (auto& shard : source) {
                    shards.push_back({execution, stage, task, std::move(shard)});
                }
                source.clear();
            }
        }
    }
    RetainBest(shards, MaxShardTraceSnapshotsPerQuery, [](const auto& lhs, const auto& rhs) {
        return ShardReadDiagnosticsRank(lhs.Value) > ShardReadDiagnosticsRank(rhs.Value);
    });
    for (auto& shard : shards) {
        snapshots[shard.Execution].Stages[shard.Stage].InterestingTasks[shard.Task].Shards.push_back(
            std::move(shard.Value));
    }
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        for (size_t stage = 0; stage < snapshots[execution].Stages.size(); ++stage) {
            auto& tasksInStage = snapshots[execution].Stages[stage].InterestingTasks;
            for (size_t task = 0; task < tasksInStage.size(); ++task) {
                auto& snapshot = tasksInStage[task];
                snapshot.ShardsTruncated += originalShards[execution][stage][task]
                    - snapshot.Shards.size();
                std::sort(snapshot.Shards.begin(), snapshot.Shards.end(), [](const auto& lhs, const auto& rhs) {
                    return ShardReadDiagnosticsRank(lhs) > ShardReadDiagnosticsRank(rhs);
                });
            }
        }
    }

    std::vector<TOwnedNode> nodes;
    std::vector<std::vector<size_t>> originalNodes(snapshots.size());
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        originalNodes[execution].resize(snapshots[execution].Stages.size());
        for (size_t stage = 0; stage < snapshots[execution].Stages.size(); ++stage) {
            auto& source = snapshots[execution].Stages[stage].TasksByNode;
            originalNodes[execution][stage] = source.size();
            for (auto& node : source) {
                nodes.push_back({execution, stage, node});
            }
            source.clear();
        }
    }
    RetainBest(nodes, MaxStageNodeDiagnosticsPerQuery, [](const auto& lhs, const auto& rhs) {
        return lhs.Value.second > rhs.Value.second;
    });
    for (auto& node : nodes) {
        snapshots[node.Execution].Stages[node.Stage].TasksByNode.push_back(node.Value);
    }
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        for (size_t stage = 0; stage < snapshots[execution].Stages.size(); ++stage) {
            auto& snapshot = snapshots[execution].Stages[stage];
            snapshot.NodesTruncated += originalNodes[execution][stage] - snapshot.TasksByNode.size();
            std::sort(snapshot.TasksByNode.begin(), snapshot.TasksByNode.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.second > rhs.second;
            });
        }
    }

    std::vector<TOwnedBufferShard> bufferShards;
    std::vector<size_t> originalBufferShards(snapshots.size());
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        auto& source = snapshots[execution].BufferLookup.Shards;
        originalBufferShards[execution] = source.size();
        for (auto& shard : source) {
            bufferShards.push_back({execution, std::move(shard)});
        }
        source.clear();
    }
    RetainBest(bufferShards, MaxBufferLookupDiagnosticsPerQuery, [](const auto& lhs, const auto& rhs) {
        return ShardReadDiagnosticsRank(lhs.Value) > ShardReadDiagnosticsRank(rhs.Value);
    });
    for (auto& shard : bufferShards) {
        snapshots[shard.Execution].BufferLookup.Shards.push_back(std::move(shard.Value));
    }
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        snapshots[execution].BufferLookup.ShardsTruncated += originalBufferShards[execution]
            - snapshots[execution].BufferLookup.Shards.size();
    }

    std::vector<TOwnedCommitShard> commitShards;
    std::vector<std::pair<size_t, size_t>> originalCommitShards(snapshots.size());
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        auto& commit = snapshots[execution].Commit;
        originalCommitShards[execution] = {commit.PreparedShards.size(), commit.CommittedShards.size()};
        for (auto& shard : commit.PreparedShards) {
            const ui64 durationUs = commit.PrepareShards.Window.Start != TInstant::Zero()
                    && shard.AcknowledgedAt >= commit.PrepareShards.Window.Start
                ? (shard.AcknowledgedAt - commit.PrepareShards.Window.Start).MicroSeconds() : 0;
            commitShards.push_back({execution, true, durationUs, std::move(shard)});
        }
        for (auto& shard : commit.CommittedShards) {
            const ui64 durationUs = commit.ApplyShards.Window.Start != TInstant::Zero()
                    && shard.AcknowledgedAt >= commit.ApplyShards.Window.Start
                ? (shard.AcknowledgedAt - commit.ApplyShards.Window.Start).MicroSeconds() : 0;
            commitShards.push_back({execution, false, durationUs, std::move(shard)});
        }
        commit.PreparedShards.clear();
        commit.CommittedShards.clear();
    }
    RetainBest(commitShards, MaxCommitShardDiagnosticsPerQuery, [](const auto& lhs, const auto& rhs) {
        return lhs.DurationUs > rhs.DurationUs;
    });
    for (auto& shard : commitShards) {
        auto& commit = snapshots[shard.Execution].Commit;
        (shard.Prepared ? commit.PreparedShards : commit.CommittedShards).push_back(std::move(shard.Value));
    }
    for (size_t execution = 0; execution < snapshots.size(); ++execution) {
        auto& commit = snapshots[execution].Commit;
        commit.PreparedShardsTruncated += originalCommitShards[execution].first
            - commit.PreparedShards.size();
        commit.CommittedShardsTruncated += originalCommitShards[execution].second
            - commit.CommittedShards.size();
    }
}

void TrimExecutionTraceSnapshot(TExecutionTraceSnapshot& snapshot) {
    std::vector<TExecutionTraceSnapshot> snapshots;
    snapshots.push_back(std::move(snapshot));
    TrimExecutionTraceSnapshots(snapshots);
    snapshot = std::move(snapshots.front());
}

void AppendExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& target,
        size_t& dropped, std::vector<TExecutionTraceSnapshot>& source,
        size_t sourceDropped, size_t limit) {
    dropped += sourceDropped;
    for (auto& snapshot : source) {
        TrimExecutionTraceSnapshot(snapshot);
        if (target.size() < limit) {
            target.push_back(std::move(snapshot));
            continue;
        }
        ++dropped;
        if (limit == 0) {
            continue;
        }
        auto least = std::min_element(target.begin(), target.end(), [](const auto& lhs, const auto& rhs) {
            return ExecutionRank(lhs) < ExecutionRank(rhs);
        });
        if (least != target.end() && ExecutionRank(*least) < ExecutionRank(snapshot)) {
            *least = std::move(snapshot);
        }
    }
    source.clear();
    TrimExecutionTraceSnapshots(target);
}

} // namespace NKikimr::NKqp
