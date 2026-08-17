#include "kqp_execution_trace.h"

#include <algorithm>
#include <tuple>

namespace NKikimr::NKqp {
namespace {

bool Failed(Ydb::StatusIds::StatusCode status) {
    return status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
        && status != Ydb::StatusIds::SUCCESS;
}

auto ShardRank(const NKqpProto::TKqpShardReadStats& shard) {
    const ui64 durationMs = shard.GetStartTimeMs()
            && shard.GetFinishTimeMs() >= shard.GetStartTimeMs()
        ? shard.GetFinishTimeMs() - shard.GetStartTimeMs() : 0;
    return std::tuple(Failed(shard.GetStatus()), shard.GetRetries() > 0, durationMs);
}

auto TaskRank(const TTaskTraceSnapshot& task) {
    return std::tuple(task.Failed, task.HasAnomaly(), task.SpilledBytes > 0, task.ReadRetries > 0,
        task.DurationUs());
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

void TrimExecutionTraceSnapshots(std::vector<TExecutionTraceSnapshot>& snapshots) {
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
        return TaskRank(lhs.Value) > TaskRank(rhs.Value);
    });
    for (auto& task : tasks) {
        snapshots[task.Execution].Stages[task.Stage].InterestingTasks.push_back(std::move(task.Value));
    }
    for (auto& trace : snapshots) {
        for (auto& stage : trace.Stages) {
            std::sort(stage.InterestingTasks.begin(), stage.InterestingTasks.end(),
                [](const auto& lhs, const auto& rhs) { return TaskRank(lhs) > TaskRank(rhs); });
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
        return ShardRank(lhs.Value) > ShardRank(rhs.Value);
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
                    return ShardRank(lhs) > ShardRank(rhs);
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
        return ShardRank(lhs.Value) > ShardRank(rhs.Value);
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
            const ui64 durationUs = commit.PrepareShards.Start != TInstant::Zero()
                    && shard.AcknowledgedAt >= commit.PrepareShards.Start
                ? (shard.AcknowledgedAt - commit.PrepareShards.Start).MicroSeconds() : 0;
            commitShards.push_back({execution, true, durationUs, std::move(shard)});
        }
        for (auto& shard : commit.CommittedShards) {
            const ui64 durationUs = commit.ApplyShards.Start != TInstant::Zero()
                    && shard.AcknowledgedAt >= commit.ApplyShards.Start
                ? (shard.AcknowledgedAt - commit.ApplyShards.Start).MicroSeconds() : 0;
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
