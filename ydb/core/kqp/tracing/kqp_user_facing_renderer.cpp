#include "kqp_user_facing.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <functional>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

namespace {

TStringBuf CompileDependencyPurposeName(ECompileDependencyPurpose purpose) {
    switch (purpose) {
        case ECompileDependencyPurpose::QueryTable:
            return "query_table";
        case ECompileDependencyPurpose::IndexImplementation:
            return "index_implementation";
        case ECompileDependencyPurpose::ExternalDataSource:
            return "external_data_source";
    }
    return "unknown";
}

using TPhaseAttrs = std::initializer_list<std::pair<TString, NWilson::TAttributeValue>>;
using TQueryLevels = TComponentTracingLevels::TQueryProcessor;
using TSpanBudget = TUserFacingSpanBudget;

void TrimCompileDependencies(std::vector<TCompileAttemptDiagnostic>& attempts) {
    struct TOwnedDependency {
        size_t Attempt;
        TCompileDependencyDiagnostic Value;
    };

    std::vector<TOwnedDependency> dependencies;
    std::vector<size_t> originalSizes(attempts.size());
    std::vector<size_t> sourceDropped(attempts.size());
    for (size_t attempt = 0; attempt < attempts.size(); ++attempt) {
        if (!attempts[attempt].Dependencies) {
            continue;
        }
        const auto& source = *attempts[attempt].Dependencies;
        originalSizes[attempt] = source.Dependencies.size();
        sourceDropped[attempt] = source.Dropped;
        for (const auto& dependency : source.Dependencies) {
            dependencies.push_back({attempt, dependency});
        }
    }
    auto rank = [](const TCompileDependencyDiagnostic& dependency) {
        const bool pending = dependency.Status == ECompileDependencyStatus::Unknown;
        const bool failed = dependency.Status == ECompileDependencyStatus::Error;
        const ui64 durationUs = dependency.Start != TInstant::Zero() && dependency.End >= dependency.Start
            ? (dependency.End - dependency.Start).MicroSeconds() : 0;
        return std::tuple(pending, failed, durationUs);
    };
    if (dependencies.size() > MaxCompileDependencyDiagnosticsPerQuery) {
        std::nth_element(dependencies.begin(),
            dependencies.begin() + MaxCompileDependencyDiagnosticsPerQuery, dependencies.end(),
            [&](const auto& lhs, const auto& rhs) { return rank(lhs.Value) > rank(rhs.Value); });
        dependencies.resize(MaxCompileDependencyDiagnosticsPerQuery);
    }

    std::vector<std::vector<TCompileDependencyDiagnostic>> retained(attempts.size());
    for (auto& dependency : dependencies) {
        retained[dependency.Attempt].push_back(std::move(dependency.Value));
    }
    for (size_t attempt = 0; attempt < attempts.size(); ++attempt) {
        if (!attempts[attempt].Dependencies) {
            continue;
        }
        const size_t retainedSize = retained[attempt].size();
        attempts[attempt].Dependencies = std::make_shared<const TCompileDiagnostics>(TCompileDiagnostics{
            .Dependencies = std::move(retained[attempt]),
            .Dropped = sourceDropped[attempt] + originalSizes[attempt]
                - retainedSize,
        });
    }
    std::sort(attempts.begin(), attempts.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.Start < rhs.Start;
    });
}

NWilson::NTraceProto::Status::StatusCode ToWilsonStatus(Ydb::StatusIds::StatusCode status) {
    if (status == Ydb::StatusIds::SUCCESS) {
        return NWilson::NTraceProto::Status::STATUS_CODE_OK;
    }
    if (status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
        return NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
    }
    return NWilson::NTraceProto::Status::STATUS_CODE_ERROR;
}

NWilson::NTraceProto::Status::StatusCode CompletedChildStatus(
        Ydb::StatusIds::StatusCode executionStatus) {
    return executionStatus == Ydb::StatusIds::SUCCESS
        ? NWilson::NTraceProto::Status::STATUS_CODE_OK
        : NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
}

NWilson::NTraceProto::Status::StatusCode PhaseStatus(
        const TExecutionTraceSnapshot& trace,
        std::initializer_list<EExecutionPhase> phases) {
    if (trace.Status == Ydb::StatusIds::SUCCESS) {
        return NWilson::NTraceProto::Status::STATUS_CODE_OK;
    }
    if (trace.Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
        return NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
    }
    if (trace.FailedPhase && std::find(phases.begin(), phases.end(), *trace.FailedPhase) != phases.end()) {
        return NWilson::NTraceProto::Status::STATUS_CODE_ERROR;
    }
    return NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
}

NWilson::TSpan MakeSpan(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}, TSpanBudget* budget = nullptr,
        ui8 requiredVerbosity = TQueryLevels::TopLevel,
        NWilson::NTraceProto::Status::StatusCode status = NWilson::NTraceProto::Status::STATUS_CODE_OK) {
    if (start == TInstant::Zero() || end == TInstant::Zero() || end < start) {
        return {};
    }
    if (budget && !budget->Admit(requiredVerbosity)) {
        return {};
    }
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parentId, parentId.Span(parentId.GetVerbosity()), start, end,
        status, name,
        NWilson::MakeUserFacingWilsonUploaderId());
    for (const auto& [key, value] : attrs) {
        span.Attribute(key, value);
    }
    return span;
}

NWilson::TSpan MakePhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}, TSpanBudget* budget = nullptr,
        ui8 requiredVerbosity = TQueryLevels::TopLevel,
        NWilson::NTraceProto::Status::StatusCode status = NWilson::NTraceProto::Status::STATUS_CODE_OK) {
    if (end <= start) {
        return {};
    }
    return MakeSpan(parentId, start, end, name, attrs, budget, requiredVerbosity, status);
}

void EmitPhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}, TSpanBudget* budget = nullptr,
        ui8 requiredVerbosity = TQueryLevels::TopLevel,
        NWilson::NTraceProto::Status::StatusCode status = NWilson::NTraceProto::Status::STATUS_CODE_OK) {
    NWilson::TSpan span = MakePhase(parentId, start, end, name, attrs, budget, requiredVerbosity, status);
    if (span) {
        span.End();
    }
}

void EmitMarker(const NWilson::TTraceId& parentId, TInstant at,
        const TString& name, TPhaseAttrs attrs = {}, TSpanBudget* budget = nullptr,
        ui8 requiredVerbosity = TQueryLevels::TopLevel,
        NWilson::NTraceProto::Status::StatusCode status = NWilson::NTraceProto::Status::STATUS_CODE_OK) {
    NWilson::TSpan span = MakeSpan(
        parentId, at, at, name, attrs, budget, requiredVerbosity, status);
    if (span) {
        span.End();
    }
}

// Keep the distinguishing suffix in the name and the full tablet id in an attribute.
TString ShardDisplayName(const TStringBuf action, ui64 shardId) {
    const TString full = ToString(shardId);
    TStringBuilder name;
    name << action << " shard ";
    if (full.size() <= 6) {
        name << full;
    } else {
        name << "\u2026" << full.substr(full.size() - 6);
    }
    return name;
}

void EmitCommitShardPhase(const NWilson::TTraceId& parent, const TTimeWindow& window,
        TStringBuf displayName, TStringBuf machineName, TStringBuf shardAction,
        const std::vector<TShardAckDiagnostic>& acknowledgements, size_t truncated,
        TSpanBudget& budget) {
    NWilson::TSpan phase = MakePhase(parent, window.Start, window.End, TString(displayName), {
        {"ydb.phase", TString(machineName)},
    }, &budget, TQueryLevels::Detailed);
    if (!phase) {
        return;
    }
    phase.Attribute("ydb.actor.type", TString("TKqpBufferWriteActor"));
    phase.Attribute("ydb.peer.actor.type", TString("DataShard"));
    if (truncated > 0) {
        phase.Attribute("ydb.shards_truncated", static_cast<i64>(truncated));
    }
    for (const auto& ack : acknowledgements) {
        if (ack.AcknowledgedAt < window.Start) {
            continue;
        }
        EmitPhase(phase.GetTraceId(), window.Start, ack.AcknowledgedAt,
            ShardDisplayName(shardAction, ack.ShardId), {
                {"ydb.shard_id", static_cast<i64>(ack.ShardId)},
                {"ydb.actor.type", TString("TKqpBufferWriteActor")},
                {"ydb.peer.actor.type", TString("DataShard")},
            }, &budget, TQueryLevels::Diagnostic);
    }
    phase.End();
}

struct TStageDescription {
    TString Name;
    TString Verb;
};

TStageDescription DescribeStage(const TStageTraceSnapshot& stage) {
    TString verb;
    switch (stage.Operation) {
        case EStageOperation::Read:
            verb = "Read";
            break;
        case EStageOperation::Write:
            verb = "Write";
            break;
        case EStageOperation::Join:
            return {"Join", "Join"};
        case EStageOperation::Aggregate:
            return {"Aggregate", "Aggregate"};
        case EStageOperation::Filter:
            return {"Filter", "Filter"};
        case EStageOperation::Compute:
            return {TStringBuilder() << "Step " << stage.StageId, "Compute"};
    }
    return stage.TablePath
        ? TStageDescription{TStringBuilder() << verb << " " << stage.TablePath, verb}
        : TStageDescription{verb, verb};
}

void EmitShardReadSpans(const NWilson::TTraceId& parent,
        const std::vector<NKqpProto::TKqpShardReadStats>& shards,
        const TTimeWindow& parentBounds, TSpanBudget& budget) {
    for (const auto& shard : shards) {
        const auto status = shard.GetStatus();
        const bool failed = status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
            && status != Ydb::StatusIds::SUCCESS;
        const bool measured = shard.GetStartTimeMs() != 0
            && shard.GetFinishTimeMs() > shard.GetStartTimeMs();
        const bool instantBoundary = shard.GetStartTimeMs() != 0
            && shard.GetFinishTimeMs() == shard.GetStartTimeMs();
        if (!measured && !instantBoundary && (!failed || shard.GetFinishTimeMs() == 0)) {
            continue;
        }
        TTimeWindow bounds;
        if (measured) {
            bounds = FitUserFacingRemoteWindow({
                TInstant::MilliSeconds(shard.GetStartTimeMs()),
                TInstant::MilliSeconds(shard.GetFinishTimeMs()),
            }, parentBounds);
            if (!bounds) {
                continue;
            }
        } else {
            const TInstant finish = TInstant::MilliSeconds(shard.GetFinishTimeMs());
            const TInstant marker = Min(Max(finish, parentBounds.Start), parentBounds.End);
            bounds = {marker, marker};
        }
        NWilson::TSpan span = MakeSpan(parent, bounds.Start, bounds.End,
            ShardDisplayName("Read from", shard.GetShardId()), {},
            &budget, TQueryLevels::Diagnostic,
            failed ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                   : NWilson::NTraceProto::Status::STATUS_CODE_OK);
        if (!span) {
            continue;
        }
        span.Attribute("ydb.shard_id", static_cast<i64>(shard.GetShardId()));
        span.Attribute("ydb.code.component", TString("KqpShardRead"));
        span.Attribute("ydb.peer.actor.type", TString("DataShard"));
        span.Attribute("ydb.rows", static_cast<i64>(shard.GetRowCount()));
        if (!measured || shard.GetStartTimeMs() == shard.GetFinishTimeMs()) {
            span.Attribute("ydb.duration.measured", false);
        }
        if (status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
            span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
            span.Attribute("ydb.finished", shard.GetFinished());
        }
        if (shard.GetRetryCount() > 0) {
            span.Attribute("ydb.read_retries", static_cast<i64>(shard.GetRetryCount()));
        }
        if (shard.GetNodeId()) {
            span.Attribute("ydb.node_id", static_cast<i64>(shard.GetNodeId()));
        }
        if (shard.GetTimingBoundary()
                == NKqpProto::TKqpShardReadStats::FIRST_MESSAGE_TO_LAST_MESSAGE) {
            span.Attribute("ydb.timing_boundary", TString("first_to_last_message"));
        }
        span.End();
    }
}

void EmitTaskSpans(const NWilson::TTraceId& stageParent, const TString& stageVerb,
        const TString& actorType, const std::vector<TTaskTraceSnapshot>& tasks,
        const TTimeWindow& stageBounds, Ydb::StatusIds::StatusCode executionStatus,
        TSpanBudget& budget) {
    for (const auto& task : tasks) {
        const auto bounds = FitUserFacingRemoteWindow(task.Window, stageBounds);
        if (!bounds) {
            continue;
        }
        NWilson::TSpan span = MakePhase(stageParent, bounds.Start, bounds.End,
            TStringBuilder() << stageVerb << " task " << task.TaskId, {},
            &budget, TQueryLevels::Detailed,
            task.Failed ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                : CompletedChildStatus(executionStatus));
        if (!span) {
            continue;
        }
        span.Attribute("ydb.task_id", static_cast<i64>(task.TaskId));
        span.Attribute("ydb.actor.type", actorType);
        if (task.NodeId) {
            span.Attribute("ydb.node_id", static_cast<i64>(task.NodeId));
        }
        span.Attribute("ydb.input_rows", static_cast<i64>(task.InputRows));
        span.Attribute("ydb.output_rows", static_cast<i64>(task.OutputRows));
        span.Attribute("ydb.duration_us", static_cast<i64>(task.DurationUs()));
        if (task.Failed) {
            span.Attribute("ydb.status_code", TString("ERROR"));
        }
        if (task.QueueDelayUs > 0) {
            span.Attribute("ydb.queue_delay_us", static_cast<i64>(task.QueueDelayUs));
        }
        if (task.ComputeCpuUs > 0) {
            span.Attribute("ydb.compute_cpu_us", static_cast<i64>(task.ComputeCpuUs));
        }
        if (task.BuildCpuUs > 0) {
            span.Attribute("ydb.build_cpu_us", static_cast<i64>(task.BuildCpuUs));
        }
        if (task.WaitUs > 0) {
            span.Attribute("ydb.wait_us", static_cast<i64>(task.WaitUs));
        }
        if (task.SpilledBytes > 0) {
            span.Attribute("ydb.spilled_bytes", static_cast<i64>(task.SpilledBytes));
        }
        if (task.ReadRetries > 0) {
            span.Attribute("ydb.read_retries", static_cast<i64>(task.ReadRetries));
        }
        EmitShardReadSpans(span.GetTraceId(), task.Shards, bounds, budget);
        if (task.ShardsTruncated > 0) {
            span.Attribute("ydb.shards_truncated", static_cast<i64>(task.ShardsTruncated));
        }
        span.End();
    }
}

void EmitStageSpans(const NWilson::TTraceId& parent, const TExecutionTraceSnapshot& trace,
        const TTimeWindow& runBounds, TSpanBudget& budget) {
    for (const auto& stage : trace.Stages) {
        if (!stage.Window) {
            continue;
        }
        const TStageDescription description = DescribeStage(stage);
        const auto stageBounds = FitUserFacingRemoteWindow(stage.Window, runBounds);
        if (!stageBounds) {
            continue;
        }
        NWilson::TSpan span = MakePhase(parent, stageBounds.Start, stageBounds.End,
            description.Name, {}, &budget, TQueryLevels::Detailed,
            stage.FailedTasks > 0 ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                : CompletedChildStatus(trace.Status));
        if (!span) {
            continue;
        }
        span.Attribute("ydb.stage_id", static_cast<i64>(stage.StageId));
        span.Attribute("ydb.stage.operation", description.Verb);
        span.Attribute("ydb.actor.type", trace.ComputeActorType);
        span.Attribute("ydb.timing_source", TString("compute_actor_stats"));
        span.Attribute("ydb.tasks", static_cast<i64>(stage.Tasks));
        if (stage.FailedTasks > 0) {
            span.Attribute("ydb.failed_tasks", static_cast<i64>(stage.FailedTasks));
        }
        span.Attribute("ydb.cpu_us", static_cast<i64>(stage.CpuUs));
        span.Attribute("ydb.input_rows", static_cast<i64>(stage.InputRows));
        span.Attribute("ydb.output_rows", static_cast<i64>(stage.OutputRows));
        if (stage.WaitUs > 0) {
            span.Attribute("ydb.wait_us", static_cast<i64>(stage.WaitUs));
        }
        if (stage.SpilledBytes > 0) {
            span.Attribute("ydb.spilled_bytes", static_cast<i64>(stage.SpilledBytes));
        }
        if (stage.Durations.Count > 0) {
            span.Attribute("ydb.task_duration_min_us", static_cast<i64>(stage.Durations.MinUs));
            span.Attribute("ydb.task_duration_avg_us",
                static_cast<i64>(stage.Durations.SumUs / stage.Durations.Count));
            span.Attribute("ydb.task_duration_max_us", static_cast<i64>(stage.Durations.MaxUs));
        }
        if (stage.Durations.Count > 1 && stage.Durations.SumUs > 0) {
            const double average = static_cast<double>(stage.Durations.SumUs) / stage.Durations.Count;
            if (stage.Durations.MaxUs > average) {
                span.Attribute("ydb.task_skew", static_cast<double>(stage.Durations.MaxUs) / average);
            }
        }
        if (!stage.TasksByNode.empty()) {
            TStringBuilder byNode;
            for (size_t i = 0; i < stage.TasksByNode.size(); ++i) {
                if (i > 0) {
                    byNode << ",";
                }
                byNode << stage.TasksByNode[i].first << ":" << stage.TasksByNode[i].second;
            }
            span.Attribute("ydb.tasks_by_node", TString(byNode));
        }
        if (stage.NodesTruncated > 0) {
            span.Attribute("ydb.nodes_truncated", static_cast<i64>(stage.NodesTruncated));
        }
        if (stage.Tasks > 1 && stage.Durations.Count > 0) {
            span.Attribute("ydb.slowest_task_node", static_cast<i64>(stage.SlowestTaskNode));
            span.Attribute("ydb.fastest_task_node", static_cast<i64>(stage.FastestTaskNode));
        }
        if (stage.Tasks > stage.InterestingTasks.size()) {
            span.Attribute("ydb.tasks_truncated",
                static_cast<i64>(stage.Tasks - stage.InterestingTasks.size()));
        }
        EmitTaskSpans(span.GetTraceId(), description.Verb, trace.ComputeActorType,
            stage.InterestingTasks, stageBounds, trace.Status, budget);
        span.End();
    }
}

template <typename THops>
void EmitProxySpans(const NWilson::TTraceId& parentId, const THops& hops,
        TInstant sessionStart, TSpanBudget& budget, size_t firstHop = 1) {
    const size_t hopCount = static_cast<size_t>(hops.size());
    std::vector<TTimeWindow> windows(hopCount);
    TInstant cursor = sessionStart;
    for (int i = static_cast<int>(hopCount) - 1; i >= 0; --i) {
        const TDuration duration = TDuration::MicroSeconds(hops[i].GetDurationUs());
        windows[i] = {cursor - duration, cursor};
        cursor -= duration;
    }
    for (size_t i = Min(firstHop, hopCount); i < hopCount; ++i) {
        const auto& hop = hops[i];
        const auto& window = windows[i];
        if (window.End == window.Start) {
            EmitMarker(parentId, window.Start, "KQP proxy", {
                {"ydb.actor.type", TString("TKqpProxyService")},
                {"ydb.node_id", static_cast<i64>(hop.GetNodeId())},
                {"ydb.target_node_id", static_cast<i64>(hop.GetTargetNodeId())},
                {"ydb.forwarded", hop.GetNodeId() != hop.GetTargetNodeId()},
                {"ydb.duration.source", TString("local_monotonic")},
            }, &budget, TQueryLevels::TopLevel);
        } else if (NWilson::TSpan proxy = MakePhase(parentId, window.Start, window.End, "KQP proxy", {},
                       &budget, TQueryLevels::TopLevel)) {
            proxy.Attribute("ydb.actor.type", TString("TKqpProxyService"));
            proxy.Attribute("ydb.node_id", static_cast<i64>(hop.GetNodeId()));
            proxy.Attribute("ydb.target_node_id", static_cast<i64>(hop.GetTargetNodeId()));
            proxy.Attribute("ydb.forwarded", hop.GetNodeId() != hop.GetTargetNodeId());
            proxy.Attribute("ydb.duration.source", TString("local_monotonic"));
            proxy.End();
        }
        if (i + 1 >= hopCount || hop.GetNodeId() == hop.GetTargetNodeId()) {
            continue;
        }
        EmitMarker(parentId, window.End, "Forward to KQP proxy", {
            {"ydb.actor.type", TString("InterconnectProxy")},
            {"ydb.source_node_id", static_cast<i64>(hop.GetNodeId())},
            {"ydb.target_node_id", static_cast<i64>(hop.GetTargetNodeId())},
            {"ydb.duration.measured", false},
        }, &budget, TQueryLevels::TopLevel);
    }
}

void EmitProxySpans(const NWilson::TTraceId& parentId, const TUserFacingQuerySnapshot& state,
        TSpanBudget& budget) {
    EmitProxySpans(parentId, state.ProxyRequestHops, state.StartTime, budget);
}

TTimeWindow GetStageBounds(const TExecutionTraceSnapshot& trace, const TTimeWindow& runBounds) {
    TTimeWindow result;
    for (const auto& stage : trace.Stages) {
        const auto stageBounds = FitUserFacingRemoteWindow(stage.Window, runBounds);
        if (!stageBounds) {
            continue;
        }
        result.Start = result.Start == TInstant::Zero()
            ? stageBounds.Start : Min(result.Start, stageBounds.Start);
        result.End = Max(result.End, stageBounds.End);
    }
    return result;
}

void EmitBufferLookupSpan(const NWilson::TTraceId& parent,
        const TBufferLookupDiagnostics& lookup,
        const TTimeWindow& executeBounds, TSpanBudget& budget) {
    TTimeWindow bounds;
    for (const auto& shard : lookup.Shards) {
        if (!shard.GetStartTimeMs() || shard.GetFinishTimeMs() < shard.GetStartTimeMs()) {
            continue;
        }
        const TInstant start = TInstant::MilliSeconds(shard.GetStartTimeMs());
        const TInstant end = TInstant::MilliSeconds(shard.GetFinishTimeMs());
        bounds.Start = bounds.Start == TInstant::Zero() ? start : Min(bounds.Start, start);
        bounds.End = Max(bounds.End, end);
    }
    if (bounds.Start != TInstant::Zero() && bounds.End == bounds.Start) {
        bounds.End += TDuration::MicroSeconds(1);
    }
    bounds = FitUserFacingRemoteWindow(bounds, executeBounds);
    if (!bounds) {
        return;
    }
    NWilson::TSpan span = MakePhase(parent, bounds.Start, bounds.End,
        "Buffer lookup", {}, &budget, TQueryLevels::Detailed);
    if (!span) {
        return;
    }
    span.Attribute("ydb.actor.type", TString("TKqpBufferLookupActor"));
    span.Attribute("ydb.code.component", TString("KqpBufferLookup"));
    EmitShardReadSpans(span.GetTraceId(), lookup.Shards, bounds, budget);
    if (lookup.ShardsTruncated > 0) {
        span.Attribute("ydb.shards_truncated", static_cast<i64>(lookup.ShardsTruncated));
    }
    span.End();
}

void RenderExecution(const NWilson::TTraceId& rootId, const TExecutionTraceSnapshot& trace,
        TSpanBudget& budget) {
    const TExecutionTimeline& tl = trace.Timeline;
    TInstant executeStart = tl.Execute.Start;
    TInstant executeEnd = tl.Execute.End;
    NWilson::TSpan executeSpan = MakePhase(rootId, executeStart, executeEnd, "Execute", {},
        &budget, TQueryLevels::Basic, ToWilsonStatus(trace.Status));
    if (!executeSpan) {
        return;
    }
    executeSpan.Attribute("ydb.actor.type", trace.ExecuterActorType);
    if (trace.Status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
        executeSpan.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(trace.Status));
    }
    const NWilson::TTraceId executeId = executeSpan.GetTraceId();

    struct TPhaseName {
        EExecutionPhase Phase;
        const char* DisplayName;
        const char* MachineName;
    };
    static constexpr TPhaseName preparePhases[] = {
        {EExecutionPhase::ResolveTables, "Resolve tables", "ResolveTables"},
        {EExecutionPhase::ResolveShards, "Locate shards", "ResolveShards"},
        {EExecutionPhase::Snapshot, "Acquire snapshot", "Snapshot"},
    };
    TInstant prepareStart = TInstant::Max();
    TInstant prepareEnd = TInstant::Zero();
    for (const auto& phaseInfo : preparePhases) {
        if (const auto& window = tl.Phase(phaseInfo.Phase)) {
            prepareStart = Min(prepareStart, window.Start);
            prepareEnd = Max(prepareEnd, window.End);
        }
    }
    if (NWilson::TSpan prepareSpan = MakePhase(executeId, prepareStart, prepareEnd, "Prepare", {},
            &budget, TQueryLevels::Detailed, PhaseStatus(trace, {
                EExecutionPhase::ResolveTables,
                EExecutionPhase::ResolveShards,
                EExecutionPhase::Snapshot,
                EExecutionPhase::ResolveMetadata,
                EExecutionPhase::ResolvePartitioning,
            }))) {
        prepareSpan.Attribute("ydb.code.component", TString("KqpExecuter.Prepare"));
        for (const auto& [phase, displayName, machineName] : preparePhases) {
            const auto& window = tl.Phase(phase);
            if (!window) {
                continue;
            }
            if (phase == EExecutionPhase::ResolveTables) {
                // Resolve windows can overlap.
                if (NWilson::TSpan rt = MakePhase(prepareSpan.GetTraceId(), window.Start, window.End, displayName, {
                        {"ydb.phase", TString(machineName)},
                    },
                        &budget, TQueryLevels::Detailed, PhaseStatus(trace, {
                            EExecutionPhase::ResolveTables,
                            EExecutionPhase::ResolveMetadata,
                            EExecutionPhase::ResolvePartitioning,
                        }))) {
                    rt.Attribute("ydb.actor.type", TString("TKqpTableResolver"));
                    if (const auto& w = tl.Phase(EExecutionPhase::ResolveMetadata)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Metadata", {
                            {"ydb.actor.type", TString("TKqpTableResolver")},
                            {"ydb.peer.actor.type", TString("SchemeCache")},
                        }, &budget, TQueryLevels::Detailed,
                            PhaseStatus(trace, {EExecutionPhase::ResolveMetadata}));
                    }
                    if (const auto& w = tl.Phase(EExecutionPhase::ResolvePartitioning)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Partitioning", {
                            {"ydb.actor.type", TString("TKqpTableResolver")},
                            {"ydb.peer.actor.type", TString("SchemeCache")},
                        }, &budget, TQueryLevels::Detailed,
                            PhaseStatus(trace, {EExecutionPhase::ResolvePartitioning}));
                    }
                    rt.End();
                }
            } else if (phase == EExecutionPhase::ResolveShards) {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, displayName, {
                    {"ydb.phase", TString(machineName)},
                    {"ydb.actor.type", TString("TKqpShardsResolver")},
                }, &budget, TQueryLevels::Detailed,
                    PhaseStatus(trace, {EExecutionPhase::ResolveShards}));
            } else {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, displayName, {
                    {"ydb.phase", TString(machineName)},
                    {"ydb.actor.type", TString("TKqpDataExecuter")},
                    {"ydb.peer.actor.type", TString("TLongTxService")},
                }, &budget, TQueryLevels::Detailed,
                    PhaseStatus(trace, {EExecutionPhase::Snapshot}));
            }
        }
        prepareSpan.End();
    }

    TTimeWindow runBounds = tl.Phase(EExecutionPhase::RunTasks);
    if (!runBounds) {
        runBounds = GetStageBounds(trace, {});
    }
    NWilson::TSpan runSpan;
    const TInstant runStart = runBounds.Start;
    const TInstant runEnd = runBounds.End;
    runSpan = MakePhase(executeId, runStart, runEnd, "Run", {}, &budget, TQueryLevels::Basic,
        PhaseStatus(trace, {EExecutionPhase::RunTasks}));
    if (runSpan) {
        runSpan.Attribute("ydb.actor.type", trace.ComputeActorType);
        runSpan.Attribute("ydb.code.component", TString("DqExecution"));
        if (trace.StagesTruncated > 0) {
            runSpan.Attribute("ydb.stages_truncated", static_cast<i64>(trace.StagesTruncated));
        }
    }
    const NWilson::TTraceId runId = runSpan ? runSpan.GetTraceId() : NWilson::TTraceId{};
    EmitStageSpans(runSpan ? runId : executeId, trace, runBounds, budget);
    if (runSpan) {
        runSpan.End();
    }
    EmitBufferLookupSpan(executeId, trace.BufferLookup, tl.Execute, budget);
    if (const auto& flush = tl.Phase(EExecutionPhase::FlushEffects)) {
        EmitPhase(executeId, flush.Start, flush.End, "Flush effects", {
            {"ydb.phase", TString("FlushEffects")},
            {"ydb.actor.type", TString("TKqpBufferWriteActor")},
        }, &budget, TQueryLevels::Basic,
            PhaseStatus(trace, {EExecutionPhase::FlushEffects}));
    }
    if (const auto& rollback = tl.Phase(EExecutionPhase::Rollback)) {
        EmitPhase(executeId, rollback.Start, rollback.End, "Rollback", {
            {"ydb.phase", TString("Rollback")},
            {"ydb.actor.type", TString("TKqpBufferWriteActor")},
        }, &budget, TQueryLevels::Basic,
            PhaseStatus(trace, {EExecutionPhase::Rollback}));
    }
    if (const auto& commit = tl.Phase(EExecutionPhase::Commit)) {
        // Per-shard children end at each acknowledgement, exposing commit stragglers.
        if (NWilson::TSpan commitSpan = MakePhase(executeId, commit.Start, commit.End, "Commit", {},
                &budget, TQueryLevels::Basic, PhaseStatus(trace, {EExecutionPhase::Commit}))) {
            commitSpan.Attribute("ydb.actor.type", TString("TKqpBufferWriteActor"));
            const size_t truncated = Max(trace.Commit.PreparedShardsTruncated,
                trace.Commit.CommittedShardsTruncated);
            if (truncated > 0) {
                commitSpan.Attribute("ydb.shards_truncated",
                    static_cast<i64>(truncated));
            }
            if (const auto& w = trace.Commit.PrepareShards) {
                EmitCommitShardPhase(commitSpan.GetTraceId(), w,
                    "Prepare shards", "CommitPrepareShards", "Prepare",
                    trace.Commit.PreparedShards, trace.Commit.PreparedShardsTruncated, budget);
            }
            if (const auto& w = trace.Commit.Coordinator) {
                EmitPhase(commitSpan.GetTraceId(), w.Start, w.End, "Coordinator", {
                    {"ydb.actor.type", TString("TKqpBufferWriteActor")},
                    {"ydb.peer.actor.type", TString("TxCoordinator")},
                }, &budget, TQueryLevels::Detailed);
            }
            if (const auto& w = trace.Commit.ApplyShards) {
                EmitCommitShardPhase(commitSpan.GetTraceId(), w,
                    "Apply commit", "CommitApplyShards", "Commit",
                    trace.Commit.CommittedShards, trace.Commit.CommittedShardsTruncated, budget);
            }
            commitSpan.End();
        }
    }
    executeSpan.End();
}

void BuildPhases(NWilson::TSpan& userSpan, const NWilson::TTraceId& parentId,
        const TUserFacingQuerySnapshot& state, TSpanBudget& budget) {
    // Metrics and trace totals are accumulated before bounded diagnostics retention.
    userSpan.Attribute("ydb.consumed_ru", static_cast<i64>(state.Metrics.ConsumedRu));
    userSpan.Attribute("ydb.rows_read", static_cast<i64>(state.Metrics.RowsRead));
    userSpan.Attribute("ydb.rows_written", static_cast<i64>(state.Metrics.RowsWritten));
    userSpan.Attribute("ydb.bytes_read", static_cast<i64>(state.Metrics.BytesRead));
    userSpan.Attribute("ydb.cpu_us", static_cast<i64>(state.ExecutionTraceTotals.CpuUs));
    if (state.ExecutionTraceTotals.WaitUs > 0) {
        userSpan.Attribute("ydb.wait_us", static_cast<i64>(state.ExecutionTraceTotals.WaitUs));
    }
    if (state.ExecutionTraceTotals.SpilledBytes > 0) {
        userSpan.Attribute("ydb.spilled_bytes", static_cast<i64>(state.ExecutionTraceTotals.SpilledBytes));
    }
    if (state.ExecutionTraceTotals.MaxTaskSkew > 1.0) {
        userSpan.Attribute("ydb.max_task_skew", state.ExecutionTraceTotals.MaxTaskSkew);
    }
    if (state.Metrics.LocksBrokenAsVictim > 0) {
        userSpan.Attribute("ydb.locks_broken_as_victim", static_cast<i64>(state.Metrics.LocksBrokenAsVictim));
    }
    if (state.Metrics.LocksBrokenAsBreaker > 0) {
        userSpan.Attribute("ydb.locks_broken_as_breaker", static_cast<i64>(state.Metrics.LocksBrokenAsBreaker));
    }

    if (state.AdmissionStartedAt != TInstant::Zero()
            && state.AdmissionFinishedAt > state.AdmissionStartedAt) {
        const TString& poolId = state.PoolId;
        const auto status = ToWilsonStatus(state.AdmissionStatus);
        if (poolId) {
            EmitPhase(parentId, state.AdmissionStartedAt, state.AdmissionFinishedAt, "Queued", {
                {"ydb.pool_id", poolId},
                {"ydb.peer.actor.type", TString("WorkloadService")},
                {"ydb.status_code", Ydb::StatusIds::StatusCode_Name(state.AdmissionStatus)},
            }, &budget, TQueryLevels::Basic, status);
        } else {
            EmitPhase(parentId, state.AdmissionStartedAt, state.AdmissionFinishedAt, "Queued", {
                {"ydb.peer.actor.type", TString("WorkloadService")},
                {"ydb.status_code", Ydb::StatusIds::StatusCode_Name(state.AdmissionStatus)},
            }, &budget, TQueryLevels::Basic, status);
        }
    }

    for (const auto& attempt : state.CompileAttempts) {
        if (attempt.FromCache && attempt.End <= attempt.Start) {
            userSpan.Attribute("ydb.compile.cache_hit", true);
            continue;
        }
        if (NWilson::TSpan compile = MakePhase(parentId, attempt.Start, attempt.End, "Compile",
                {{"ydb.compile.cache_hit", attempt.FromCache}}, &budget, TQueryLevels::Basic,
                ToWilsonStatus(attempt.Status))) {
            compile.Attribute("ydb.actor.type", TString("TKqpCompileService"));
            if (attempt.Partial) {
                compile.Attribute("ydb.trace.coverage", TString("joined_in_progress"));
            }
            if (attempt.Status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
                compile.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(attempt.Status));
            }
            auto emitDependencies = [&](const NWilson::TTraceId& dependencyParent,
                    TInstant windowStart, TInstant windowEnd) {
                if (!attempt.Dependencies) {
                    return;
                }
                for (const auto& dependency : attempt.Dependencies->Dependencies) {
                    const bool metadata = dependency.Dependency == ECompileDependency::SchemeCache;
                    TStringBuilder name;
                    name << (metadata ? "Load metadata" : "Load statistics");
                    if (dependency.Target) {
                        name << " " << dependency.Target;
                    }
                    const TInstant start = Max(dependency.Start, windowStart);
                    const TInstant end = Min(dependency.End, windowEnd);
                    const auto status = dependency.Status == ECompileDependencyStatus::Ok
                        ? NWilson::NTraceProto::Status::STATUS_CODE_OK
                        : dependency.Status == ECompileDependencyStatus::Error
                            ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                            : NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
                    if (NWilson::TSpan child = MakePhase(dependencyParent, start, end, TString(name), {},
                            &budget, TQueryLevels::Detailed, status)) {
                        child.Attribute("ydb.actor.type", TString("TActorRequestHandler"));
                        child.Attribute("ydb.code.component", TString("KqpTableMetadataLoader"));
                        child.Attribute("ydb.peer.actor.type", TString(metadata ? "SchemeCache" : "StatisticsService"));
                        child.Attribute("ydb.compile_dependency.purpose",
                            TString(CompileDependencyPurposeName(dependency.Purpose)));
                        if (dependency.Target) {
                            child.Attribute("db.collection.name", dependency.Target);
                        }
                        child.Attribute("ydb.status_code",
                            dependency.Status == ECompileDependencyStatus::Ok ? TString("SUCCESS")
                            : dependency.Status == ECompileDependencyStatus::Error ? TString("ERROR")
                            : TString("UNKNOWN"));
                        child.End();
                    }
                }
                if (attempt.Dependencies->Dropped > 0) {
                    compile.Attribute("ydb.dependencies_truncated",
                        static_cast<i64>(attempt.Dependencies->Dropped));
                }
            };
            bool actorEmitted = false;
            if (attempt.Actor) {
                const auto& actorWindow = *attempt.Actor;
                const TInstant actorStart = Max(actorWindow.Start, attempt.Start);
                const TInstant actorEnd = Min(actorWindow.End, attempt.End);
                if (NWilson::TSpan actor = MakePhase(
                        compile.GetTraceId(), actorStart, actorEnd, "Compile query", {},
                        &budget, TQueryLevels::Basic)) {
                    actor.Attribute("ydb.actor.type", TString("TKqpCompileActor"));
                    emitDependencies(actor.GetTraceId(), actorStart, actorEnd);
                    actor.End();
                    actorEmitted = true;
                }
            }
            if (!actorEmitted) {
                emitDependencies(compile.GetTraceId(), attempt.Start, attempt.End);
            }
            compile.End();
        }
    }
    if (state.CompileAttemptsDropped > 0) {
        userSpan.Attribute("ydb.compile_attempts_truncated",
            static_cast<i64>(state.CompileAttemptsDropped));
    }

    for (const auto& trace : state.ExecutionTraces) {
        RenderExecution(parentId, trace, budget);
    }
    if (state.ExecutionTracesDropped > 0) {
        userSpan.Attribute("ydb.executions_truncated",
            static_cast<i64>(state.ExecutionTracesDropped));
    }
    if (budget.Dropped() > 0) {
        userSpan.Attribute("ydb.spans_truncated", static_cast<i64>(budget.Dropped()));
    }
}

} // namespace

void RenderUserFacingSpan(TUserFacingQuerySnapshot state) {
    TrimCompileDependencies(state.CompileAttempts);
    NWilson::TTraceId traceId = std::move(state.TraceId);
    if (!traceId) {
        return;
    }
    const TInstant rootEnd = state.RootEnd;
    TSpanBudget budget(traceId.GetVerbosity());
    EmitProxySpans(traceId, state, budget);
    NWilson::TSpan sessionSpan = NWilson::TSpan::ConstructTerminated(
        traceId, traceId.Span(traceId.GetVerbosity()),
        state.StartTime, rootEnd,
        state.Success ? NWilson::NTraceProto::Status::STATUS_CODE_OK
                : NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
        "Session", NWilson::MakeUserFacingWilsonUploaderId());
    if (sessionSpan) {
        sessionSpan.Attribute("ydb.actor.type", TString("TKqpSessionActor"));
        sessionSpan.Attribute("db.operation.name", state.Operation ? state.Operation : state.RootName);
        sessionSpan.Attribute("ydb.status_code", state.StatusCode);
        if (state.QueryText) {
            if (const TString queryText = ProtectUserFacingQueryText(state.QueryText)) {
                sessionSpan.Attribute("db.query.text", queryText);
            }
        }
        if (state.ExecutionDelegated) {
            sessionSpan.Attribute("ydb.trace.coverage", TString("routing_session_only"));
        }
        BuildPhases(sessionSpan, sessionSpan.GetTraceId(), state, budget);
        sessionSpan.End();
    }
}

class TUserFacingTraceRendererActor
    : public NActors::TActorBootstrapped<TUserFacingTraceRendererActor> {
public:
    explicit TUserFacingTraceRendererActor(TUserFacingQuerySnapshot snapshot)
        : Snapshot(std::move(snapshot))
    {}

    void Bootstrap() {
        RenderUserFacingSpan(std::move(Snapshot));
        PassAway();
    }

private:
    TUserFacingQuerySnapshot Snapshot;
};

void RenderRejectedUserFacingSpan(TRejectedUserFacingQuerySnapshot snapshot) {
    NWilson::TTraceId traceId = std::move(snapshot.TraceId);
    if (!traceId) {
        return;
    }

    const TInstant rejectedAt = snapshot.RejectedAt;
    TSpanBudget budget(traceId.GetVerbosity());
    EmitProxySpans(traceId, snapshot.ProxyRequestHops, rejectedAt, budget);
    NWilson::TSpan session = NWilson::TSpan::ConstructTerminated(
        traceId, traceId.Span(traceId.GetVerbosity()), rejectedAt,
        rejectedAt + TDuration::MicroSeconds(1),
        NWilson::NTraceProto::Status::STATUS_CODE_ERROR, "Session",
        NWilson::MakeUserFacingWilsonUploaderId());
    if (!session) {
        return;
    }
    session.Attribute("ydb.actor.type", TString("TKqpSessionActor"));
    session.Attribute("ydb.rejected", true);
    session.Attribute("ydb.trace.coverage", TString("rejected_before_query_state"));
    session.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(snapshot.Status));
    if (const TString queryText = ProtectUserFacingQueryText(snapshot.QueryText)) {
        session.Attribute("db.query.text", queryText);
    }
    session.EndError(Ydb::StatusIds::StatusCode_Name(snapshot.Status));
}

class TRejectedUserFacingTraceRendererActor
    : public NActors::TActorBootstrapped<TRejectedUserFacingTraceRendererActor> {
public:
    explicit TRejectedUserFacingTraceRendererActor(TRejectedUserFacingQuerySnapshot snapshot)
        : Snapshot(std::move(snapshot))
    {}

    void Bootstrap() {
        RenderRejectedUserFacingSpan(std::move(Snapshot));
        PassAway();
    }

private:
    TRejectedUserFacingQuerySnapshot Snapshot;
};

void RenderProxyUserFacingTrace(TProxyUserFacingTraceSnapshot snapshot) {
    NWilson::TTraceId parentTraceId = std::move(snapshot.ParentTraceId);
    NWilson::TTraceId rootTraceId = std::move(snapshot.RootTraceId);
    if (!parentTraceId || !rootTraceId) {
        return;
    }
    const bool success = snapshot.Status == Ydb::StatusIds::SUCCESS;
    const auto spanStatus = success
        ? NWilson::NTraceProto::Status::STATUS_CODE_OK
        : NWilson::NTraceProto::Status::STATUS_CODE_ERROR;
    NWilson::TSpan root = NWilson::TSpan::ConstructTerminated(
        parentTraceId, rootTraceId, snapshot.StartedAt, snapshot.FinishedAt,
        spanStatus, snapshot.Name, NWilson::MakeUserFacingWilsonUploaderId());
    if (!root) {
        return;
    }
    root.Attribute("ydb.tracing.layer", TString("user"));
    root.Attribute("ydb.code.component", TString("KQP"));
    root.Attribute("db.system.name", TString("ydb"));
    root.Attribute("db.operation.name", snapshot.Operation);
    root.Attribute("db.response.status_code", Ydb::StatusIds::StatusCode_Name(snapshot.Status));
    root.Attribute("ydb.duration.source", TString("origin_monotonic"));
    if (AppData()) {
        root.Attribute("db.namespace", AppData()->TenantName);
    }
    if (!snapshot.HasSessionTrace) {
        root.Attribute("ydb.trace.coverage", TString("proxy_only"));
    } else if (snapshot.Coverage) {
        root.Attribute("ydb.trace.coverage", snapshot.Coverage);
    }

    const TInstant proxyEnd = snapshot.SentAt != TInstant::Zero()
        ? snapshot.SentAt : snapshot.FinishedAt;
    NWilson::TSpan proxy = NWilson::TSpan::ConstructTerminated(
        root.GetTraceId(), root.GetTraceId().Span(root.GetTraceId().GetVerbosity()),
        snapshot.StartedAt, proxyEnd,
        snapshot.SentAt == TInstant::Zero() ? spanStatus
                                            : NWilson::NTraceProto::Status::STATUS_CODE_OK,
        "KQP proxy", NWilson::MakeUserFacingWilsonUploaderId());
    if (proxy) {
        proxy.Attribute("ydb.actor.type", TString("TKqpProxyService"));
        proxy.Attribute("ydb.node_id", static_cast<i64>(snapshot.NodeId));
        if (snapshot.SentAt == TInstant::Zero()) {
            proxy.Attribute("ydb.rejected", true);
        }
        proxy.End();
    }
    if (snapshot.SentAt != TInstant::Zero() && snapshot.FinishedAt >= snapshot.SentAt) {
        NWilson::TSpan roundTrip = NWilson::TSpan::ConstructTerminated(
            root.GetTraceId(), root.GetTraceId().Span(root.GetTraceId().GetVerbosity()),
            snapshot.SentAt, snapshot.FinishedAt, spanStatus,
            "KQP session round trip", NWilson::MakeUserFacingWilsonUploaderId());
        if (roundTrip) {
            roundTrip.Attribute("ydb.actor.type", TString("TKqpSessionActor"));
            roundTrip.Attribute("ydb.source_node_id", static_cast<i64>(snapshot.NodeId));
            roundTrip.Attribute("ydb.target_node_id", static_cast<i64>(snapshot.TargetNodeId));
            roundTrip.Attribute("ydb.forwarded", snapshot.NodeId != snapshot.TargetNodeId);
            roundTrip.Attribute("ydb.duration.source", TString("origin_monotonic"));
            roundTrip.End();
        }
    }
    if (snapshot.SentAt != TInstant::Zero() && snapshot.NodeId != snapshot.TargetNodeId) {
        NWilson::TSpan forwarding = NWilson::TSpan::ConstructTerminated(
            root.GetTraceId(), root.GetTraceId().Span(root.GetTraceId().GetVerbosity()),
            snapshot.SentAt, snapshot.SentAt, NWilson::NTraceProto::Status::STATUS_CODE_OK,
            "Forward to KQP proxy", NWilson::MakeUserFacingWilsonUploaderId());
        if (forwarding) {
            forwarding.Attribute("ydb.actor.type", TString("InterconnectProxy"));
            forwarding.Attribute("ydb.source_node_id", static_cast<i64>(snapshot.NodeId));
            forwarding.Attribute("ydb.target_node_id", static_cast<i64>(snapshot.TargetNodeId));
            forwarding.Attribute("ydb.duration.measured", false);
            forwarding.End();
        }
    }
    if (success) {
        root.EndOk();
    } else {
        root.EndError(Ydb::StatusIds::StatusCode_Name(snapshot.Status));
    }
}

class TProxyUserFacingTraceRendererActor
    : public NActors::TActorBootstrapped<TProxyUserFacingTraceRendererActor> {
public:
    explicit TProxyUserFacingTraceRendererActor(TProxyUserFacingTraceSnapshot snapshot)
        : Snapshot(std::move(snapshot))
    {}

    void Bootstrap() {
        RenderProxyUserFacingTrace(std::move(Snapshot));
        PassAway();
    }

private:
    TProxyUserFacingTraceSnapshot Snapshot;
};


NActors::IActor* CreateUserFacingTraceRendererActor(TUserFacingQuerySnapshot snapshot) {
    return new TUserFacingTraceRendererActor(std::move(snapshot));
}

NActors::IActor* CreateRejectedUserFacingTraceRendererActor(
        TRejectedUserFacingQuerySnapshot snapshot) {
    return new TRejectedUserFacingTraceRendererActor(std::move(snapshot));
}

NActors::IActor* CreateProxyUserFacingTraceRendererActor(
        TProxyUserFacingTraceSnapshot snapshot) {
    return new TProxyUserFacingTraceRendererActor(std::move(snapshot));
}

} // namespace NKikimr::NKqp
