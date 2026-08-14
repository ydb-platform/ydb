#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/core/kqp/common/kqp_execution_trace.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/generic/utility.h>
#include <util/generic/vector.h>

#include <algorithm>
#include <functional>
#include <vector>
#include <util/string/builder.h>

// Renders the user-facing trace at reply time from collected timings and stats.

namespace NKikimr::NKqp {

TTimeWindow FitUserFacingRemoteWindow(TTimeWindow window, const TTimeWindow& parent) {
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

namespace {

using TPhaseAttrs = std::initializer_list<std::pair<TString, NWilson::TAttributeValue>>;
using TQueryLevels = TComponentTracingLevels::TQueryProcessor;

using TSpanBudget = TUserFacingSpanBudget;

NWilson::TSpan MakePhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}, TSpanBudget* budget = nullptr,
        ui8 requiredVerbosity = TQueryLevels::TopLevel,
        NWilson::NTraceProto::Status::StatusCode status = NWilson::NTraceProto::Status::STATUS_CODE_OK) {
    if (start == TInstant::Zero() || end == TInstant::Zero() || end <= start) {
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
        ui8 requiredVerbosity = TQueryLevels::TopLevel) {
    if (budget && !budget->Admit(requiredVerbosity)) {
        return;
    }
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parentId, parentId.Span(parentId.GetVerbosity()), at, at,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, name,
        NWilson::MakeUserFacingWilsonUploaderId());
    for (const auto& [key, value] : attrs) {
        span.Attribute(key, value);
    }
    span.End();
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
        if (shard.GetStartTimeMs() == 0 || shard.GetFinishTimeMs() < shard.GetStartTimeMs()) {
            continue;
        }
        const auto bounds = FitUserFacingRemoteWindow({
            TInstant::MilliSeconds(shard.GetStartTimeMs()),
            TInstant::MilliSeconds(shard.GetFinishTimeMs()),
        }, parentBounds);
        if (!bounds) {
            continue;
        }
        if (!budget.Admit(TQueryLevels::Diagnostic)) {
            continue;
        }
        const auto status = shard.GetStatus();
        const bool failed = status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
            && status != Ydb::StatusIds::SUCCESS;
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            parent, parent.Span(parent.GetVerbosity()), bounds.Start, bounds.End,
            failed ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                   : NWilson::NTraceProto::Status::STATUS_CODE_OK,
            ShardDisplayName("Read from", shard.GetShardId()),
            NWilson::MakeUserFacingWilsonUploaderId());
        if (!span) {
            continue;
        }
        span.Attribute("ydb.shard_id", static_cast<i64>(shard.GetShardId()));
        span.Attribute("ydb.code.component", TString("KqpShardRead"));
        span.Attribute("ydb.peer.actor.type", TString("DataShard"));
        span.Attribute("ydb.rows", static_cast<i64>(shard.GetRows()));
        if (status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
            span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
            span.Attribute("ydb.finished", shard.GetFinished());
        }
        if (shard.GetRetries() > 0) {
            span.Attribute("ydb.read_retries", static_cast<i64>(shard.GetRetries()));
        }
        if (shard.GetNodeId()) {
            span.Attribute("ydb.node_id", static_cast<i64>(shard.GetNodeId()));
        }
        if (shard.GetTiming() == NKqpProto::TKqpShardReadStats::FIRST_TO_LAST_MESSAGE) {
            span.Attribute("ydb.timing_boundary", TString("first_to_last_message"));
        }
        span.End();
    }
}

void EmitTaskSpans(const NWilson::TTraceId& stageParent, const TString& stageVerb,
        const TString& actorType, const std::vector<TTaskTraceSnapshot>& tasks,
        const TTimeWindow& stageBounds, TSpanBudget& budget) {
    for (const auto& task : tasks) {
        const auto bounds = FitUserFacingRemoteWindow(task.Window, stageBounds);
        if (!bounds) {
            continue;
        }
        if (!budget.Admit(TQueryLevels::Detailed)) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            stageParent, stageParent.Span(stageParent.GetVerbosity()),
            bounds.Start, bounds.End,
            NWilson::NTraceProto::Status::STATUS_CODE_OK,
            TStringBuilder() << stageVerb << " task " << task.TaskId,
            NWilson::MakeUserFacingWilsonUploaderId());
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
        const TStageDescription description = DescribeStage(stage);
        const auto stageBounds = stage.Window ? FitUserFacingRemoteWindow(stage.Window, runBounds) : runBounds;
        if (!stageBounds) {
            continue;
        }
        if (!budget.Admit(TQueryLevels::Detailed)) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            parent, parent.Span(parent.GetVerbosity()),
            stageBounds.Start, stageBounds.End,
            NWilson::NTraceProto::Status::STATUS_CODE_OK, description.Name,
            NWilson::MakeUserFacingWilsonUploaderId());
        if (!span) {
            continue;
        }
        span.Attribute("ydb.stage_id", static_cast<i64>(stage.StageId));
        span.Attribute("ydb.actor.type", trace.ComputeActorType);
        span.Attribute("ydb.timing_source", TString("compute_actor_stats"));
        span.Attribute("ydb.tasks", static_cast<i64>(stage.Tasks));
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
        if (stage.Tasks > 1 && stage.Durations.Count > 0) {
            span.Attribute("ydb.slowest_task_node", static_cast<i64>(stage.SlowestTaskNode));
            span.Attribute("ydb.fastest_task_node", static_cast<i64>(stage.FastestTaskNode));
        }
        if (stage.Tasks > stage.InterestingTasks.size()) {
            span.Attribute("ydb.tasks_truncated",
                static_cast<i64>(stage.Tasks - stage.InterestingTasks.size()));
        }
        EmitTaskSpans(span.GetTraceId(), description.Verb, trace.ComputeActorType,
            stage.InterestingTasks, stageBounds, budget);
        span.End();
    }
}

void EmitProxySpans(const NWilson::TTraceId& parentId, const TKqpQueryState& state,
        TSpanBudget& budget) {
    const auto& hops = state.ProxyRequestHops;
    std::vector<TTimeWindow> windows(hops.size());
    TInstant cursor = state.StartTime;
    for (int i = static_cast<int>(hops.size()) - 1; i >= 0; --i) {
        const TDuration duration = TDuration::MicroSeconds(hops[i].GetDurationUs());
        windows[i] = {cursor - duration, cursor};
        cursor -= duration;
    }
    for (size_t i = 0; i < hops.size(); ++i) {
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
        if (i + 1 >= hops.size() || hop.GetNodeId() == hop.GetTargetNodeId()) {
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

TInstant GetProxyTimelineStart(const TKqpQueryState& state) {
    return state.ProxyRequestStartTime;
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
    if (!budget.Admit(TQueryLevels::Detailed)) {
        return;
    }
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parent, parent.Span(parent.GetVerbosity()), bounds.Start, bounds.End,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, "Buffer lookup",
        NWilson::MakeUserFacingWilsonUploaderId());
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
        &budget, TQueryLevels::Basic);
    if (!executeSpan) {
        return;
    }
    executeSpan.Attribute("ydb.actor.type", trace.ExecuterActorType);
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
            &budget, TQueryLevels::Detailed)) {
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
                        &budget, TQueryLevels::Detailed)) {
                    rt.Attribute("ydb.actor.type", TString("TKqpTableResolver"));
                    if (const auto& w = tl.Phase(EExecutionPhase::ResolveMetadata)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Metadata", {
                            {"ydb.actor.type", TString("TKqpTableResolver")},
                            {"ydb.peer.actor.type", TString("SchemeCache")},
                        }, &budget, TQueryLevels::Detailed);
                    }
                    if (const auto& w = tl.Phase(EExecutionPhase::ResolvePartitioning)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Partitioning", {
                            {"ydb.actor.type", TString("TKqpTableResolver")},
                            {"ydb.peer.actor.type", TString("SchemeCache")},
                        }, &budget, TQueryLevels::Detailed);
                    }
                    rt.End();
                }
            } else if (phase == EExecutionPhase::ResolveShards) {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, displayName, {
                    {"ydb.phase", TString(machineName)},
                    {"ydb.actor.type", TString("TKqpShardsResolver")},
                }, &budget, TQueryLevels::Detailed);
            } else {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, displayName, {
                    {"ydb.phase", TString(machineName)},
                    {"ydb.actor.type", TString("TKqpDataExecuter")},
                    {"ydb.peer.actor.type", TString("TLongTxService")},
                }, &budget, TQueryLevels::Detailed);
            }
        }
        prepareSpan.End();
    }

    TTimeWindow runBounds = tl.Phase(EExecutionPhase::RunTasks);
    if (!runBounds) {
        runBounds = tl.Execute;
    }
    const auto stageBounds = GetStageBounds(trace, runBounds);
    if (!runBounds) {
        runBounds = stageBounds;
    }
    NWilson::TSpan runSpan;
    const TInstant runStart = runBounds.Start;
    const TInstant runEnd = runBounds.End;
    runSpan = MakePhase(executeId, runStart, runEnd, "Run", {}, &budget, TQueryLevels::Basic);
    if (runSpan) {
        runSpan.Attribute("ydb.actor.type", trace.ComputeActorType);
        runSpan.Attribute("ydb.code.component", TString("DqExecution"));
    }
    const NWilson::TTraceId runId = runSpan ? runSpan.GetTraceId() : NWilson::TTraceId{};
    EmitStageSpans(runSpan ? runId : executeId, trace, runBounds, budget);
    if (runSpan) {
        runSpan.End();
    }
    EmitBufferLookupSpan(executeId, trace.BufferLookup, tl.Execute, budget);
    if (const auto& commit = tl.Phase(EExecutionPhase::Commit)) {
        // Per-shard children end at each acknowledgement, exposing commit stragglers.
        if (NWilson::TSpan commitSpan = MakePhase(executeId, commit.Start, commit.End, "Commit", {},
                &budget, TQueryLevels::Basic)) {
            commitSpan.Attribute("ydb.actor.type", TString("TKqpBufferWriteActor"));
            const auto& acks = trace.Commit.Shards;
            if (trace.Commit.ShardsTruncated > 0) {
                commitSpan.Attribute("ydb.shards_truncated",
                    static_cast<i64>(trace.Commit.ShardsTruncated));
            }
            if (const auto& w = trace.Commit.PrepareShards) {
                if (NWilson::TSpan prep = MakePhase(commitSpan.GetTraceId(), w.Start, w.End, "Prepare shards", {
                        {"ydb.phase", TString("CommitPrepareShards")},
                    },
                        &budget, TQueryLevels::Detailed)) {
                    prep.Attribute("ydb.actor.type", TString("TKqpBufferWriteActor"));
                    prep.Attribute("ydb.peer.actor.type", TString("DataShard"));
                    for (const auto& ack : acks) {
                        if (ack.PreparedAt >= w.Start) {
                            EmitPhase(prep.GetTraceId(), w.Start, ack.PreparedAt,
                                ShardDisplayName("Prepare", ack.ShardId),
                                {
                                    {"ydb.shard_id", static_cast<i64>(ack.ShardId)},
                                    {"ydb.actor.type", TString("TKqpBufferWriteActor")},
                                    {"ydb.peer.actor.type", TString("DataShard")},
                                }, &budget, TQueryLevels::Diagnostic);
                        }
                    }
                    prep.End();
                }
            }
            if (const auto& w = trace.Commit.Coordinator) {
                EmitPhase(commitSpan.GetTraceId(), w.Start, w.End, "Coordinator", {
                    {"ydb.actor.type", TString("TKqpBufferWriteActor")},
                    {"ydb.peer.actor.type", TString("TxCoordinator")},
                }, &budget, TQueryLevels::Detailed);
            }
            if (const auto& w = trace.Commit.ApplyShards) {
                if (NWilson::TSpan apply = MakePhase(commitSpan.GetTraceId(), w.Start, w.End, "Apply commit", {
                        {"ydb.phase", TString("CommitApplyShards")},
                    },
                        &budget, TQueryLevels::Detailed)) {
                    apply.Attribute("ydb.actor.type", TString("TKqpBufferWriteActor"));
                    apply.Attribute("ydb.peer.actor.type", TString("DataShard"));
                    for (const auto& ack : acks) {
                        if (ack.CommittedAt >= w.Start) {
                            EmitPhase(apply.GetTraceId(), w.Start, ack.CommittedAt,
                                ShardDisplayName("Commit", ack.ShardId),
                                {
                                    {"ydb.shard_id", static_cast<i64>(ack.ShardId)},
                                    {"ydb.actor.type", TString("TKqpBufferWriteActor")},
                                    {"ydb.peer.actor.type", TString("DataShard")},
                                }, &budget, TQueryLevels::Diagnostic);
                        }
                    }
                    apply.End();
                }
            }
            commitSpan.End();
        }
    }
    executeSpan.End();
}

void BuildPhases(NWilson::TSpan& userSpan, const NWilson::TTraceId& parentId,
        const TKqpQueryState& state, TSpanBudget& budget) {
    i64 rowsRead = 0;
    i64 rowsWritten = 0;
    i64 bytesRead = 0;
    ui64 cpuUs = 0;
    ui64 waitUs = 0;
    ui64 spilledBytes = 0;
    double maxSkew = 0.0;
    // Row totals cover every execution; detailed signals use the trace-depth snapshot.
    for (const auto& e : state.QueryStats.Executions) {
        for (const auto& table : e.GetTables()) {
            rowsRead += table.GetReadRows();
            rowsWritten += table.GetWriteRows();
            bytesRead += table.GetReadBytes();
        }
    }
    for (const auto& trace : state.QueryStats.ExecutionTraces) {
        cpuUs += trace.CpuUs;
        for (const auto& stage : trace.Stages) {
            waitUs += stage.WaitUs;
            spilledBytes += stage.SpilledBytes;
            if (stage.Durations.Count > 1 && stage.Durations.SumUs > 0) {
                const double average = static_cast<double>(stage.Durations.SumUs) / stage.Durations.Count;
                maxSkew = Max(maxSkew, static_cast<double>(stage.Durations.MaxUs) / average);
            }
        }
    }

    userSpan.Attribute("ydb.consumed_ru", static_cast<i64>(CalcRequestUnit(state.QueryStats)));
    userSpan.Attribute("ydb.rows_read", rowsRead);
    userSpan.Attribute("ydb.rows_written", rowsWritten);
    userSpan.Attribute("ydb.bytes_read", bytesRead);
    userSpan.Attribute("ydb.cpu_us", static_cast<i64>(cpuUs));
    if (waitUs > 0) {
        userSpan.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
    }
    if (spilledBytes > 0) {
        userSpan.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
    }
    if (maxSkew > 1.0) {
        userSpan.Attribute("ydb.max_task_skew", maxSkew);
    }
    if (state.QueryStats.LocksBrokenAsVictim > 0) {
        userSpan.Attribute("ydb.locks_broken_as_victim", static_cast<i64>(state.QueryStats.LocksBrokenAsVictim));
    }
    if (state.QueryStats.LocksBrokenAsBreaker > 0) {
        userSpan.Attribute("ydb.locks_broken_as_breaker", static_cast<i64>(state.QueryStats.LocksBrokenAsBreaker));
    }

    if (state.ContinueTime != TInstant::Zero() && state.ContinueTime > state.StartTime) {
        const TString poolId = state.UserRequestContext ? state.UserRequestContext->PoolId : TString();
        if (poolId) {
            EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued", {
                {"ydb.pool_id", poolId},
                {"ydb.peer.actor.type", TString("WorkloadService")},
            }, &budget, TQueryLevels::Basic);
        } else {
            EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued", {
                {"ydb.peer.actor.type", TString("WorkloadService")},
            }, &budget, TQueryLevels::Basic);
        }
    }

    if (state.CompileWallStart && state.CompileWallEnd > state.CompileWallStart) {
        if (NWilson::TSpan compile = MakePhase(parentId, state.CompileWallStart, state.CompileWallEnd, "Compile",
                {{"ydb.compile.cache_hit", state.CompileStats.FromCache}}, &budget, TQueryLevels::Basic)) {
            compile.Attribute("ydb.actor.type", TString("TKqpCompileService"));
            auto emitDependencies = [&](const NWilson::TTraceId& dependencyParent,
                    TInstant windowStart, TInstant windowEnd) {
                if (!state.UserFacingCompileSpans) {
                    return;
                }
                for (const auto& dependency : state.UserFacingCompileSpans->Spans) {
                    const bool metadata = dependency.Dependency == EUserFacingCompileDependency::SchemeCache;
                    TStringBuilder name;
                    name << (metadata ? "Load metadata" : "Load statistics");
                    if (dependency.Target) {
                        name << " " << dependency.Target;
                    }
                    const TInstant start = Max(dependency.Start, windowStart);
                    const TInstant end = Min(dependency.End, windowEnd);
                    const auto status = dependency.Status == EUserFacingCompileStatus::Ok
                        ? NWilson::NTraceProto::Status::STATUS_CODE_OK
                        : dependency.Status == EUserFacingCompileStatus::Error
                            ? NWilson::NTraceProto::Status::STATUS_CODE_ERROR
                            : NWilson::NTraceProto::Status::STATUS_CODE_UNSET;
                    if (NWilson::TSpan child = MakePhase(dependencyParent, start, end, TString(name), {},
                            &budget, TQueryLevels::Detailed, status)) {
                        child.Attribute("ydb.actor.type", TString("TActorRequestHandler"));
                        child.Attribute("ydb.code.component", TString("KqpTableMetadataLoader"));
                        child.Attribute("ydb.peer.actor.type", TString(metadata ? "SchemeCache" : "StatisticsService"));
                        if (dependency.Target) {
                            child.Attribute("db.collection.name", dependency.Target);
                        }
                        child.Attribute("ydb.status_code",
                            dependency.Status == EUserFacingCompileStatus::Ok ? TString("SUCCESS")
                            : dependency.Status == EUserFacingCompileStatus::Error ? TString("ERROR")
                            : TString("UNKNOWN"));
                        child.End();
                    }
                }
                if (state.UserFacingCompileSpans->Dropped > 0) {
                    compile.Attribute("ydb.dependencies_truncated",
                        static_cast<i64>(state.UserFacingCompileSpans->Dropped));
                }
            };
            bool actorEmitted = false;
            if (state.UserFacingCompileActorSpan) {
                const auto& actorWindow = *state.UserFacingCompileActorSpan;
                const TInstant actorStart = Max(actorWindow.Start, state.CompileWallStart);
                const TInstant actorEnd = Min(actorWindow.End, state.CompileWallEnd);
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
                emitDependencies(compile.GetTraceId(), state.CompileWallStart, state.CompileWallEnd);
            }
            compile.End();
        }
    } else if (state.CompileStats.FromCache) {
        userSpan.Attribute("ydb.compile.cache_hit", true);
    }

    for (const auto& trace : state.QueryStats.ExecutionTraces) {
        RenderExecution(parentId, trace, budget);
    }
    if (budget.Dropped() > 0) {
        userSpan.Attribute("ydb.spans_truncated", static_cast<i64>(budget.Dropped()));
    }
}

} // namespace

void UpdateUserFacingRootSpanName(TKqpQueryState& state) {
    if (!state.UserFacingTraceId || !state.PreparedQuery) {
        return;
    }
    const TString candidate = DescribeUserFacingQuery(state);
    if (!candidate) {
        return;
    }
    if (candidate == "EXECUTE SCRIPT") {
        state.UserFacingRootName = candidate;
        return;
    }
    if (!state.UserFacingRootName) {
        state.UserFacingRootName = candidate;
    } else if (state.UserFacingRootName != candidate) {
        state.UserFacingRootName = "EXECUTE SCRIPT";
    }
}

void InitializeUserFacingQueryText(TKqpQueryState& state) {
    if (state.UserFacingTraceId && state.RequestEv) {
        state.ObfuscatedQueryText = SanitizeUserFacingQueryText(state.RequestEv->GetQuery());
    }
}

void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode) {
    NWilson::TTraceId traceId = std::move(state.UserFacingTraceId);
    if (!traceId) {
        return;
    }
    const TString rootName = state.UserFacingRootName
        ? state.UserFacingRootName : FallbackUserFacingQueryName(state);
    const TInstant rootStart = GetProxyTimelineStart(state);
    const TInstant rootEnd = TInstant::Now();
    TSpanBudget budget(traceId.GetVerbosity());
    NWilson::TSpan userSpan = NWilson::TSpan::ConstructTerminated(
        traceId, traceId.Span(traceId.GetVerbosity()),
        rootStart, rootEnd,
        success ? NWilson::NTraceProto::Status::STATUS_CODE_OK
                : NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
        rootName,
        NWilson::MakeUserFacingWilsonUploaderId());
    if (!userSpan) {
        return;
    }
    userSpan.Attribute("ydb.tracing.layer", TString("user"));
    userSpan.Attribute("ydb.code.component", TString("KQP"));
    userSpan.Attribute("db.system.name", TString("ydb"));
    if (AppData()) {
        userSpan.Attribute("db.namespace", AppData()->TenantName);
    }
    // OTel db.operation.name contains the bare operation, without the target.
    const size_t verbEnd = rootName.find(' ');
    userSpan.Attribute("db.operation.name",
        verbEnd == TString::npos ? rootName : rootName.substr(0, verbEnd));
    if (state.ObfuscatedQueryText) {
        userSpan.Attribute("db.query.text", state.ObfuscatedQueryText);
    }
    EmitProxySpans(userSpan.GetTraceId(), state, budget);
    NWilson::TSpan sessionSpan = NWilson::TSpan::ConstructTerminated(
        userSpan.GetTraceId(), userSpan.GetTraceId().Span(userSpan.GetTraceId().GetVerbosity()),
        state.StartTime, rootEnd,
        success ? NWilson::NTraceProto::Status::STATUS_CODE_OK
                : NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
        "Session", NWilson::MakeUserFacingWilsonUploaderId());
    if (sessionSpan) {
        sessionSpan.Attribute("ydb.actor.type", TString("TKqpSessionActor"));
        BuildPhases(userSpan, sessionSpan.GetTraceId(), state, budget);
        sessionSpan.End();
    } else {
        BuildPhases(userSpan, userSpan.GetTraceId(), state, budget);
    }
    userSpan.Attribute("db.response.status_code", statusCode);
    if (success) {
        userSpan.EndOk();
    } else {
        userSpan.EndError(statusCode);
    }
}

} // namespace NKikimr::NKqp
