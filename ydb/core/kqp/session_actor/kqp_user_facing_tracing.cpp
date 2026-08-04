#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/core/kqp/common/kqp_user_facing_trace_data.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/security/util.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>

#include <algorithm>
#include <functional>
#include <util/string/builder.h>

// Renders the user-facing trace at reply time from collected timings and stats.

namespace NKikimr::NKqp {

namespace {

using TPhaseAttrs = std::initializer_list<std::pair<TString, NWilson::TAttributeValue>>;

NWilson::TSpan MakePhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}) {
    if (start == TInstant::Zero() || end == TInstant::Zero() || end <= start) {
        return {};
    }
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parentId, parentId.Span(parentId.GetVerbosity()), start, end,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, name,
        NWilson::MakeUserFacingWilsonUploaderId());
    for (const auto& [key, value] : attrs) {
        span.Attribute(key, value);
    }
    return span;
}

void EmitPhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}) {
    NWilson::TSpan span = MakePhase(parentId, start, end, name, attrs);
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

constexpr size_t MaxNodesInStageAttribute = 32;
constexpr size_t MaxErrorMessageBytes = 1024;
constexpr size_t PhaseSpanEstimatePerExecution = 12;

struct TSpanBudget {
    ui64 TaskDurationCutoffMs = 0;
    i64 ShardSpansRemaining = 0;
    ui64 Dropped = 0;
};

struct TStageSignals {
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    double Skew = 0.0;
};

TStageSignals CollectStageSignals(const NYql::NDqProto::TDqStageStats& stage) {
    TStageSignals signals;
    signals.WaitUs = stage.GetWaitInputTimeUs().GetSum() + stage.GetWaitOutputTimeUs().GetSum();
    signals.SpilledBytes = stage.GetSpillingComputeBytes().GetSum() + stage.GetSpillingChannelBytes().GetSum();
    const auto& taskDur = stage.GetDurationUs();
    if (stage.GetTotalTasksCount() > 1 && taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
        const double avg = static_cast<double>(taskDur.GetSum()) / taskDur.GetCnt();
        if (avg > 0 && taskDur.GetMax() > avg) {
            signals.Skew = static_cast<double>(taskDur.GetMax()) / avg;
        }
    }
    return signals;
}

struct TStageDescription {
    TString Name;
    TString Verb;
};

TStageDescription DescribeStage(const NYql::NDqProto::TDqStageStats& stage,
        const TUserFacingStageHint* hint) {
    if (stage.OperatorJoinSize() > 0) {
        return {"Join", "Join"};
    }
    if (stage.OperatorAggregationSize() > 0) {
        return {"Aggregate", "Aggregate"};
    }
    if (stage.OperatorFilterSize() > 0) {
        return {"Filter", "Filter"};
    }
    if (stage.TablesSize() > 0) {
        const auto& table = stage.GetTables(0);
        const TString verb = table.GetWriteRows().GetSum() > 0 ? "Write" : "Read";
        return {TStringBuilder() << verb << " " << table.GetTablePath(), verb};
    }
    if (hint && hint->TablePath) {
        const TString verb = hint->IsWrite ? "Write" : "Read";
        return {TStringBuilder() << verb << " " << hint->TablePath, verb};
    }
    return {TStringBuilder() << "Step " << stage.GetStageId(), "Compute"};
}

// Task timestamps are absolute; stage timestamps below are relative to BaseTimeMs.
void EmitTaskSpans(const NWilson::TTraceId& stageParent, const TString& stageVerb,
        const std::unordered_map<ui64, TUserFacingTaskSnapshot>& tasks, ui64 stageStartMs,
        TSpanBudget& budget) {
    for (const auto& [taskId, task] : tasks) {
        if (budget.TaskDurationCutoffMs && task.DurationMs() < budget.TaskDurationCutoffMs) {
            ++budget.Dropped;
            continue;
        }
        // Datashard write tasks may omit StartTimeMs.
        const ui64 startMs = task.StartTimeMs ? task.StartTimeMs
            : task.CreateTimeMs ? task.CreateTimeMs
            : stageStartMs;
        const ui64 finishMs = task.FinishTimeMs;
        if (startMs == 0 || finishMs < startMs) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            stageParent, stageParent.Span(stageParent.GetVerbosity()),
            TInstant::MilliSeconds(startMs), TInstant::MilliSeconds(finishMs),
            NWilson::NTraceProto::Status::STATUS_CODE_OK,
            TStringBuilder() << stageVerb << " task " << task.TaskId,
            NWilson::MakeUserFacingWilsonUploaderId());
        if (!span) {
            continue;
        }
        span.Attribute("ydb.task_id", static_cast<i64>(task.TaskId));
        if (task.NodeId) {
            span.Attribute("ydb.node_id", static_cast<i64>(task.NodeId));
        }
        span.Attribute("ydb.input_rows", static_cast<i64>(task.InputRows));
        span.Attribute("ydb.output_rows", static_cast<i64>(task.OutputRows));
        span.Attribute("ydb.duration_us", static_cast<i64>((finishMs - startMs) * 1000));
        if (task.CreateTimeMs && task.StartTimeMs > task.CreateTimeMs) {
            span.Attribute("ydb.queue_delay_us",
                static_cast<i64>((task.StartTimeMs - task.CreateTimeMs) * 1000));
        }
        if (task.ComputeCpuTimeUs > 0) {
            span.Attribute("ydb.compute_cpu_us", static_cast<i64>(task.ComputeCpuTimeUs));
        }
        if (task.BuildCpuTimeUs > 0) {
            span.Attribute("ydb.build_cpu_us", static_cast<i64>(task.BuildCpuTimeUs));
        }
        const ui64 waitUs = task.WaitInputTimeUs + task.WaitOutputTimeUs;
        if (waitUs > 0) {
            span.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
        }
        if (task.SpilledBytes > 0) {
            span.Attribute("ydb.spilled_bytes", static_cast<i64>(task.SpilledBytes));
        }
        if (task.ReadRetries > 0) {
            span.Attribute("ydb.read_retries", static_cast<i64>(task.ReadRetries));
        }
        for (const auto& shard : task.ShardReads) {
            if (shard.GetStartTimeMs() == 0 || shard.GetFinishTimeMs() < shard.GetStartTimeMs()) {
                continue;
            }
            if (budget.ShardSpansRemaining <= 0) {
                ++budget.Dropped;
                continue;
            }
            --budget.ShardSpansRemaining;
            NWilson::TSpan shardSpan = NWilson::TSpan::ConstructTerminated(
                span.GetTraceId(), span.GetTraceId().Span(span.GetTraceId().GetVerbosity()),
                TInstant::MilliSeconds(shard.GetStartTimeMs()),
                TInstant::MilliSeconds(shard.GetFinishTimeMs()),
                NWilson::NTraceProto::Status::STATUS_CODE_OK,
                ShardDisplayName("Read from", shard.GetShardId()),
                NWilson::MakeUserFacingWilsonUploaderId());
            if (!shardSpan) {
                continue;
            }
            shardSpan.Attribute("ydb.shard_id", static_cast<i64>(shard.GetShardId()));
            shardSpan.Attribute("ydb.rows", static_cast<i64>(shard.GetRows()));
            if (shard.GetRetries() > 0) {
                shardSpan.Attribute("ydb.read_retries", static_cast<i64>(shard.GetRetries()));
            }
            if (shard.GetNodeId()) {
                shardSpan.Attribute("ydb.node_id", static_cast<i64>(shard.GetNodeId()));
            }
            shardSpan.End();
        }
        if (task.ShardReadsTruncated > 0) {
            span.Attribute("ydb.shards_truncated", static_cast<i64>(task.ShardReadsTruncated));
        }
        span.End();
    }
}

void EmitStageSpans(const NWilson::TTraceId& parent, const TUserFacingTraceExecutionData& trace,
        TSpanBudget& budget) {
    const NYql::NDqProto::TDqExecutionStats& stats = trace.ExecStats;
    const TUserFacingTraceTaskStats& taskStats = trace.TaskStats;
    for (const auto& stage : stats.GetStages()) {
        const auto hintIt = trace.StageHints.find(stage.GetStageId());
        const TUserFacingStageHint* hint = hintIt != trace.StageHints.end() ? &hintIt->second : nullptr;
        const TStageDescription description = DescribeStage(stage, hint);
        const ui64 base = stage.GetBaseTimeMs();
        const ui64 startMs = stage.GetStartTimeMs().GetMin();
        const ui64 finishMs = stage.GetFinishTimeMs().GetMax();
        if (base == 0 || finishMs < startMs) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            parent, parent.Span(parent.GetVerbosity()),
            TInstant::MilliSeconds(base + startMs), TInstant::MilliSeconds(base + finishMs),
            NWilson::NTraceProto::Status::STATUS_CODE_OK, description.Name,
            NWilson::MakeUserFacingWilsonUploaderId());
        if (!span) {
            continue;
        }
        span.Attribute("ydb.stage_id", static_cast<i64>(stage.GetStageId()));
        span.Attribute("ydb.tasks", static_cast<i64>(stage.GetTotalTasksCount()));
        span.Attribute("ydb.cpu_us", static_cast<i64>(stage.GetCpuTimeUs().GetSum()));
        span.Attribute("ydb.input_rows", static_cast<i64>(stage.GetInputRows().GetSum()));
        span.Attribute("ydb.output_rows", static_cast<i64>(stage.GetOutputRows().GetSum()));

        const TStageSignals signals = CollectStageSignals(stage);
        if (signals.WaitUs > 0) {
            span.Attribute("ydb.wait_us", static_cast<i64>(signals.WaitUs));
        }
        if (signals.SpilledBytes > 0) {
            span.Attribute("ydb.spilled_bytes", static_cast<i64>(signals.SpilledBytes));
        }
        const auto& taskDur = stage.GetDurationUs();
        if (taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
            span.Attribute("ydb.task_duration_min_us", static_cast<i64>(taskDur.GetMin()));
            span.Attribute("ydb.task_duration_avg_us", static_cast<i64>(taskDur.GetSum() / taskDur.GetCnt()));
            span.Attribute("ydb.task_duration_max_us", static_cast<i64>(taskDur.GetMax()));
        }
        if (signals.Skew > 0) {
            span.Attribute("ydb.task_skew", signals.Skew);
        }
        if (stage.GetTotalTasksCount() > 1) {
            const auto& st = stage.GetStartTimeMs();
            const auto& fin = stage.GetFinishTimeMs();
            if (st.GetCnt() > 0 && st.GetMax() > st.GetMin()) {
                span.Attribute("ydb.task_start_spread_us", static_cast<i64>((st.GetMax() - st.GetMin()) * 1000));
                span.Attribute("ydb.task_start_avg_offset_us",
                    static_cast<i64>((st.GetSum() / st.GetCnt() - st.GetMin()) * 1000));
            }
            if (fin.GetCnt() > 0 && fin.GetMax() > fin.GetMin()) {
                span.Attribute("ydb.task_finish_spread_us", static_cast<i64>((fin.GetMax() - fin.GetMin()) * 1000));
                if (st.GetCnt() > 0 && fin.GetSum() / fin.GetCnt() >= st.GetMin()) {
                    span.Attribute("ydb.task_finish_avg_offset_us",
                        static_cast<i64>((fin.GetSum() / fin.GetCnt() - st.GetMin()) * 1000));
                }
            }
        }
        if (const auto aggIt = trace.StageAggs.find(stage.GetStageId()); aggIt != trace.StageAggs.end()) {
            const auto* agg = &aggIt->second;
            if (!agg->TasksByNode.empty()) {
                std::vector<std::pair<ui32, ui32>> nodes(agg->TasksByNode.begin(), agg->TasksByNode.end());
                std::sort(nodes.begin(), nodes.end(),
                    [](const auto& a, const auto& b) { return a.second > b.second; });
                TStringBuilder byNode;
                size_t shown = 0;
                for (const auto& [nodeId, count] : nodes) {
                    if (shown == MaxNodesInStageAttribute) {
                        byNode << ",+" << nodes.size() - shown << " nodes";
                        break;
                    }
                    if (shown++ > 0) {
                        byNode << ",";
                    }
                    byNode << nodeId << ":" << count;
                }
                span.Attribute("ydb.tasks_by_node", TString(byNode));
            }
            if (stage.GetTotalTasksCount() > 1 && agg->MaxDurationMs > 0) {
                span.Attribute("ydb.slowest_task_node", static_cast<i64>(agg->MaxDurationNode));
                span.Attribute("ydb.fastest_task_node", static_cast<i64>(agg->MinDurationNode));
            }
        }
        if (const auto stageTasksIt = taskStats.find(stage.GetStageId()); stageTasksIt != taskStats.end()) {
            const auto& stageTasks = stageTasksIt->second;
            if (stageTasks.size() >= MaxUserFacingTraceTasksPerStage
                    && stage.GetTotalTasksCount() > stageTasks.size()) {
                span.Attribute("ydb.tasks_truncated",
                    static_cast<i64>(stage.GetTotalTasksCount() - stageTasks.size()));
            }
            EmitTaskSpans(span.GetTraceId(), description.Verb, stageTasks, base + startMs, budget);
        }
        span.End();
    }
}

void RenderExecution(const NWilson::TTraceId& rootId, const TUserFacingTraceExecutionData& trace,
        TSpanBudget& budget) {
    const TUserFacingTraceTimeline& tl = trace.Timeline;
    NWilson::TSpan executeSpan = MakePhase(rootId, tl.Execute.Start, tl.Execute.End, "Execute");
    if (!executeSpan) {
        return;
    }
    const NWilson::TTraceId executeId = executeSpan.GetTraceId();

    struct TPhaseName {
        EUserFacingTracePhase Phase;
        const char* Name;
    };
    static constexpr TPhaseName preparePhases[] = {
        {EUserFacingTracePhase::ResolveTables, "ResolveTables"},
        {EUserFacingTracePhase::ResolveShards, "ResolveShards"},
        {EUserFacingTracePhase::Snapshot, "Snapshot"},
    };
    TInstant prepareStart = TInstant::Max();
    TInstant prepareEnd = TInstant::Zero();
    for (const auto& [phase, name] : preparePhases) {
        if (const auto& window = tl.Phase(phase)) {
            prepareStart = Min(prepareStart, window.Start);
            prepareEnd = Max(prepareEnd, window.End);
        }
    }
    if (NWilson::TSpan prepareSpan = MakePhase(executeId, prepareStart, prepareEnd, "Prepare")) {
        for (const auto& [phase, name] : preparePhases) {
            const auto& window = tl.Phase(phase);
            if (!window) {
                continue;
            }
            if (phase == EUserFacingTracePhase::ResolveTables) {
                // Resolve windows can overlap.
                if (NWilson::TSpan rt = MakePhase(prepareSpan.GetTraceId(), window.Start, window.End, name)) {
                    if (const auto& w = tl.Phase(EUserFacingTracePhase::ResolveMetadata)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Metadata");
                    }
                    if (const auto& w = tl.Phase(EUserFacingTracePhase::ResolvePartitioning)) {
                        EmitPhase(rt.GetTraceId(), w.Start, w.End, "Partitioning");
                    }
                    rt.End();
                }
            } else {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, name);
            }
        }
        prepareSpan.End();
    }

    NWilson::TSpan runSpan;
    if (const auto& run = tl.Phase(EUserFacingTracePhase::RunTasks)) {
        runSpan = MakePhase(executeId, run.Start, run.End, "Run");
    }
    const NWilson::TTraceId runId = runSpan ? runSpan.GetTraceId() : NWilson::TTraceId{};
    EmitStageSpans(runSpan ? runId : executeId, trace, budget);
    if (runSpan) {
        runSpan.End();
    }
    if (const auto& commit = tl.Phase(EUserFacingTracePhase::Commit)) {
        // Per-shard children end at each acknowledgement, exposing commit stragglers.
        if (NWilson::TSpan commitSpan = MakePhase(executeId, commit.Start, commit.End, "Commit")) {
            const auto& acks = trace.ShardCommitAcks;
            if (const auto& w = tl.Phase(EUserFacingTracePhase::CommitPrepareShards)) {
                if (NWilson::TSpan prep = MakePhase(commitSpan.GetTraceId(), w.Start, w.End, "PrepareShards")) {
                    for (const auto& ack : acks) {
                        if (ack.PreparedAt >= w.Start) {
                            EmitPhase(prep.GetTraceId(), w.Start, ack.PreparedAt,
                                ShardDisplayName("Prepare", ack.ShardId),
                                {{"ydb.shard_id", static_cast<i64>(ack.ShardId)}});
                        }
                    }
                    prep.End();
                }
            }
            if (const auto& w = tl.Phase(EUserFacingTracePhase::CommitCoordinator)) {
                EmitPhase(commitSpan.GetTraceId(), w.Start, w.End, "Coordinator");
            }
            if (const auto& w = tl.Phase(EUserFacingTracePhase::CommitApplyShards)) {
                if (NWilson::TSpan apply = MakePhase(commitSpan.GetTraceId(), w.Start, w.End, "ApplyShards")) {
                    for (const auto& ack : acks) {
                        if (ack.CommittedAt >= w.Start) {
                            EmitPhase(apply.GetTraceId(), w.Start, ack.CommittedAt,
                                ShardDisplayName("Commit", ack.ShardId),
                                {{"ydb.shard_id", static_cast<i64>(ack.ShardId)}});
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

void BuildPhases(NWilson::TSpan& userSpan, const TKqpQueryState& state) {
    const NWilson::TTraceId parentId = userSpan.GetTraceId();

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
    for (const auto& trace : state.QueryStats.UserFacingTraces) {
        cpuUs += trace.ExecStats.GetCpuTimeUs();
        for (const auto& stage : trace.ExecStats.GetStages()) {
            const TStageSignals signals = CollectStageSignals(stage);
            waitUs += signals.WaitUs;
            spilledBytes += signals.SpilledBytes;
            maxSkew = Max(maxSkew, signals.Skew);
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
            EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued", {{"ydb.pool_id", poolId}});
        } else {
            EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued");
        }
    }

    if (state.CompileWallStart && state.CompileWallEnd > state.CompileWallStart) {
        EmitPhase(parentId, state.CompileWallStart, state.CompileWallEnd, "Compile",
            {{"ydb.compile.cache_hit", state.CompileStats.FromCache}});
    } else if (state.CompileStats.FromCache) {
        userSpan.Attribute("ydb.compile.cache_hit", true);
    }

    // Tasks compete globally by duration; shard children consume the remaining budget.
    TSpanBudget budget;
    {
        size_t fixedSpans = 0;
        std::vector<ui64> taskDurations;
        for (const auto& trace : state.QueryStats.UserFacingTraces) {
            fixedSpans += PhaseSpanEstimatePerExecution + trace.ExecStats.StagesSize()
                + 2 * trace.ShardCommitAcks.size();
            for (const auto& [stageId, tasks] : trace.TaskStats) {
                for (const auto& [taskId, task] : tasks) {
                    taskDurations.push_back(task.DurationMs());
                }
            }
        }
        const size_t taskBudget = MaxUserFacingSpansPerQuery > fixedSpans
            ? MaxUserFacingSpansPerQuery - fixedSpans : 0;
        if (taskDurations.size() > taskBudget) {
            auto nth = taskDurations.begin() + taskBudget;
            std::nth_element(taskDurations.begin(), nth, taskDurations.end(), std::greater<ui64>());
            // Exclude tasks tied at the cutoff to avoid overshooting the budget.
            budget.TaskDurationCutoffMs = *nth + 1;
        }
        const size_t admittedTasks = Min(taskDurations.size(), taskBudget);
        budget.ShardSpansRemaining = static_cast<i64>(taskBudget - admittedTasks);
    }
    for (const auto& trace : state.QueryStats.UserFacingTraces) {
        RenderExecution(parentId, trace, budget);
    }
    if (budget.Dropped > 0) {
        userSpan.Attribute("ydb.spans_truncated", static_cast<i64>(budget.Dropped));
    }
}

const char* SinkVerb(NKikimrKqp::TKqpTableSinkSettings::EType mode) {
    switch (mode) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE: return "REPLACE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:  return "UPSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:  return "INSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:  return "DELETE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:  return "UPDATE";
        default: return nullptr;
    }
}

// Across scripts, scheme work outranks writes and writes outrank reads.
int RootNameRank(const TString& name) {
    if (!name) {
        return 0;
    }
    if (name == "DDL") {
        return 3;
    }
    return name.StartsWith("SELECT") ? 1 : 2;
}

// Use "VERB /table/path" only when the physical query has one unambiguous target.
TString RootNameFromQuery(const NKqpProto::TKqpPhyQuery& query) {
    const char* writeVerb = nullptr;
    bool hasReads = false;
    TString writeTable;
    TString readTable;
    bool multiWrite = false;
    bool multiRead = false;
    auto note = [](TString& table, bool& multi, const TString& path) {
        if (path) {
            multi = multi || (table && table != path);
            table = table ? table : path;
        }
    };
    for (const auto& tx : query.GetTransactions()) {
        if (tx.GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
            return "DDL";
        }
        for (const auto& stage : tx.GetStages()) {
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink
                        && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    if (sink.GetInternalSink().GetSettings().UnpackTo(&settings)) {
                        if (const char* verb = SinkVerb(settings.GetType())) {
                            writeVerb = writeVerb ? writeVerb : verb;
                            note(writeTable, multiWrite, settings.GetTable().GetPath());
                        }
                    }
                }
            }
            for (const auto& op : stage.GetTableOps()) {
                switch (op.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                        writeVerb = writeVerb ? writeVerb : "UPSERT";
                        note(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        writeVerb = writeVerb ? writeVerb : "DELETE";
                        note(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    default:
                        hasReads = true;
                        note(readTable, multiRead, op.GetTable().GetPath());
                        break;
                }
            }
            for (const auto& source : stage.GetSources()) {
                hasReads = true;
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    note(readTable, multiRead, source.GetReadRangesSource().GetTable().GetPath());
                }
            }
        }
    }
    if (writeVerb) {
        return multiWrite || !writeTable
            ? TString(writeVerb) : TStringBuilder() << writeVerb << " " << writeTable;
    }
    if (hasReads || query.ResultBindingsSize() > 0) {
        return multiRead || !readTable
            ? TString("SELECT") : TStringBuilder() << "SELECT " << readTable;
    }
    return {};
}

// Never fall back to raw text: literals are replaced and lexer failure returns no attribute.
TString SanitizeQueryText(const TString& text) {
    // Sensitive statements use the same protection as query logs.
    TString protectedText;
    if (NKikimr::ProtectQueryForLoggingIfSensitive(text, protectedText)) {
        return protectedText;
    }
    static const NSQLTranslationV1::TLexers lexers = [] {
        NSQLTranslationV1::TLexers l;
        l.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        l.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        return l;
    }();
    for (const bool ansi : {false, true}) {
        auto lexer = NSQLTranslationV1::MakeLexer(lexers, ansi);
        NSQLTranslation::TParsedTokenList tokens;
        NYql::TIssues issues;
        if (!NSQLTranslation::Tokenize(*lexer, text, {}, tokens, issues, /*maxErrors*/ 1)) {
            continue;
        }
        TStringBuilder out;
        for (const auto& token : tokens) {
            if (token.Name == "STRING_VALUE" || token.Name == "DIGITS"
                    || token.Name == "INTEGER_VALUE" || token.Name == "REAL" || token.Name == "BLOB") {
                out << '?';
            } else if (token.Name != "COMMENT" && token.Name != "EOF") {
                out << token.Content;
            }
        }
        return out;
    }
    return {};
}

TString FallbackRootName(const TKqpQueryState& state) {
    TString name = NKikimrKqp::EQueryAction_Name(state.GetAction());
    constexpr TStringBuf prefix = "QUERY_ACTION_";
    if (name.StartsWith(prefix)) {
        name = name.substr(prefix.size());
    }
    return name;
}

} // namespace

void UpdateUserFacingRootSpanName(TKqpQueryState& state) {
    if (!state.UserFacingTraceId || !state.PreparedQuery) {
        return;
    }
    const TString candidate = RootNameFromQuery(state.PreparedQuery->GetPhysicalQuery());
    if (RootNameRank(candidate) > RootNameRank(state.UserFacingRootName)) {
        state.UserFacingRootName = candidate;
    }
}

void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode,
        const TString& errorMessage) {
    NWilson::TTraceId traceId = std::move(state.UserFacingTraceId);
    if (!traceId) {
        return;
    }
    const TString rootName = state.UserFacingRootName ? state.UserFacingRootName : FallbackRootName(state);
    NWilson::TSpan userSpan = NWilson::TSpan::ConstructTerminated(
        traceId, traceId.Span(traceId.GetVerbosity()),
        state.StartTime, TInstant::Now(),
        NWilson::NTraceProto::Status::STATUS_CODE_OK, rootName,
        NWilson::MakeUserFacingWilsonUploaderId());
    if (!userSpan) {
        return;
    }
    userSpan.Attribute("ydb.tracing.layer", TString("user"));
    userSpan.Attribute("db.system.name", TString("ydb"));
    if (AppData()) {
        userSpan.Attribute("db.namespace", AppData()->TenantName);
    }
    // OTel db.operation.name contains the bare operation, without the target.
    const size_t verbEnd = rootName.find(' ');
    userSpan.Attribute("db.operation.name",
        verbEnd == TString::npos ? rootName : rootName.substr(0, verbEnd));
    if (const TString sanitized = SanitizeQueryText(state.RequestEv->GetQuery())) {
        userSpan.Attribute("db.query.text", sanitized);
    }
    BuildPhases(userSpan, state);
    userSpan.Attribute("db.response.status_code", statusCode);
    if (success) {
        userSpan.EndOk();
    } else {
        userSpan.EndError(errorMessage ? errorMessage.substr(0, MaxErrorMessageBytes) : statusCode);
    }
}

} // namespace NKikimr::NKqp
