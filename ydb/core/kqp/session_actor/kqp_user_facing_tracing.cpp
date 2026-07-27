#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/core/kqp/common/kqp_user_facing_trace_data.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
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

// The single renderer of the user-facing trace. Everything the user sees is built here, at reply
// time, from data the engine merely recorded: session timings (queue/compile), the executer's
// phase timeline and per-task retention (TUserFacingTraceExecutionData), and the finalized stats.
//
//   root (SQL verb [+ table])
//   ├── Queued
//   ├── Compile
//   └── Execute                (one per execution)
//       ├── Prepare
//       │   ├── ResolveTables / ResolveShards / Snapshot
//       └── Run
//           └── stage (Join / Aggregate / Read <table> / ...)
//               └── Task N

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
        NWilson::NTraceProto::Status::STATUS_CODE_OK, name);
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

// Shard spans are named by the action, not the object ("Read from shard ..."), so the tree reads
// as a story. Tablet ids are ~17 digits with a long common prefix within a table; the name keeps
// only the distinguishing tail, the full id lives in the ydb.shard_id attribute.
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

// Global span budget of one query render (see MaxUserFacingSpansPerQuery). Tasks are admitted
// by the precomputed duration cutoff (global top-K), shard children by the running counter.
struct TSpanBudget {
    ui64 TaskDurationCutoffMs = 0; // emit tasks with duration >= cutoff; 0 => all
    i64 ShardSpansRemaining = 0;
    ui64 Dropped = 0;
};

// Slowdown signals of one stage, shared between the stage span and the root-level aggregation.
struct TStageSignals {
    ui64 WaitUs = 0;       // time compute actors spent waiting on I/O rather than computing
    ui64 SpilledBytes = 0; // bytes spilled to disk under memory pressure
    double Skew = 0.0;     // slowest-task / avg-task ratio (straggler detector); 0 when N/A
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

// Short action of a stage, inherited by its task spans ("Read task 3"): a task viewed out of
// context should still say what it was doing.
TString StageShortVerb(const NYql::NDqProto::TDqStageStats& stage, const TUserFacingStageHint* hint) {
    if (stage.OperatorJoinSize() > 0) {
        return "Join";
    }
    if (stage.OperatorAggregationSize() > 0) {
        return "Aggregate";
    }
    if (stage.OperatorFilterSize() > 0) {
        return "Filter";
    }
    if (stage.TablesSize() > 0) {
        return stage.GetTables(0).GetWriteRows().GetSum() > 0 ? "Write" : "Read";
    }
    if (hint && hint->TablePath) {
        return hint->IsWrite ? "Write" : "Read";
    }
    return "Compute";
}

TString StageDisplayName(const NYql::NDqProto::TDqStageStats& stage, const TUserFacingStageHint* hint) {
    // Name by the dominant operator first; a table name only labels a pure read/write stage.
    if (stage.OperatorJoinSize() > 0) {
        return "Join";
    }
    if (stage.OperatorAggregationSize() > 0) {
        return "Aggregate";
    }
    if (stage.OperatorFilterSize() > 0) {
        return "Filter";
    }
    if (stage.TablesSize() > 0) {
        const auto& table = stage.GetTables(0);
        return TStringBuilder() << (table.GetWriteRows().GetSum() > 0 ? "Write " : "Read ") << table.GetTablePath();
    }
    // Sink-write stages carry no table info in exported stats; the executer-captured hint does.
    if (hint && hint->TablePath) {
        return TStringBuilder() << (hint->IsWrite ? "Write " : "Read ") << hint->TablePath;
    }
    return TStringBuilder() << "Step " << stage.GetStageId();
}

// Task start/finish are ABSOLUTE epoch ms (raw from the compute actor), unlike the stage
// aggregate which is offset from BaseTimeMs — so no base is added here.
void EmitTaskSpans(const NWilson::TTraceId& stageParent, const TString& stageVerb,
        const std::unordered_map<ui64, TUserFacingTaskSnapshot>& tasks, ui64 stageStartMs,
        TSpanBudget& budget) {
    for (const auto& [taskId, task] : tasks) {
        if (budget.TaskDurationCutoffMs && task.DurationMs() < budget.TaskDurationCutoffMs) {
            ++budget.Dropped;
            continue;
        }
        // Write tasks (datashard) report FinishTimeMs but leave StartTimeMs unset; fall back to
        // creation time, then to the stage's start, so the task still gets a span in-window.
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
            TStringBuilder() << stageVerb << " task " << task.TaskId);
        if (!span) {
            continue;
        }
        span.Attribute("ydb.task_id", static_cast<i64>(task.TaskId));
        if (task.NodeId) {
            span.Attribute("ydb.node_id", static_cast<i64>(task.NodeId));
        }
        span.Attribute("ydb.cpu_us", static_cast<i64>(task.CpuTimeUs));
        span.Attribute("ydb.input_rows", static_cast<i64>(task.InputRows));
        span.Attribute("ydb.output_rows", static_cast<i64>(task.OutputRows));
        // Stated explicitly because the span's own duration rounds to zero for sub-ms tasks.
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
        // Per-shard reads (collected at full stats level). Sub-ms reads render as
        // zero-length spans; the attributes still carry the detail.
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
                ShardDisplayName("Read from", shard.GetShardId()));
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
        const TUserFacingStageHint* hint = trace.StageHints.FindPtr(stage.GetStageId());
        // Stage start/finish are offsets from BaseTimeMs (absolute epoch ms); base 0 => untimed stage.
        const ui64 base = stage.GetBaseTimeMs();
        const ui64 startMs = stage.GetStartTimeMs().GetMin();
        const ui64 finishMs = stage.GetFinishTimeMs().GetMax();
        if (base == 0 || finishMs < startMs) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            parent, parent.Span(parent.GetVerbosity()),
            TInstant::MilliSeconds(base + startMs), TInstant::MilliSeconds(base + finishMs),
            NWilson::NTraceProto::Status::STATUS_CODE_OK, StageDisplayName(stage, hint));
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
        // Stated explicitly: a trace UI can only aggregate span durations, which are ms-grained.
        const auto& taskDur = stage.GetDurationUs();
        if (taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
            span.Attribute("ydb.task_duration_min_us", static_cast<i64>(taskDur.GetMin()));
            span.Attribute("ydb.task_duration_avg_us", static_cast<i64>(taskDur.GetSum() / taskDur.GetCnt()));
            span.Attribute("ydb.task_duration_max_us", static_cast<i64>(taskDur.GetMax()));
        }
        if (signals.Skew > 0) {
            span.Attribute("ydb.task_skew", signals.Skew);
        }
        // A wide finish spread next to a narrow start spread means stragglers. Averages are
        // offsets from the earliest task start, so min/avg/max of both start and finish are
        // readable together: start = [0, avg_offset, spread], finish likewise.
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
        // Task placement: which nodes ran this stage's tasks and how many each — placement skew
        // is often the real cause of a wide finish spread. Extreme-task nodes point at the
        // machine to look at; the fastest task is not retained by the top-N cap, only its node.
        if (const auto* agg = trace.StageAggs.FindPtr(stage.GetStageId())) {
            if (!agg->TasksByNode.empty()) {
                TVector<std::pair<ui32, ui32>> nodes(agg->TasksByNode.begin(), agg->TasksByNode.end());
                std::sort(nodes.begin(), nodes.end(),
                    [](const auto& a, const auto& b) { return a.second > b.second; });
                TStringBuilder byNode;
                size_t shown = 0;
                for (const auto& [nodeId, count] : nodes) {
                    if (shown == 32) {
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
            // Set only when the cap was actually hit — a task that never reported stats is
            // simply absent, not truncated.
            if (stageTasks.size() >= MaxUserFacingTraceTasksPerStage
                    && stage.GetTotalTasksCount() > stageTasks.size()) {
                span.Attribute("ydb.tasks_truncated",
                    static_cast<i64>(stage.GetTotalTasksCount() - stageTasks.size()));
            }
            EmitTaskSpans(span.GetTraceId(), StageShortVerb(stage, hint), stageTasks, base + startMs, budget);
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
                // The two scheme-cache round-trips run concurrently; their windows may overlap.
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
        // Distributed-commit breakdown (empty on the immediate single-shard path). At the
        // full-detail tier the prepare/apply windows carry per-shard children: each shard's span runs from
        // the phase start to that shard's acknowledgement, so stragglers stand out.
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
    // Row/byte totals come from the client-visible stats (exported unconditionally, and they
    // also cover executions the trace didn't record, e.g. literal ones); everything deeper
    // comes from the trace's own export — the client one is capped at the requested mode.
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
    // Lock contention: victim => this query aborted/retried due to a conflict; breaker => it
    // invalidated another tx's locks. Either explains latency that isn't CPU or I/O.
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

    // On a cache hit no compilation happened: no span, only the root attribute.
    if (state.CompileWallStart && state.CompileWallEnd > state.CompileWallStart) {
        EmitPhase(parentId, state.CompileWallStart, state.CompileWallEnd, "Compile",
            {{"ydb.compile.cache_hit", state.CompileStats.FromCache}});
    } else if (state.CompileStats.FromCache) {
        userSpan.Attribute("ydb.compile.cache_hit", true);
    }

    // Global span budget: stages/phases always render; tasks are admitted by a duration cutoff
    // chosen so that at most K tasks (globally, across executions) fit the budget; shard children
    // consume the remainder.
    TSpanBudget budget;
    {
        size_t fixedSpans = 0;
        TVector<ui64> taskDurations;
        for (const auto& trace : state.QueryStats.UserFacingTraces) {
            fixedSpans += 10 + trace.ExecStats.StagesSize(); // phases estimate + stage spans
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
            // +1 so tasks exactly at the cutoff are dropped rather than overshooting the budget.
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

// Root-name candidates ranked so that across a script's statements the strongest one wins:
// scheme work beats writes (CTAS => DDL), a write beats reads (INSERT..SELECT => INSERT).
int RootNameRank(const TString& name) {
    if (!name) {
        return 0;
    }
    if (name == "DDL") {
        return 3;
    }
    return name.StartsWith("SELECT") ? 1 : 2;
}

// Candidate from one compiled statement — "VERB /table/path" when exactly one table is the
// target (OTel db semconv), bare verb otherwise, empty when the statement neither touches
// tables nor returns rows. Sinks carry the SQL-level write mode; legacy table ops only
// distinguish upsert/delete.
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
                        // An unrecognized sink mode contributes neither verb nor table — noting
                        // its table without a verb would mislabel the query as a read.
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
    // No writes: anything that read tables or returns rows is a SELECT to the user.
    if (hasReads || query.ResultBindingsSize() > 0) {
        return multiRead || !readTable
            ? TString("SELECT") : TStringBuilder() << "SELECT " << readTable;
    }
    return {};
}

// db.query.text must not carry user data (it feeds the user-facing channel and literals are
// PII): literals become '?' placeholders per OTel db semconv, comments are dropped. On lexer
// failure returns empty — the raw text is never exposed.
TString SanitizeQueryText(const TString& text) {
    // Credential-bearing queries (CREATE USER ... PASSWORD, secrets) are hidden entirely, same
    // as the query logs do — even their parameterized shape shouldn't reach the trace.
    TString protectedText;
    if (NKikimr::ProtectQueryForLoggingIfSensitive(text, protectedText)) {
        return protectedText;
    }
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
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
    // Moved out so a repeated call becomes a no-op instead of a duplicate root.
    NWilson::TTraceId traceId = std::move(state.UserFacingTraceId);
    if (!traceId) {
        return;
    }
    const TString rootName = state.UserFacingRootName ? state.UserFacingRootName : FallbackRootName(state);
    NWilson::TSpan userSpan = NWilson::TSpan::ConstructTerminated(
        traceId, traceId.Span(traceId.GetVerbosity()),
        state.StartTime, TInstant::Now(),
        NWilson::NTraceProto::Status::STATUS_CODE_OK, rootName);
    if (!userSpan) {
        return;
    }
    userSpan.Attribute("ydb.tracing.layer", TString("user"));
    userSpan.Attribute("db.system.name", TString("ydb"));
    if (AppData()) {
        userSpan.Attribute("db.namespace", AppData()->TenantName);
    }
    // OTel db semconv: the span name is "{operation} {target}", db.operation.name is the bare
    // operation — the verb of the root name, never the raw action enum.
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
        // The span status message is what trace UIs surface as the failure reason.
        userSpan.EndError(errorMessage ? errorMessage.substr(0, 1024) : statusCode);
    }
}

} // namespace NKikimr::NKqp
