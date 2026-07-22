#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/core/kqp/common/kqp_user_trace_data.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

// The single renderer of the user-facing trace. Everything the user sees is built here, at reply
// time, from data the engine merely recorded: session timings (queue/compile), the executer's
// phase timeline and per-task retention (TUserTraceExecutionData), and the finalized stats.
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
        parentId, parentId.Span(TWilsonKqp::KqpSession), start, end,
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

TString StageDisplayName(const NYql::NDqProto::TDqStageStats& stage) {
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
    return TStringBuilder() << "Step " << stage.GetStageId();
}

// Task start/finish are ABSOLUTE epoch ms (raw from the compute actor), unlike the stage
// aggregate which is offset from BaseTimeMs — so no base is added here.
void EmitTaskSpans(const NWilson::TTraceId& stageParent,
        const std::unordered_map<ui64, NYql::NDqProto::TDqComputeActorStats>& tasks, ui64 stageStartMs) {
    for (const auto& [retainedTaskId, ca] : tasks) {
        for (const auto& task : ca.GetTasks()) {
            // A CA snapshot can carry sibling tasks; emit each task only under its own key.
            if (task.GetTaskId() != retainedTaskId) {
                continue;
            }
            // Write tasks (datashard) report FinishTimeMs but leave StartTimeMs unset; fall back to
            // creation time, then to the stage's start, so the task still gets a span in-window.
            const ui64 startMs = task.GetStartTimeMs() ? task.GetStartTimeMs()
                : task.GetCreateTimeMs() ? task.GetCreateTimeMs()
                : stageStartMs;
            const ui64 finishMs = task.GetFinishTimeMs();
            if (startMs == 0 || finishMs < startMs) {
                continue;
            }
            NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
                stageParent, stageParent.Span(TWilsonKqp::KqpSession),
                TInstant::MilliSeconds(startMs), TInstant::MilliSeconds(finishMs),
                NWilson::NTraceProto::Status::STATUS_CODE_OK,
                TStringBuilder() << "Task " << task.GetTaskId());
            if (!span) {
                continue;
            }
            span.Attribute("ydb.task_id", static_cast<i64>(task.GetTaskId()));
            if (task.GetNodeId()) {
                span.Attribute("ydb.node_id", static_cast<i64>(task.GetNodeId()));
            }
            span.Attribute("ydb.cpu_us", static_cast<i64>(task.GetCpuTimeUs()));
            span.Attribute("ydb.input_rows", static_cast<i64>(task.GetInputRows()));
            span.Attribute("ydb.output_rows", static_cast<i64>(task.GetOutputRows()));
            // Stated explicitly because the span's own duration rounds to zero for sub-ms tasks.
            span.Attribute("ydb.duration_us", static_cast<i64>((finishMs - startMs) * 1000));
            if (task.GetCreateTimeMs() && task.GetStartTimeMs() > task.GetCreateTimeMs()) {
                span.Attribute("ydb.queue_delay_us",
                    static_cast<i64>((task.GetStartTimeMs() - task.GetCreateTimeMs()) * 1000));
            }
            if (task.GetComputeCpuTimeUs() > 0) {
                span.Attribute("ydb.compute_cpu_us", static_cast<i64>(task.GetComputeCpuTimeUs()));
            }
            if (task.GetBuildCpuTimeUs() > 0) {
                span.Attribute("ydb.build_cpu_us", static_cast<i64>(task.GetBuildCpuTimeUs()));
            }
            const ui64 waitUs = task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs();
            if (waitUs > 0) {
                span.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
            }
            const ui64 spilledBytes = task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();
            if (spilledBytes > 0) {
                span.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
            }
            span.End();
        }
    }
}

void EmitStageSpans(const NWilson::TTraceId& parent, const NYql::NDqProto::TDqExecutionStats& stats,
        const TUserTraceTaskStats& taskStats) {
    for (const auto& stage : stats.GetStages()) {
        // Stage start/finish are offsets from BaseTimeMs (absolute epoch ms); base 0 => untimed stage.
        const ui64 base = stage.GetBaseTimeMs();
        const ui64 startMs = stage.GetStartTimeMs().GetMin();
        const ui64 finishMs = stage.GetFinishTimeMs().GetMax();
        if (base == 0 || finishMs < startMs) {
            continue;
        }
        NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
            parent, parent.Span(TWilsonKqp::KqpSession),
            TInstant::MilliSeconds(base + startMs), TInstant::MilliSeconds(base + finishMs),
            NWilson::NTraceProto::Status::STATUS_CODE_OK, StageDisplayName(stage));
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
        // A wide finish spread next to a narrow start spread means stragglers.
        if (stage.GetTotalTasksCount() > 1) {
            const auto& st = stage.GetStartTimeMs();
            const auto& fin = stage.GetFinishTimeMs();
            if (st.GetCnt() > 0 && st.GetMax() > st.GetMin()) {
                span.Attribute("ydb.task_start_spread_us", static_cast<i64>((st.GetMax() - st.GetMin()) * 1000));
            }
            if (fin.GetCnt() > 0 && fin.GetMax() > fin.GetMin()) {
                span.Attribute("ydb.task_finish_spread_us", static_cast<i64>((fin.GetMax() - fin.GetMin()) * 1000));
            }
        }
        if (const auto stageTasksIt = taskStats.find(stage.GetStageId()); stageTasksIt != taskStats.end()) {
            const auto& stageTasks = stageTasksIt->second;
            // Set only when the cap was actually hit — a task that never reported stats is
            // simply absent, not truncated.
            if (stageTasks.size() >= MaxUserTraceTasksPerStage
                    && stage.GetTotalTasksCount() > stageTasks.size()) {
                span.Attribute("ydb.tasks_truncated",
                    static_cast<i64>(stage.GetTotalTasksCount() - stageTasks.size()));
            }
            EmitTaskSpans(span.GetTraceId(), stageTasks, base + startMs);
        }
        span.End();
    }
}

void RenderExecution(const NWilson::TTraceId& rootId, const NYql::NDqProto::TDqExecutionStats& stats,
        const TUserTraceExecutionData& trace) {
    const TUserTraceTimeline& tl = trace.Timeline;
    NWilson::TSpan executeSpan = MakePhase(rootId, tl.Execute.Start, tl.Execute.End, "Execute");
    if (!executeSpan) {
        return;
    }
    const NWilson::TTraceId executeId = executeSpan.GetTraceId();

    struct TPhaseName {
        EUserTracePhase Phase;
        const char* Name;
    };
    static constexpr TPhaseName preparePhases[] = {
        {EUserTracePhase::ResolveTables, "ResolveTables"},
        {EUserTracePhase::ResolveShards, "ResolveShards"},
        {EUserTracePhase::Snapshot, "Snapshot"},
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
            if (const auto& window = tl.Phase(phase)) {
                EmitPhase(prepareSpan.GetTraceId(), window.Start, window.End, name);
            }
        }
        prepareSpan.End();
    }

    NWilson::TSpan runSpan;
    if (const auto& run = tl.Phase(EUserTracePhase::RunTasks)) {
        runSpan = MakePhase(executeId, run.Start, run.End, "Run");
    }
    const NWilson::TTraceId runId = runSpan ? runSpan.GetTraceId() : NWilson::TTraceId{};
    EmitStageSpans(runSpan ? runId : executeId, stats, trace.TaskStats);
    if (runSpan) {
        runSpan.End();
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
    for (const auto& e : state.QueryStats.Executions) {
        cpuUs += e.GetCpuTimeUs();
        for (const auto& table : e.GetTables()) {
            rowsRead += table.GetReadRows();
            rowsWritten += table.GetWriteRows();
            bytesRead += table.GetReadBytes();
        }
        for (const auto& stage : e.GetStages()) {
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
        EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued", {{"ydb.pool_id", poolId}});
    }

    // On a cache hit no compilation happened: no span, only the root attribute.
    if (state.CompileWallStart && state.CompileWallEnd > state.CompileWallStart) {
        EmitPhase(parentId, state.CompileWallStart, state.CompileWallEnd, "Compile",
            {{"ydb.compile.cache_hit", state.CompileStats.FromCache}});
    } else if (state.CompileStats.FromCache) {
        userSpan.Attribute("ydb.compile.cache_hit", true);
    }

    // UserTraces[i] belongs to Executions[i]: the two stay index-aligned because they are only
    // ever appended as a pair (ProcessExecuterResult).
    const auto& userTraces = state.QueryStats.UserTraces;
    for (size_t i = 0; i < state.QueryStats.Executions.size() && i < userTraces.size(); ++i) {
        RenderExecution(parentId, state.QueryStats.Executions[i], userTraces[i]);
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
                        writeVerb = writeVerb ? writeVerb : SinkVerb(settings.GetType());
                        note(writeTable, multiWrite, settings.GetTable().GetPath());
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
        traceId, traceId.Span(TWilsonKqp::KqpSession),
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
    userSpan.Attribute("db.operation.name", NKikimrKqp::EQueryAction_Name(state.GetAction()));
    userSpan.Attribute("db.query.text", state.RequestEv->GetQuery()); // TODO: parameterize (raw text is PII)
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
