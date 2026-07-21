#include "kqp_executer_user_trace.h"

#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

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

// Emits one span per task as a child of its stage span. Task start/finish are ABSOLUTE epoch ms
// (raw from the compute actor via ComputeActors), unlike the stage aggregate which is offset from
// BaseTimeMs — so no base is added here. NodeId comes straight off the task. Today ComputeActors
// carries only the stage's longest task (see kqp_executer_stats.cpp), so this is the slowest-task
// drill-down until per-task retention is widened.
void EmitTaskSpans(const NWilson::TTraceId& stageParent, const NYql::NDqProto::TDqStageStats& stage, ui64 stageStartMs) {
    for (const auto& ca : stage.GetComputeActors()) {
        for (const auto& task : ca.GetTasks()) {
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
            // Wall time is only millisecond-grained in the stats, so it is exposed explicitly:
            // the span's own duration rounds to zero for sub-millisecond tasks.
            span.Attribute("ydb.duration_us", static_cast<i64>((finishMs - startMs) * 1000));
            // How long the task sat between being created and actually starting — scheduling delay,
            // which for skewed OLAP stages often explains more than the run time itself.
            if (task.GetCreateTimeMs() && task.GetStartTimeMs() > task.GetCreateTimeMs()) {
                span.Attribute("ydb.queue_delay_us",
                    static_cast<i64>((task.GetStartTimeMs() - task.GetCreateTimeMs()) * 1000));
            }
            // Splits preparation (building the computation graph) from actual computation.
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

} // namespace

void EmitUserStagePhases(const NWilson::TTraceId& parent, const NYql::NDqProto::TDqExecutionStats& stats) {
    if (!parent) {
        return;
    }
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

        const ui64 waitUs = stage.GetWaitInputTimeUs().GetSum() + stage.GetWaitOutputTimeUs().GetSum();
        if (waitUs > 0) {
            span.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
        }
        const ui64 spilledBytes = stage.GetSpillingComputeBytes().GetSum() + stage.GetSpillingChannelBytes().GetSum();
        if (spilledBytes > 0) {
            span.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
        }
        // Per-stage parallelism summary. The per-task spans below carry the detail, but a trace UI
        // can only aggregate span durations (not attributes), and those are millisecond-grained —
        // so the task duration min/avg/max is stated explicitly here for a quick read of the spread.
        const auto& taskDur = stage.GetDurationUs();
        if (taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
            const double avg = static_cast<double>(taskDur.GetSum()) / taskDur.GetCnt();
            span.Attribute("ydb.task_duration_min_us", static_cast<i64>(taskDur.GetMin()));
            span.Attribute("ydb.task_duration_avg_us", static_cast<i64>(avg));
            span.Attribute("ydb.task_duration_max_us", static_cast<i64>(taskDur.GetMax()));
            // Straggler detector: slowest task vs average across this stage's tasks.
            if (stage.GetTotalTasksCount() > 1 && avg > 0 && taskDur.GetMax() > avg) {
                span.Attribute("ydb.task_skew", static_cast<double>(taskDur.GetMax()) / avg);
            }
        }
        // How staggered the tasks were: the window between the first and last task starting, and
        // likewise finishing. A wide finish spread next to a narrow start spread means stragglers.
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
        EmitTaskSpans(span.GetTraceId(), stage, base + startMs);
        span.End();
    }
}

} // namespace NKikimr::NKqp
