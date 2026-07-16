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
        // Straggler detector: slowest task vs average across this stage's tasks.
        const auto& taskDur = stage.GetDurationUs();
        if (stage.GetTotalTasksCount() > 1 && taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
            const double avg = static_cast<double>(taskDur.GetSum()) / taskDur.GetCnt();
            if (avg > 0 && taskDur.GetMax() > avg) {
                span.Attribute("ydb.task_skew", static_cast<double>(taskDur.GetMax()) / avg);
            }
        }
        span.End();
    }
}

} // namespace NKikimr::NKqp
