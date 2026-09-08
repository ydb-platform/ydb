#include "kqp_execution_tracing.h"
#include "kqp_trace_settings.h"

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <algorithm>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

TExecutionTrace::TExecutionTrace(ui8 verbosity)
    : CollectDetails(verbosity >= TComponentTracingLevels::TQueryProcessor::Detailed) {
}

ui64 TExecutionTrace::StartStage(const NWilson::TSpan& parent, std::pair<ui64, ui32> stageId,
        const TTaskTraceDescription& description, ui64 taskCount) {
    if (!CollectDetails || !parent || !taskCount
            || (!Stages.contains(stageId) && Stages.size() == NQueryTraceSettings::MaxStages)) {
        return 0;
    }
    auto& stage = Stages[stageId];
    if (!stage.Span.GetTraceId()) {
        stage.Description = description;
        stage.TaskCount = taskCount;
        stage.Span = parent.CreateChild(TComponentTracingLevels::TQueryProcessor::Detailed,
            description.StageName(), NWilson::EFlags::AUTO_END);
        stage.Span.Attribute("ydb.tx_index", static_cast<i64>(stageId.first));
        stage.Span.Attribute("ydb.stage_id", static_cast<i64>(stageId.second));
        stage.Span.Attribute("ydb.timing_boundary", TString("tasks_launch_to_last_report"));
    }
    ui64 spanId = 0;
    if (stage.Span) {
        memcpy(&spanId, stage.Span.GetTraceId().GetSpanIdPtr(), sizeof(spanId));
    }
    return spanId;
}

void TExecutionTrace::OnTaskFinished(std::pair<ui64, ui32> stageId, const TTaskTraceDescription& description,
        ui64 taskCount, const NYql::NDqProto::TEvComputeActorState& state, ui32 nodeId) {
    NYql::NDqProto::TDqTaskStats empty;
    empty.SetTaskId(state.GetTaskId());
    empty.SetStageId(stageId.second);
    const auto& stats = state.GetStats();
    const auto& task = stats.TasksSize() ? stats.GetTasks(0) : empty;
    std::optional<ui64> durationUs;
    if (stats.GetDurationUs()) {
        durationUs = stats.GetDurationUs();
    } else if (task.GetStartTimeMs() && task.GetFinishTimeMs() >= task.GetStartTimeMs()) {
        durationUs = (task.GetFinishTimeMs() - task.GetStartTimeMs()) * 1000;
    }
    AddTask(stageId.first, description, taskCount, task,
        durationUs, nodeId, state.GetState() == NYql::NDqProto::COMPUTE_STATE_FAILURE
            ? NYql::NDq::DqStatusToYdbStatus(state.GetStatusCode()) : Ydb::StatusIds::SUCCESS);
}

void TExecutionTrace::AddTask(ui64 txIndex, const TTaskTraceDescription& description, ui64 taskCount,
        const NYql::NDqProto::TDqTaskStats& task, std::optional<ui64> durationUs, ui32 nodeId, Ydb::StatusIds::StatusCode status) {
    const bool failed = status != Ydb::StatusIds::SUCCESS;
    const ui64 waitUs = task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs();
    const ui64 spilledBytes = task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();
    WaitUs += waitUs;
    SpilledBytes += spilledBytes;
    auto it = Stages.find({txIndex, task.GetStageId()});
    if (it == Stages.end()) {
        if (Stages.size() == NQueryTraceSettings::MaxStages) {
            ++UnrepresentedStageTasks;
            return;
        }
        it = Stages.try_emplace(std::make_pair(txIndex, task.GetStageId())).first;
        if (CollectDetails) {
            it->second.Description = description;
        }
    }
    auto& summary = it->second;
    summary.TaskCount = taskCount;
    ++summary.Reports;
    if (durationUs) {
        if (!summary.Durations || *durationUs < summary.MinDurationUs) {
            summary.MinDurationUs = *durationUs;
            summary.FastestNode = nodeId;
        }
        if (!summary.Durations || *durationUs > summary.MaxDurationUs) {
            summary.MaxDurationUs = *durationUs;
            summary.SlowestNode = nodeId;
        }
        ++summary.Durations;
        summary.SumDurationUs += *durationUs;
    }
    if (!CollectDetails) {
        return;
    }
    TTask sample;
    sample.Id = task.GetTaskId();
    sample.Node = nodeId;
    sample.DurationUs = durationUs.value_or(0);
    sample.CpuUs = task.GetCpuTimeUs();
    sample.InputRows = task.GetInputRows();
    sample.OutputRows = task.GetOutputRows();
    sample.WaitUs = waitUs;
    sample.SpilledBytes = spilledBytes;
    sample.Failed = failed;
    NKqpProto::TKqpTaskExtraStats extra;
    if (task.GetExtra().UnpackTo(&extra)) {
        sample.Retries = extra.GetReadRetriesCount() + extra.GetScanTaskExtraStats().GetRetriesCount();
    }
    summary.FailedTasks += failed;
    if (failed && summary.Status == Ydb::StatusIds::SUCCESS) {
        summary.Status = status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
            ? Ydb::StatusIds::GENERIC_ERROR : status;
    }
    summary.CpuUs += sample.CpuUs;
    summary.InputRows += sample.InputRows;
    summary.OutputRows += sample.OutputRows;
    summary.WaitUs += waitUs;
    summary.SpilledBytes += spilledBytes;
    if (summary.TasksByNode.contains(nodeId)
            || summary.TasksByNode.size() < NQueryTraceSettings::MaxNodesPerStage) {
        ++summary.TasksByNode[nodeId];
    } else {
        ++summary.UnrepresentedNodeTasks;
    }
    summary.Tasks.push_back(sample);
    std::sort(summary.Tasks.begin(), summary.Tasks.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.Rank() > rhs.Rank();
    });
    if (summary.Tasks.size() > NQueryTraceSettings::MaxTasksPerStage) {
        summary.Tasks.pop_back();
    }
    if (summary.Reports == summary.TaskCount) {
        FinishStage(summary, Ydb::StatusIds::SUCCESS);
    }
}

void TExecutionTrace::FinishStage(TStage& stage, Ydb::StatusIds::StatusCode status) {
    if (!stage.Span) {
        return;
    }
    const double skew = stage.SumDurationUs
        ? static_cast<double>(stage.MaxDurationUs) * stage.Durations / stage.SumDurationUs : 0;
    NWilson::TArrayValue tasks;
    for (const auto& task : stage.Tasks) {
        tasks.emplace_back(NWilson::TKeyValueList{{
            {"ydb.task_id", static_cast<i64>(task.Id)},
            {"ydb.node_id", static_cast<i64>(task.Node)},
            {"ydb.duration_us", static_cast<i64>(task.DurationUs)},
            {"ydb.cpu_us", static_cast<i64>(task.CpuUs)},
            {"ydb.input_rows", static_cast<i64>(task.InputRows)},
            {"ydb.output_rows", static_cast<i64>(task.OutputRows)},
            {"ydb.wait_us", static_cast<i64>(task.WaitUs)},
            {"ydb.spilled_bytes", static_cast<i64>(task.SpilledBytes)},
            {"ydb.read_retries", static_cast<i64>(task.Retries)},
            {"ydb.failed", task.Failed},
        }});
    }
    TStringBuilder nodes;
    for (const auto& [node, count] : stage.TasksByNode) {
        if (nodes) {
            nodes << ",";
        }
        nodes << node << ":" << count;
    }
    const std::initializer_list<std::pair<TString, NWilson::TAttributeValue>> attributes = {
        {"ydb.stage.operations", stage.Description.OperationsAttribute()},
        {"ydb.tasks", static_cast<i64>(stage.TaskCount)},
        {"ydb.reported_tasks", static_cast<i64>(stage.Reports)},
        {"ydb.failed_tasks", static_cast<i64>(stage.FailedTasks)},
        {"ydb.cpu_us", static_cast<i64>(stage.CpuUs)},
        {"ydb.input_rows", static_cast<i64>(stage.InputRows)},
        {"ydb.output_rows", static_cast<i64>(stage.OutputRows)},
        {"ydb.wait_us", static_cast<i64>(stage.WaitUs)},
        {"ydb.spilled_bytes", static_cast<i64>(stage.SpilledBytes)},
        {"ydb.task_duration_min_us", static_cast<i64>(stage.MinDurationUs)},
        {"ydb.task_duration_avg_us", static_cast<i64>(stage.Durations ? stage.SumDurationUs / stage.Durations : 0)},
        {"ydb.task_duration_max_us", static_cast<i64>(stage.MaxDurationUs)},
        {"ydb.timed_tasks", static_cast<i64>(stage.Durations)},
        {"ydb.task_skew", skew},
        {"ydb.tasks_by_node", TString(nodes)},
        {"ydb.tasks_without_node_details", static_cast<i64>(stage.UnrepresentedNodeTasks)},
        {"ydb.fastest_task_node", static_cast<i64>(stage.FastestNode)},
        {"ydb.slowest_task_node", static_cast<i64>(stage.SlowestNode)},
        {"ydb.interesting_tasks", std::move(tasks)},
        {"ydb.tasks_truncated", static_cast<i64>(stage.Reports - stage.Tasks.size())},
    };
    for (const auto& [key, value] : attributes) {
        stage.Span.Attribute(key, value);
    }
    stage.Span.Attribute("ydb.task_stats_incomplete",
        stage.Reports != stage.TaskCount || stage.Durations != stage.Reports || stage.FailedTasks);
    EndQueryTraceSpan(stage.Span, stage.Status != Ydb::StatusIds::SUCCESS ? stage.Status : status);
}

void TExecutionTrace::Finish(NWilson::TSpan& span, NYql::NDqProto::TDqExecutionStats& stats,
        Ydb::StatusIds::StatusCode status) {
    double maxSkew = 0;
    bool incomplete = status != Ydb::StatusIds::SUCCESS || UnrepresentedStageTasks;
    for (auto& [id, stage] : Stages) {
        const double skew = stage.SumDurationUs
            ? static_cast<double>(stage.MaxDurationUs) * stage.Durations / stage.SumDurationUs : 0;
        maxSkew = std::max(maxSkew, skew);
        incomplete |= stage.Reports != stage.TaskCount || stage.Durations != stage.Reports || stage.FailedTasks;
        FinishStage(stage, status == Ydb::StatusIds::SUCCESS && stage.Reports != stage.TaskCount
            ? Ydb::StatusIds::STATUS_CODE_UNSPECIFIED : status);
    }
    if (!span) {
        return;
    }
    span.Attribute("ydb.wait_us", static_cast<i64>(WaitUs));
    span.Attribute("ydb.spilled_bytes", static_cast<i64>(SpilledBytes));
    span.Attribute("ydb.max_task_skew", maxSkew);
    span.Attribute("ydb.task_stats_incomplete", incomplete);
    span.Attribute("ydb.tasks_without_stage_details", static_cast<i64>(UnrepresentedStageTasks));
    NKqpProto::TKqpExecutionExtraStats extra;
    stats.GetExtra().UnpackTo(&extra);
    extra.SetWaitTimeUs(WaitUs);
    extra.SetSpilledBytes(SpilledBytes);
    extra.SetMaxTaskSkew(maxSkew);
    extra.SetTaskStatsIncomplete(incomplete);
    stats.MutableExtra()->PackFrom(extra);
}

}
