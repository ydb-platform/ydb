#include "kqp_execution_tracing.h"
#include "kqp_trace_settings.h"

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <algorithm>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

TString StageOperation(const NKqpProto::TKqpPhyStage& stage) {
    for (const auto& input : stage.GetInputs()) {
        if (input.HasStreamLookup()) {
            return "Lookup";
        }
    }
    if (!stage.GetSinks().empty() || stage.GetIsEffectsStage()) {
        return "Write";
    }
    const auto& ast = stage.GetProgramAst();
    if (ast.Contains("Join")) {
        return "Join";
    }
    if (ast.Contains("Combine") || ast.Contains("Aggregate")) {
        return "Aggregate";
    }
    if (ast.Contains("Filter")) {
        return "Filter";
    }
    if (!stage.GetSources().empty() || !stage.GetTableOps().empty()) {
        return "Read";
    }
    return "Compute";
}

}

void TExecutionTraceStats::OnTaskFinished(std::pair<ui64, ui32> stageId, const NKqpProto::TKqpPhyStage& stage,
        ui64 taskCount, const NYql::NDqProto::TEvComputeActorState& state, ui32 nodeId) {
    NYql::NDqProto::TDqTaskStats empty;
    empty.SetTaskId(state.GetTaskId());
    empty.SetStageId(stageId.second);
    const auto& stats = state.GetStats();
    AddTask(stageId.first, stage, taskCount, stats.TasksSize() ? stats.GetTasks(0) : empty,
        stats.GetDurationUs(), nodeId, state.GetState() == NYql::NDqProto::COMPUTE_STATE_FAILURE);
}

void TExecutionTraceStats::AddTask(ui64 txIndex, const NKqpProto::TKqpPhyStage& stage, ui64 taskCount,
        const NYql::NDqProto::TDqTaskStats& task, ui64 durationUs, ui32 nodeId, bool failed) {
    TTask sample;
    sample.Id = task.GetTaskId();
    sample.Node = nodeId;
    sample.DurationUs = durationUs;
    sample.CpuUs = task.GetCpuTimeUs();
    sample.InputRows = task.GetInputRows();
    sample.OutputRows = task.GetOutputRows();
    sample.WaitUs = task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs();
    sample.SpilledBytes = task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();
    sample.Failed = failed;
    NKqpProto::TKqpTaskExtraStats extra;
    if (task.GetExtra().UnpackTo(&extra)) {
        sample.Retries = extra.GetReadRetriesCount() + extra.GetScanTaskExtraStats().GetRetriesCount();
    }
    WaitUs += sample.WaitUs;
    SpilledBytes += sample.SpilledBytes;
    auto it = Stages.find({txIndex, task.GetStageId()});
    if (it == Stages.end()) {
        if (Stages.size() == NQueryTraceSettings::MaxStages) {
            ++UnrepresentedStageTasks;
            return;
        }
        it = Stages.try_emplace(std::make_pair(txIndex, task.GetStageId())).first;
        it->second.Operation = StageOperation(stage);
    }
    auto& summary = it->second;
    summary.TaskCount = taskCount;
    ++summary.Reports;
    summary.FailedTasks += failed;
    summary.CpuUs += sample.CpuUs;
    summary.InputRows += sample.InputRows;
    summary.OutputRows += sample.OutputRows;
    summary.WaitUs += sample.WaitUs;
    summary.SpilledBytes += sample.SpilledBytes;
    if (durationUs) {
        if (!summary.Durations || durationUs < summary.MinDurationUs) {
            summary.MinDurationUs = durationUs;
            summary.FastestNode = nodeId;
        }
        if (!summary.Durations || durationUs > summary.MaxDurationUs) {
            summary.MaxDurationUs = durationUs;
            summary.SlowestNode = nodeId;
        }
        ++summary.Durations;
        summary.SumDurationUs += durationUs;
    }
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
}

void TExecutionTraceStats::Finish(NWilson::TSpan& span, NYql::NDqProto::TDqExecutionStats& stats,
        Ydb::StatusIds::StatusCode status) const {
    if (!span) {
        return;
    }
    double maxSkew = 0;
    bool incomplete = status != Ydb::StatusIds::SUCCESS || UnrepresentedStageTasks;
    for (const auto& [id, stage] : Stages) {
        const double skew = stage.SumDurationUs
            ? static_cast<double>(stage.MaxDurationUs) * stage.Durations / stage.SumDurationUs : 0;
        maxSkew = std::max(maxSkew, skew);
        incomplete |= stage.Reports != stage.TaskCount || stage.Durations != stage.Reports;
        if (span.GetTraceId().GetVerbosity() < TComponentTracingLevels::TQueryProcessor::Detailed) {
            continue;
        }
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
        span.Event("Stage statistics", {
            {"ydb.tx_index", static_cast<i64>(id.first)},
            {"ydb.stage_id", static_cast<i64>(id.second)},
            {"ydb.stage.operation", stage.Operation},
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
        });
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
