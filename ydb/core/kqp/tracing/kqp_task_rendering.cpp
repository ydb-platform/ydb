#include "kqp_task_rendering.h"

#include "kqp_trace_settings.h"

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/ast/yql_ast.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {
namespace {

constexpr TStringBuf TASK_OPERATIONS_PARAM = "ydb.trace.task_operations";
constexpr char STAGE_TRACE_ID_PARAM[] = "ydb.trace.stage_trace_id";
enum class EOperation : ui32 {
    None = 0,
    Read = 1 << 0,
    Lookup = 1 << 1,
    Join = 1 << 2,
    Filter = 1 << 3,
    Aggregate = 1 << 4,
    Sort = 1 << 5,
    Write = 1 << 6,
};
constexpr std::array OPERATION_NAMES = {
    std::pair{EOperation::Read, "Read"}, std::pair{EOperation::Lookup, "Lookup"}, std::pair{EOperation::Join, "Join"},
    std::pair{EOperation::Filter, "Filter"}, std::pair{EOperation::Aggregate, "Aggregate"},
    std::pair{EOperation::Sort, "Sort"}, std::pair{EOperation::Write, "Write"},
};

EOperation CallableOperation(TStringBuf name) {
    if (name.Contains("Join")) {
        return EOperation::Join;
    }
    if (name.Contains("Combine") || name.Contains("Aggregate") || name == "Condense1"
            || name == "BlockMergeFinalizeHashed" || name == "BlockMergeManyFinalizeHashed") {
        return EOperation::Aggregate;
    }
    if (name.Contains("Filter")) {
        return EOperation::Filter;
    }
    if (name == "Sort" || name == "Top" || name == "TopSort"
            || name.StartsWith("WideSort") || name.StartsWith("WideTop")
            || name.StartsWith("BlockSort") || name.StartsWith("BlockTop")) {
        return EOperation::Sort;
    }
    return EOperation::None;
}

} // namespace

TTaskTraceDescription TTaskTraceDescription::FromStage(const NKqpProto::TKqpPhyStage& stage) {
    TTaskTraceDescription result;
    if (!stage.GetSources().empty()) {
        result.Operations_ |= static_cast<ui32>(EOperation::Read);
    }
    for (const auto& input : stage.GetInputs()) {
        if (input.HasStreamLookup()) {
            result.Operations_ |= static_cast<ui32>(EOperation::Lookup);
        }
    }
    if (!stage.GetSinks().empty() || stage.GetIsEffectsStage()) {
        result.Operations_ |= static_cast<ui32>(EOperation::Write);
    }
    for (const auto& op : stage.GetTableOps()) {
        switch (op.GetTypeCase()) {
            case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
            case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                result.Operations_ |= static_cast<ui32>(EOperation::Write);
                break;
            case NKqpProto::TKqpPhyTableOperation::kReadRange:
            case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
            case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                result.Operations_ |= static_cast<ui32>(EOperation::Read);
                break;
            default:
                break;
        }
    }
    const auto ast = NYql::ParseAst(stage.GetProgramAst());
    if (ast.IsOk()) {
        TVector<const NYql::TAstNode*> pending{ast.Root};
        while (!pending.empty()) {
            const auto* node = pending.back();
            pending.pop_back();
            if (!node->IsList() || !node->GetChildrenCount()) {
                continue;
            }
            const auto* head = node->GetChild(0);
            if (head->IsAtom()) {
                if (head->GetContent() == "quote") {
                    if (node->GetChildrenCount() == 2 && node->GetChild(1)->IsList()) {
                        for (const auto* item : node->GetChild(1)->GetChildren()) {
                            pending.push_back(item);
                        }
                    }
                    continue;
                }
                if (head->GetFlags() == NYql::TNodeFlags::Default) {
                    result.Operations_ |= static_cast<ui32>(CallableOperation(head->GetContent()));
                }
            }
            for (const auto* child : node->GetChildren()) {
                pending.push_back(child);
            }
        }
    }
    return result;
}

NWilson::TArrayValue TTaskTraceDescription::OperationsAttribute() const {
    NWilson::TArrayValue result;
    for (const auto& [bit, name] : OPERATION_NAMES) {
        if (Operations_ & static_cast<ui32>(bit)) {
            result.emplace_back(TString(name));
        }
    }
    if (result.empty()) {
        result.emplace_back(TString("Compute"));
    }
    return result;
}

TString TTaskTraceDescription::Name() const {
    return Name("Task: ");
}

TString TTaskTraceDescription::StageName() const {
    return Name("Stage: ");
}

TString TTaskTraceDescription::Name(TStringBuf prefix) const {
    TStringBuilder name;
    name << prefix;
    size_t count = 0;
    bool truncated = false;
    for (const auto& [operation, operationName] : OPERATION_NAMES) {
        if (!(Operations_ & static_cast<ui32>(operation))) {
            continue;
        }
        if (count == NQueryTraceSettings::MAX_TASK_NAME_OPERATIONS) {
            truncated = true;
            break;
        }
        if (count++) {
            name << " + ";
        }
        name << operationName;
    }
    if (!count) {
        name << "Compute";
    } else if (truncated) {
        name << " + ...";
    }
    return name;
}

void TTaskTraceDescription::Save(NYql::NDqProto::TDqTask& task) const {
    if (!Operations_) {
        if (!task.GetTaskParams().empty()) {
            task.MutableTaskParams()->erase(TString(TASK_OPERATIONS_PARAM));
        }
        return;
    }
    (*task.MutableTaskParams())[TString(TASK_OPERATIONS_PARAM)] = ToString(Operations_);
}

void TTaskTraceDescription::Annotate(NWilson::TSpan& span, const NYql::NDqProto::TDqTask& task) {
    if (!span) {
        return;
    }
    TTaskTraceDescription description;
    const auto it = task.GetTaskParams().find(TString(TASK_OPERATIONS_PARAM));
    if (it != task.GetTaskParams().end()) {
        TryFromString(it->second, description.Operations_);
    }
    span.Name(description.Name());
    span.Attribute("ydb.stage_id", static_cast<i64>(task.GetStageId()));
    span.Attribute("ydb.task_id", static_cast<i64>(task.GetId()));
    span.Attribute("ydb.task.operations", description.OperationsAttribute());
}

void SaveTaskTraceParent(NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& stageTraceId) {
    // ExecutionTrace writes this when it creates a stage span; the compute actor consumes it.
    // Keep it absent when tracing is disabled so reused task graphs cannot retain stale parents.
    if (stageTraceId) {
        NActorsProto::TTraceId serializedTraceId;
        stageTraceId.Serialize(&serializedTraceId);
        (*task.MutableTaskParams())[STAGE_TRACE_ID_PARAM] = serializedTraceId.SerializeAsString();
    } else if (!task.GetTaskParams().empty()) {
        task.MutableTaskParams()->erase(STAGE_TRACE_ID_PARAM);
    }
}

NWilson::TTraceId GetTaskTraceParent(const NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& parent) {
    const auto it = task.GetTaskParams().find(STAGE_TRACE_ID_PARAM);
    if (!parent || !parent.GetTimeToLive()
            || parent.GetVerbosity() < TComponentTracingLevels::TQueryProcessor::Detailed
            || it == task.GetTaskParams().end()) {
        return NWilson::TTraceId(parent);
    }
    NActorsProto::TTraceId serializedTraceId;
    if (!serializedTraceId.ParseFromString(it->second)) {
        return NWilson::TTraceId(parent);
    }
    NWilson::TTraceId stageTraceId(serializedTraceId);
    if (!stageTraceId || !stageTraceId.IsSameTrace(parent)) {
        return NWilson::TTraceId(parent);
    }
    return stageTraceId;
}

void AddReadTraceStats(NWilson::TSpan& span, NYql::NDqProto::TDqTaskStats& stats, ui64 retries) {
    if (span.GetTraceId() && retries) {
        NKqpProto::TKqpTaskExtraStats extra;
        stats.GetExtra().UnpackTo(&extra);
        extra.SetReadRetriesCount(extra.GetReadRetriesCount() + retries);
        stats.MutableExtra()->PackFrom(extra);
    }
}

void AddReadTraceAttributes(NWilson::TSpan& span, const TString& table, ui64 rows, ui64 retries) {
    if (span) {
        span.Attribute("db.collection.name", table);
        span.Attribute("ydb.rows", static_cast<i64>(rows));
        span.Attribute("ydb.read_retries", static_cast<i64>(retries));
    }
}

void AddKqpTaskTraceAttributes(NWilson::TSpan& span, const NYql::NDqProto::TDqComputeActorStats& stats,
        bool spilledBytesAvailable) {
    if (!span) {
        return;
    }
    if (stats.TasksSize() == 1) {
        const auto& task = stats.GetTasks(0);
        span.Attribute("ydb.cpu_us", static_cast<i64>(task.GetCpuTimeUs()));
        span.Attribute("ydb.input_rows", static_cast<i64>(task.GetInputRows()));
        span.Attribute("ydb.output_rows", static_cast<i64>(task.GetOutputRows()));
        span.Attribute("ydb.wait_us", static_cast<i64>(task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs()));
        span.Attribute("ydb.compute_cpu_us", static_cast<i64>(task.GetComputeCpuTimeUs()));
        span.Attribute("ydb.build_cpu_us", static_cast<i64>(task.GetBuildCpuTimeUs()));
        span.Attribute("ydb.node_id", static_cast<i64>(task.GetNodeId()));
        if (stats.GetDurationUs()) {
            span.Attribute("ydb.duration_us", static_cast<i64>(stats.GetDurationUs()));
            span.Attribute("ydb.duration_measured", true);
        } else if (task.GetStartTimeMs() && task.GetFinishTimeMs() >= task.GetStartTimeMs()) {
            span.Attribute("ydb.duration_us", static_cast<i64>(
                (task.GetFinishTimeMs() - task.GetStartTimeMs()) * 1000));
            span.Attribute("ydb.duration_measured", true);
        }
        span.Attribute("ydb.spilled_bytes_available", spilledBytesAvailable);
        span.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytesAvailable
            ? task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes()
            : 0));
        if (task.GetCreateTimeMs() && task.GetStartTimeMs() >= task.GetCreateTimeMs()) {
            span.Attribute("ydb.queue_delay_us", static_cast<i64>(
                (task.GetStartTimeMs() - task.GetCreateTimeMs()) * 1000));
        }
        NKqpProto::TKqpTaskExtraStats extra;
        if (task.GetExtra().UnpackTo(&extra)) {
            span.Attribute("ydb.read_retries", static_cast<i64>(extra.GetReadRetriesCount()));
        }
    } else if (stats.TasksSize() > 1) {
        span.Attribute("ydb.cpu_us_shared", true);
    }
}

} // namespace NKikimr::NKqp
