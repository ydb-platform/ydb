#include "kqp_task_tracing.h"

#include "kqp_trace_settings.h"

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/ast/yql_ast.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <array>
#include <cstring>

namespace NKikimr::NKqp {
namespace {

constexpr TStringBuf TASK_OPERATIONS_PARAM = "ydb.trace.task_operations";
constexpr char STAGE_SPAN_ID_PARAM[] = "ydb.trace.stage_span_id";
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
    if (name.Contains("Combine") || name.Contains("Aggregate")) {
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
    for (const auto& op : OperationsAttribute()) {
        if (count == NQueryTraceSettings::MAX_TASK_NAME_OPERATIONS) {
            break;
        }
        if (count++) {
            name << " + ";
        }
        name << std::get<TString>(op);
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
    span.Attribute("ydb.task.operations", description.OperationsAttribute());
}

void SaveTaskTraceParent(NYql::NDqProto::TDqTask& task, ui64 stageSpanId) {
    if (stageSpanId) {
        (*task.MutableTaskParams())[STAGE_SPAN_ID_PARAM] = ToString(stageSpanId);
    } else if (!task.GetTaskParams().empty()) {
        task.MutableTaskParams()->erase(STAGE_SPAN_ID_PARAM);
    }
}

NWilson::TTraceId GetTaskTraceParent(const NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& parent) {
    const auto it = task.GetTaskParams().find(STAGE_SPAN_ID_PARAM);
    ui64 spanId = 0;
    if (!parent || !parent.GetTimeToLive()
            || parent.GetVerbosity() < TComponentTracingLevels::TQueryProcessor::Detailed
            || it == task.GetTaskParams().end()
            || !TryFromString(it->second, spanId) || !spanId) {
        return NWilson::TTraceId(parent);
    }
    std::array<ui64, 2> traceId;
    memcpy(traceId.data(), parent.GetTraceIdPtr(), parent.GetTraceIdSize());
    return NWilson::TTraceId(traceId, spanId, parent.GetVerbosity(), parent.GetTimeToLive() - 1,
        parent.IsRetroTrace());
}

} // namespace NKikimr::NKqp
