#include "kqp_task_tracing.h"
#include "kqp_trace_settings.h"

#include <yql/essentials/ast/yql_ast.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <array>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {
namespace {

constexpr TStringBuf TaskOperationsParam = "ydb.trace.task_operations";
constexpr char StageSpanIdParam[] = "ydb.trace.stage_span_id";
enum EOperation : ui32 {
    Read = 1 << 0,
    Lookup = 1 << 1,
    Join = 1 << 2,
    Filter = 1 << 3,
    Aggregate = 1 << 4,
    Sort = 1 << 5,
    Write = 1 << 6,
};
constexpr std::array OperationNames = {
    std::pair{Read, "Read"}, std::pair{Lookup, "Lookup"}, std::pair{Join, "Join"},
    std::pair{Filter, "Filter"}, std::pair{Aggregate, "Aggregate"},
    std::pair{Sort, "Sort"}, std::pair{Write, "Write"},
};

ui32 CallableOperation(TStringBuf name) {
    if (name.Contains("Join")) {
        return Join;
    }
    if (name.Contains("Combine") || name.Contains("Aggregate")) {
        return Aggregate;
    }
    if (name.Contains("Filter")) {
        return Filter;
    }
    if (name == "Sort" || name == "Top" || name == "TopSort"
            || name.StartsWith("WideSort") || name.StartsWith("WideTop")
            || name.StartsWith("BlockSort") || name.StartsWith("BlockTop")) {
        return Sort;
    }
    return 0;
}

} // namespace

TTaskTraceDescription TTaskTraceDescription::FromStage(const NKqpProto::TKqpPhyStage& stage) {
    TTaskTraceDescription result;
    if (!stage.GetSources().empty()) {
        result.Operations |= Read;
    }
    for (const auto& input : stage.GetInputs()) {
        if (input.HasStreamLookup()) {
            result.Operations |= Lookup;
        }
    }
    if (!stage.GetSinks().empty() || stage.GetIsEffectsStage()) {
        result.Operations |= Write;
    }
    for (const auto& op : stage.GetTableOps()) {
        switch (op.GetTypeCase()) {
            case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
            case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                result.Operations |= Write;
                break;
            case NKqpProto::TKqpPhyTableOperation::kReadRange:
            case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
            case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                result.Operations |= Read;
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
                    result.Operations |= CallableOperation(head->GetContent());
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
    for (const auto& [bit, name] : OperationNames) {
        if (Operations & bit) {
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
        if (count == NQueryTraceSettings::MaxTaskNameOperations) {
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
    if (!Operations) {
        if (!task.GetTaskParams().empty()) {
            task.MutableTaskParams()->erase(TString(TaskOperationsParam));
        }
        return;
    }
    (*task.MutableTaskParams())[TString(TaskOperationsParam)] = ToString(Operations);
}

void TTaskTraceDescription::Annotate(NWilson::TSpan& span, const NYql::NDqProto::TDqTask& task) {
    if (!span) {
        return;
    }
    TTaskTraceDescription description;
    const auto it = task.GetTaskParams().find(TString(TaskOperationsParam));
    if (it != task.GetTaskParams().end()) {
        TryFromString(it->second, description.Operations);
    }
    span.Name(description.Name());
    span.Attribute("ydb.task.operations", description.OperationsAttribute());
}

void SaveTaskTraceParent(NYql::NDqProto::TDqTask& task, ui64 stageSpanId) {
    if (stageSpanId) {
        (*task.MutableTaskParams())[StageSpanIdParam] = ToString(stageSpanId);
    } else if (!task.GetTaskParams().empty()) {
        task.MutableTaskParams()->erase(StageSpanIdParam);
    }
}

NWilson::TTraceId GetTaskTraceParent(const NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& parent) {
    const auto it = task.GetTaskParams().find(StageSpanIdParam);
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
