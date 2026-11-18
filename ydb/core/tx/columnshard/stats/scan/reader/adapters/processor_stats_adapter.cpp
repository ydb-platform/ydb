#include "processor_stats_adapter.h"

#include <ydb/core/formats/arrow/program/aggr_keys.h>
#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

TString ColumnName(ui32 columnId, const THashMap<ui32, TString>& names) {
    if (const auto it = names.find(columnId); it != names.end()) {
        return it->second;
    }
    return TStringBuilder() << "ssa_" << columnId;
}

template <class TDetails>
void Columns(const NArrow::NSSA::IResourceProcessor& processor, TDetails& details, const THashMap<ui32, TString>& names) {
    for (const auto& column : processor.GetInput()) {
        details.AddInputColumns(ColumnName(column.GetColumnId(), names));
    }
    for (const auto& column : processor.GetOutput()) {
        details.AddOutputColumns(ColumnName(column.GetColumnId(), names));
    }
}

TString FunctionName(const NArrow::NSSA::IStepFunction& function) {
    if (const auto* internal = dynamic_cast<const NArrow::NSSA::TInternalFunction*>(&function)) {
        const auto names = internal->GetFunctionNames();
        if (!names.empty()) {
            return TString(names.front().data(), names.front().size());
        }
    } else if (const auto* kernel = dynamic_cast<const NArrow::NSSA::TKernelFunction*>(&function)) {
        return kernel->GetFunctionName();
    }
    return "unknown";
}

bool HasConfiguredKernel(const NArrow::NSSA::TCalculationProcessor& processor) {
    const auto& kernel = processor.GetKernelLogic();
    if (!kernel) {
        return false;
    }
    const auto* simple = dynamic_cast<const NArrow::NSSA::TSimpleKernelLogic*>(kernel.get());
    return !simple || simple->GetYqlOperationId().has_value();
}

TString ConfiguredOperationName(const NArrow::NSSA::TCalculationProcessor& processor) {
    if (!HasConfiguredKernel(processor)) {
        return FunctionName(*processor.GetFunction());
    }
    const auto& kernel = *processor.GetKernelLogic();
    if (kernel.GetYqlOperationId()) {
        return ToString(static_cast<NYql::TKernelRequestBuilder::EBinaryOp>(*kernel.GetYqlOperationId()));
    }
    return kernel.GetClassName();
}

TString Expression(const TString& function, const std::vector<NArrow::NSSA::TColumnChainInfo>& input, const THashMap<ui32, TString>& labels) {
    TStringBuilder result;
    result << function << "(";
    bool first = true;
    for (const auto& column : input) {
        if (!first) {
            result << ", ";
        }
        first = false;
        if (const auto it = labels.find(column.GetColumnId()); it != labels.end()) {
            result << it->second;
        } else {
            result << "?column#" << column.GetColumnId();
        }
    }
    return result << ")";
}

}   // namespace

NArrow::NSSA::EProcessorType TProcessorStatsAdapter::GetObservedType() const {
    const auto& processor = Processor;
    if (processor.GetProcessorType() == NArrow::NSSA::EProcessorType::Calculation &&
        !HasConfiguredKernel(static_cast<const NArrow::NSSA::TCalculationProcessor&>(processor)) &&
        static_cast<const NArrow::NSSA::TCalculationProcessor&>(processor).GetFunction()->IsAggregation()) {
        return NArrow::NSSA::EProcessorType::Aggregation;
    }
    return processor.GetProcessorType();
}

THashMap<ui32, TString> TProcessorStatsAdapter::DescribeExpressions(const THashMap<ui32, TString>& inputExpressions) const {
    const auto& processor = Processor;
    if (processor.GetOutput().empty()) {
        return {};
    }
    const auto outputId = processor.GetOutput().front().GetColumnId();
    switch (processor.GetProcessorType()) {
        case NArrow::NSSA::EProcessorType::Calculation:
            return { { outputId, Expression(ConfiguredOperationName(static_cast<const NArrow::NSSA::TCalculationProcessor&>(processor)),
                                     processor.GetInput(), inputExpressions) } };
        case NArrow::NSSA::EProcessorType::Aggregation: {
            THashMap<ui32, TString> result;
            for (const auto& option :
                static_cast<const NArrow::NSSA::NAggregation::TWithKeysAggregationProcessor&>(processor).GetAggregations()) {
                result.emplace(
                    option.GetOutput().GetColumnId(), Expression(ToString(option.GetAggregationId()), option.GetInputs(), inputExpressions));
            }
            return result;
        }
        case NArrow::NSSA::EProcessorType::Const: {
            const auto value = static_cast<const NArrow::NSSA::TConstProcessor&>(processor).GetScalarConstant()->ToString();
            return { { outputId, TStringBuilder() << "literal(" << TString(value.data(), value.size()) << ")" } };
        }
        case NArrow::NSSA::EProcessorType::StreamLogic:
            return { { outputId, Expression(ToString(static_cast<const NArrow::NSSA::TStreamLogicProcessor&>(processor).GetOperation()),
                                     processor.GetInput(), inputExpressions) } };
        case NArrow::NSSA::EProcessorType::CheckIndexData:
            return { { outputId, Expression("index_check", processor.GetInput(), inputExpressions) } };
        case NArrow::NSSA::EProcessorType::CheckHeaderData:
            return { { outputId, Expression("header_check", processor.GetInput(), inputExpressions) } };
        default:
            return {};
    }
}

std::optional<NColumnShardStats::TOperation> TProcessorStatsAdapter::DescribeOperation(
    ui64 nodeId, const THashMap<ui32, TString>& names, const THashMap<ui32, TString>& expressions) const {
    using NArrow::NSSA::EProcessorType;
    NColumnShardStats::TOperation result;
    switch (GetObservedType()) {
        case EProcessorType::Calculation: {
            result.SetName("Calculation");
            auto& details = *result.MutableCalculation();
            const auto ownExpressions = DescribeExpressions(names);
            for (const auto& output : Processor.GetOutput()) {
                if (const auto it = ownExpressions.find(output.GetColumnId()); it != ownExpressions.end()) {
                    details.AddExpressions(it->second);
                }
            }
            Columns(Processor, details, names);
            break;
        }
        case EProcessorType::Aggregation: {
            result.SetName("Aggregation");
            auto& details = *result.MutableAggregation();
            details.SetKind(NColumnShardStats::PROGRAM_AGGREGATION);
            if (Processor.GetProcessorType() == EProcessorType::Aggregation) {
                for (const auto& key :
                    static_cast<const NArrow::NSSA::NAggregation::TWithKeysAggregationProcessor&>(Processor).GetAggregationKeys()) {
                    details.AddKeys(ColumnName(key.GetColumnId(), names));
                }
            }
            const auto ownExpressions = DescribeExpressions(names);
            for (const auto& output : Processor.GetOutput()) {
                if (const auto it = ownExpressions.find(output.GetColumnId()); it != ownExpressions.end()) {
                    details.AddExpressions(it->second);
                }
            }
            Columns(Processor, details, names);
            break;
        }
        case EProcessorType::Filter:
            result.SetName("Filter");
            result.MutableFilter()->SetKind(NColumnShardStats::PREDICATE);
            if (!Processor.GetInput().empty()) {
                const auto columnId = Processor.GetInput().front().GetColumnId();
                if (const auto it = expressions.find(columnId); it != expressions.end()) {
                    result.MutableFilter()->SetPredicate(it->second);
                } else {
                    result.MutableFilter()->SetPredicate(ColumnName(columnId, names));
                }
            }
            break;
        case EProcessorType::Projection:
            result.SetName("Projection");
            result.MutableProjection();
            for (const auto& column : Processor.GetInput()) {
                result.MutableProjection()->AddColumns(ColumnName(column.GetColumnId(), names));
            }
            break;
        default:
            return std::nullopt;
    }
    result.SetKey(TStringBuilder() << "ssa/" << nodeId);
    return result;
}

}   // namespace NKikimr::NOlap::NStats::NScan
