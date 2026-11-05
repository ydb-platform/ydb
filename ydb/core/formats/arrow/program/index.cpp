#include "execution.h"
#include "index.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<TExecutionResult> TIndexCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto scalarConst = context.GetResources().GetConstantScalarVerified(GetInput().back().GetColumnId());

    auto& source = context.GetDataSource();
    auto conclusion = source.CheckIndex(context, IndexContext, scalarConst);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    auto inputRows = context.GetResources().GetFilter().GetFilteredCount();
    std::optional<ui64> outputRows;
    bool appliedToResourceFilter = false;
    if (conclusion->IsTotalDenyFilter()) {
        const ui64 records = context.GetResources().GetRecordsCountRobustVerified();
        if (!inputRows) {
            inputRows = records;
        }
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildFalseArrayUI8(records), false);
        outputRows = 0;
    } else if (conclusion->IsTotalAllowFilter() || !ApplyToFilterFlag) {
        const ui64 records = context.GetResources().GetRecordsCountRobustVerified();
        if (!inputRows) {
            inputRows = records;
        }
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildTrueArrayUI8(records), false);
        outputRows = inputRows;
    } else {
        if (!inputRows) {
            inputRows = conclusion->GetRecordsCountVerified();
        }
        context.MutableResources().AddFilter(*conclusion);
        appliedToResourceFilter = true;
        outputRows = context.GetResources().GetFilter().GetFilteredCount();
    }
    if (const auto& operation = NScanStats::Current()) {
        operation->Update([&](NScanStats::TStep& record) {
            if (record.GetStepKind() == NScanStats::TStep::IndexCheck) {
                auto* details = record.MutableDetails()->MutableIndexCheck();
                details->SetAppliedToResourceFilter(appliedToResourceFilter);
                if (inputRows) {
                    details->SetInputRows(*inputRows);
                }
                if (outputRows) {
                    details->SetOutputRows(*outputRows);
                }
            }
        });
    }
    return TExecutionResult::Done();
}

}   // namespace NKikimr::NArrow::NSSA
