#include "execution.h"
#include "stream_logic.h"

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TStreamLogicProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    AFL_VERIFY(context.GetResources()->GetAccessorOptional(GetOutputColumnIdOnce()));
    return IResourceProcessor::EExecutionResult::Success;
}

TConclusion<bool> TStreamLogicProcessor::OnInputReady(
    const ui32 inputId, const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto accInput = context.GetResources()->GetAccessorVerified(inputId);
    std::shared_ptr<arrow::Scalar> monoValue;
    AFL_VERIFY(!context.GetResources()->HasMarker(FinishMarker));
    if (auto isMonoValue = accInput->CheckOneValueAccessor(monoValue)) {
        const auto isFalseConclusion = ScalarIsFalse(monoValue);
        if (isFalseConclusion.IsFail()) {
            return isFalseConclusion;
        }
        const auto isTrueConclusion = ScalarIsTrue(monoValue);
        if (isTrueConclusion.IsFail()) {
            return isTrueConclusion;
        }
        if ((Operation == NKernels::EOperation::And && *isFalseConclusion) || (Operation == NKernels::EOperation::Or && *isTrueConclusion)) {
            context.GetResources()->Remove(GetOutputColumnIdOnce(), true);
            context.GetResources()->AddVerified(GetOutputColumnIdOnce(), accInput, false);
            context.GetResources()->AddMarker(FinishMarker);
            return true;
        }
    }
    const auto acc = context.GetResources()->ExtractAccessorOptional(GetOutputColumnIdOnce());
    if (!acc) {
        context.GetResources()->AddVerified(GetOutputColumnIdOnce(), accInput, false);
    } else {
        auto result = Function->Call(TColumnChainInfo::BuildVector({ GetOutputColumnIdOnce(), inputId }), context.GetResources());
        if (result.IsFail()) {
            return result;
        }
        context.GetResources()->AddVerified(GetOutputColumnIdOnce(), std::move(*result), false);
    }
    return false;
}

ui64 TStreamLogicProcessor::DoGetWeight() const {
    return 1;
}

}   // namespace NKikimr::NArrow::NSSA
