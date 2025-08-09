#include "assign_internal.h"
#include "execution.h"

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TCalculationProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    if (KernelLogic) {
        auto resultKernel = KernelLogic->Execute(GetInput(), GetOutput(), context.MutableResources());
        if (resultKernel.IsFail()) {
            return resultKernel;
        } else if (*resultKernel) {
            return IResourceProcessor::EExecutionResult::Success;
        } else {
        }
    }
    auto result = Function->Call(GetInput(), context.MutableResources());
    if (result.IsFail()) {
        return result;
    }
    context.MutableResources().AddCalculated(GetOutputColumnIdOnce(), result.DetachResult());
    return IResourceProcessor::EExecutionResult::Success;
}

TConclusion<std::shared_ptr<TCalculationProcessor>> TCalculationProcessor::Build(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output,
    const std::shared_ptr<IStepFunction>& function, const std::shared_ptr<IKernelLogic>& kernelLogic) {
    if (!function) {
        return TConclusionStatus::Fail("null function is impossible for processor construct");
    }

    auto checkStatus = function->CheckIO(input, { output });
    if (checkStatus.IsFail()) {
        return checkStatus;
    }
    std::vector<TColumnChainInfo> outputColumns = { output };
    return std::shared_ptr<TCalculationProcessor>(new TCalculationProcessor(std::move(input), std::move(outputColumns), function, kernelLogic));
}

NJson::TJsonValue TCalculationProcessor::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("kernel", KernelLogic->DebugJson());
    result.InsertValue("function", Function->DebugJson());
    return result;
}

ui64 TCalculationProcessor::DoGetWeight() const {
    return (ui64)KernelLogic->GetWeight();
}

TString TCalculationProcessor::DoGetSignalCategoryName() const {
    return ::ToString(GetProcessorType()) + "::" + KernelLogic->SignalDescription();
}

}   // namespace NKikimr::NArrow::NSSA
