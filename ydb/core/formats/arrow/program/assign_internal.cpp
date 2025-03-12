#include "assign_internal.h"
#include "execution.h"

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TCalculationProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    if (KernelLogic) {
        auto resultKernel = KernelLogic->Execute(GetInput(), GetOutput(), context.GetResources());
        if (resultKernel.IsFail()) {
            return resultKernel;
        } else if (*resultKernel) {
            return IResourceProcessor::EExecutionResult::Success;
        } else {
        }
    }
    auto result = Function->Call(GetInput(), context.GetResources());
    if (result.IsFail()) {
        return result;
    }
    context.GetResources()->AddVerified(GetOutputColumnIdOnce(), std::move(*result), false);
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
    if (!!YqlOperationId) {
        result.InsertValue("yql_op", ::ToString((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId));
    }
    if (!!KernelLogic) {
        result.InsertValue("kernel", KernelLogic->GetClassName());
    }
    return result;
}

ui64 TCalculationProcessor::DoGetWeight() const {
    if (KernelLogic) {
        return 0;
    }
    if (!YqlOperationId) {
        return 10;
    } else if ((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId == NYql::TKernelRequestBuilder::EBinaryOp::StartsWith ||
               (NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId == NYql::TKernelRequestBuilder::EBinaryOp::EndsWith) {
        return 7;
    } else if ((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId == NYql::TKernelRequestBuilder::EBinaryOp::StringContains) {
        return 10;
    } else if ((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId == NYql::TKernelRequestBuilder::EBinaryOp::Equals) {
        return 5;
    }
    return 0;
}

TString TCalculationProcessor::DoGetSignalCategoryName() const {
    if (KernelLogic) {
        return ::ToString(GetProcessorType()) + "::" + KernelLogic->GetClassName();
    } else if (YqlOperationId) {
        return ::ToString(GetProcessorType()) + "::" + ::ToString((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId);
    } else {
        return ::ToString(GetProcessorType());
    }
}

}   // namespace NKikimr::NArrow::NSSA
