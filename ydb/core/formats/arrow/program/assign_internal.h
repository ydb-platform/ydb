#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"

#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA {

class TCalculationProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    YDB_ACCESSOR_DEF(std::optional<ui32>, YqlOperationId);
    YDB_ACCESSOR_DEF(std::shared_ptr<IKernelLogic>, KernelLogic);

    std::shared_ptr<IStepFunction> Function;

    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources, const TProcessorContext& context) const override;

    TCalculationProcessor(std::vector<TColumnChainInfo>&& input, std::vector<TColumnChainInfo>&& output,
        const std::shared_ptr<IStepFunction>& function, const std::shared_ptr<IKernelLogic>& kernelLogic)
        : TBase(std::move(input), std::move(output), EProcessorType::Calculation)
        , KernelLogic(kernelLogic)
        , Function(function) {
    }

    virtual bool IsAggregation() const override {
        return Function->IsAggregation();
    }

    virtual ui64 DoGetWeight() const override {
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

public:
    virtual std::optional<TFetchingInfo> BuildFetchTask(const ui32 columnId, const NAccessor::IChunkedArray::EType arrType,
        const std::shared_ptr<TAccessorsCollection>& resources) const override {
        if (!KernelLogic) {
            return TBase::BuildFetchTask(columnId, arrType, resources);
        }
        return KernelLogic->BuildFetchTask(columnId, arrType, GetInput(), resources);
    }

    static TConclusion<std::shared_ptr<TCalculationProcessor>> Build(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, 
        const std::shared_ptr<IStepFunction>& function, const std::shared_ptr<IKernelLogic>& kernelLogic = nullptr);
};

}   // namespace NKikimr::NArrow::NSSA
