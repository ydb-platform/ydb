#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"

#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA {

class TCalculationProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    YDB_ACCESSOR_DEF(std::shared_ptr<IKernelLogic>, KernelLogic);

    std::shared_ptr<IStepFunction> Function;

    virtual NJson::TJsonValue DoDebugJson() const override;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual TString DoGetSignalCategoryName() const override;

    TCalculationProcessor(std::vector<TColumnChainInfo>&& input, std::vector<TColumnChainInfo>&& output,
        const std::shared_ptr<IStepFunction>& function, const std::shared_ptr<IKernelLogic>& kernelLogic)
        : TBase(std::move(input), std::move(output), EProcessorType::Calculation)
        , KernelLogic(kernelLogic)
        , Function(function) {
        AFL_VERIFY(KernelLogic);
        AFL_VERIFY(Function);
    }

    virtual bool IsAggregation() const override {
        return Function->IsAggregation();
    }

    virtual ui64 DoGetWeight() const override;


    virtual std::shared_ptr<IResourcesAggregator> BuildResultsAggregator() const override {
        return Function->BuildResultsAggregator(GetOutputColumnIdOnce());
    }


public:
    static TConclusion<std::shared_ptr<TCalculationProcessor>> Build(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, 
        const std::shared_ptr<IStepFunction>& function, const std::shared_ptr<IKernelLogic>& kernelLogic);
};

}   // namespace NKikimr::NArrow::NSSA
