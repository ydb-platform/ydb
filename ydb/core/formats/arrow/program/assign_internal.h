#pragma once
#include "abstract.h"
#include "functions.h"

namespace NKikimr::NArrow::NSSA {

class TCalculationProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    YDB_ACCESSOR_DEF(std::optional<ui32>, YqlOperationId);

    std::shared_ptr<IStepFunction> Function;

    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const override;

    TCalculationProcessor(
        std::vector<TColumnChainInfo>&& input, std::vector<TColumnChainInfo>&& output, const std::shared_ptr<IStepFunction>& function)
        : TBase(std::move(input), std::move(output), EProcessorType::Calculation)
        , Function(function) {
    }

public:
    static TConclusion<std::shared_ptr<TCalculationProcessor>> Build(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, 
        const std::shared_ptr<IStepFunction>& function);
};

}   // namespace NKikimr::NArrow::NSSA
