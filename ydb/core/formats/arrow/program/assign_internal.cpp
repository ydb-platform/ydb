#include "assign_internal.h"

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

TConclusionStatus TCalculationProcessor::DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const {
    auto result = Function->Call(GetInput(), resources);
    if (result.IsFail()) {
        return result;
    }
    resources->AddVerified(GetOutputColumnIdOnce(), std::move(*result));
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<TCalculationProcessor>> TCalculationProcessor::Build(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, const std::shared_ptr<IStepFunction>& function) {
    if (!function) {
        return TConclusionStatus::Fail("null function is impossible for processor construct");
    }

    auto checkStatus = function->CheckIO(input, { output });
    if (checkStatus.IsFail()) {
        return checkStatus;
    }
    std::vector<TColumnChainInfo> outputColumns = { output };
    return std::shared_ptr<TCalculationProcessor>(new TCalculationProcessor(std::move(input), std::move(outputColumns), function));
}

}   // namespace NKikimr::NArrow::NSSA
