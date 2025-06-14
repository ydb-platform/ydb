#include "fetcher.h"

namespace NKikimr::NOlap::NDataFetcher {

void TPortionsDataFetcher::StartColumnsFetching(TRequestInput&& input, std::shared_ptr<NReader::NCommon::TColumnsSetIds>& entityIds,
    std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
    const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    std::shared_ptr<TScript> script = []() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskDataResourceStep>(entityIds));
        steps.emplace_back(std::make_shared<TAskDataStep>(entityIds));
        return std::make_shared<TScript>(std::move(steps), "PARTIAL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume();
}

void TPortionsDataFetcher::StartFullPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
    const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    static const std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskDataResourceStep>());
        steps.emplace_back(std::make_shared<TAskDataStep>());
        return std::make_shared<TScript>(std::move(steps), "FULL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume();
}

}   // namespace NKikimr::NOlap::NDataFetcher
