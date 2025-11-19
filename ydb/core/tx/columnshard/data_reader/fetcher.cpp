#include "fetcher.h"
#include "fetching_steps.h"

namespace NKikimr::NOlap::NDataFetcher {

void TPortionsDataFetcher::StartAssembledColumnsFetchingNoAllocation(TRequestInput&& input,
    const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& entityIds, std::shared_ptr<IFetchCallback>&& callback,
    const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskDataStep>(entityIds));
        steps.emplace_back(std::make_shared<TAssembleDataStep>());
        return std::make_shared<TScript>(
            std::move(steps), "ASSEMBLED_PARTIAL_PORTIONS_FETCHING_NO_ALLOCATION::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume(fetcher);
}

void TPortionsDataFetcher::StartAssembledColumnsFetching(TRequestInput&& input,
    const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& entityIds, std::shared_ptr<IFetchCallback>&& callback,
    const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskRawDataResourceStep>(entityIds));
        steps.emplace_back(std::make_shared<TAskDataStep>(entityIds));
        steps.emplace_back(std::make_shared<TAssembleDataStep>());
        steps.emplace_back(std::make_shared<TAskUsageResourceStep>(entityIds));
        return std::make_shared<TScript>(std::move(steps), "ASSEMBLED_PARTIAL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume(fetcher);
}

void TPortionsDataFetcher::StartColumnsFetching(TRequestInput&& input, const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& entityIds,
    std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
    const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskBlobDataResourceStep>(entityIds));
        steps.emplace_back(std::make_shared<TAskDataStep>(entityIds));
        steps.emplace_back(std::make_shared<TAskUsageResourceStep>(entityIds));
        return std::make_shared<TScript>(std::move(steps), "PARTIAL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume(fetcher);
}

void TPortionsDataFetcher::StartFullPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
    const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    static const std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskBlobDataResourceStep>(nullptr));
        steps.emplace_back(std::make_shared<TAskDataStep>(nullptr));
        steps.emplace_back(std::make_shared<TAskUsageResourceStep>(nullptr));
        return std::make_shared<TScript>(std::move(steps), "FULL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume(fetcher);
}

void TPortionsDataFetcher::StartAccessorPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
    const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
    static const std::shared_ptr<TScript> script = [&]() {
        std::vector<std::shared_ptr<IFetchingStep>> steps;
        steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
        steps.emplace_back(std::make_shared<TAskAccessorsStep>());
        steps.emplace_back(std::make_shared<TAskUsageResourceStep>(nullptr));
        return std::make_shared<TScript>(std::move(steps), "ACCESSOR_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
    }();
    auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
    fetcher->Resume(fetcher);
}

}   // namespace NKikimr::NOlap::NDataFetcher
