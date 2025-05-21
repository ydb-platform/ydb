#include "manager.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void TLocalManager::DrainQueue() {
    std::optional<TManagerKey> lastManagerKey;
    IGranuleDataAccessor* lastDataAccessor = nullptr;
    TPositiveControlInteger countToFlight;
    while (PortionsAskInFlight + countToFlight < NYDBTest::TControllers::GetColumnShardController()->GetLimitForPortionsMetadataAsk() &&
           PortionsAsk.size()) {
        THashMap<TManagerKey, std::vector<TPortionInfo::TConstPtr>> portionsToAsk;
        while (PortionsAskInFlight + countToFlight < 1000 && PortionsAsk.size()) {
            auto [managerKey, portionToAsk] = PortionsAsk.front();
            auto p = portionToAsk.ExtractPortion();
            PortionsAsk.pop_front();
            if (!lastManagerKey || *lastManagerKey != managerKey) {
                lastManagerKey = managerKey;
                auto it = Managers.find(managerKey);
                if (it == Managers.end()) {
                    lastDataAccessor = nullptr;
                } else {
                    lastDataAccessor = it->second.get();
                }
            }
            auto it = RequestsByPortion.find(TUniquePortionId{managerKey, p->GetPortionId()});
            if (it == RequestsByPortion.end()) {
                continue;
            }
            if (!lastDataAccessor) {
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddError(p->GetPathId(), "path id absent");
                    }
                }
                RequestsByPortion.erase(it);
            } else {
                bool toAsk = false;
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        toAsk = true;
                    }
                }
                if (!toAsk) {
                    RequestsByPortion.erase(it);
                } else {
                    portionsToAsk[managerKey].emplace_back(p);
                    ++countToFlight;
                }
            }
        }
        for (auto&& i : portionsToAsk) {
            auto it = Managers.find(i.first);
            AFL_VERIFY(it != Managers.end());
            auto dataAnalyzed = it->second->AnalyzeData(i.second, "ANALYZE");
            for (auto&& accessor : dataAnalyzed.GetCachedAccessors()) {
                auto it = RequestsByPortion.find(TUniquePortionId{i.first, accessor.GetPortionInfo().GetPortionId()});
                AFL_VERIFY(it != RequestsByPortion.end());
                for (auto&& i : it->second) {
                    Counters.ResultFromCache->Add(1);
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddAccessor(accessor);
                    }
                }
                RequestsByPortion.erase(it);
                --countToFlight;
            }
            if (dataAnalyzed.GetPortionsToAsk().size()) {
                Counters.ResultAskDirectly->Add(dataAnalyzed.GetPortionsToAsk().size());
                it->second->AskData(dataAnalyzed.GetPortionsToAsk(), AccessorCallback, "ANALYZE");
            }
        }
    }
    PortionsAskInFlight.Add(countToFlight);
    Counters.FetchingCount->Set(PortionsAskInFlight);
    Counters.QueueSize->Set(PortionsAsk.size());
}

void TLocalManager::DoAskData(const TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ask_data")("request", request->DebugString());
    for (auto&& pathId : request->GetPathIds()) {
        auto managerKey = TManagerKey{tabletId, pathId};
        auto portions = request->StartFetching(pathId);
        for (auto&& [_, i] : portions) {
            auto uniquePortionId = TUniquePortionId{managerKey, i->GetPortionId()};
            auto itRequest = RequestsByPortion.find(uniquePortionId);
            if (itRequest == RequestsByPortion.end()) {
                AFL_VERIFY(RequestsByPortion.emplace(uniquePortionId, std::vector<std::shared_ptr<TDataAccessorsRequest>>({request})).second);
                PortionsAsk.emplace_back(managerKey, TPortionToAsk{i, request->GetAbortionFlag()});
                Counters.AskNew->Add(1);
            } else {
                itRequest->second.emplace_back(request);
                Counters.AskDuplication->Add(1);
            }
        }
    }
    DrainQueue();
}

void TLocalManager::DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) {
    auto managerKey = TManagerKey{controller->GetTabletId(), controller->GetPathId()};
    const auto it = Managers.find(managerKey);
    if (update) {
        if (it != Managers.end()) {
            it->second = std::move(controller);
        }
    } else {
        if (it == Managers.end()) {
            AFL_VERIFY(Managers.emplace(managerKey, std::move(controller)).second);
        }
    }
}

void TLocalManager::DoAddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor) {
    auto managerKey = TManagerKey(tabletId, accessor.GetPortionInfo().GetPathId());
    {
        auto it = Managers.find(managerKey);
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({ accessor }, {});
    }
    {
        auto it = RequestsByPortion.find(TUniquePortionId{managerKey, accessor.GetPortionInfo().GetPortionId()});
        if (it != RequestsByPortion.end()) {
            for (auto&& i : it->second) {
                i->AddAccessor(accessor);
            }
            --PortionsAskInFlight;
        }
        RequestsByPortion.erase(it);
    }
    DrainQueue();
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
