#include "manager.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void TLocalManager::DrainQueue() {
    std::optional<ui64> lastPathId;
    IGranuleDataAccessor* lastDataAccessor = nullptr;
    ui32 countToFlight = 0;
    while (PortionsAskInFlight + countToFlight < NYDBTest::TControllers::GetColumnShardController()->GetLimitForPortionsMetadataAsk() &&
           PortionsAsk.size()) {
        THashMap<ui64, std::vector<TPortionInfo::TConstPtr>> portionsToAsk;
        while (PortionsAskInFlight + countToFlight < 1000 && PortionsAsk.size()) {
            auto p = PortionsAsk.front().ExtractPortion();
            PortionsAsk.pop_front();
            if (!lastPathId || *lastPathId != p->GetPathId()) {
                lastPathId = p->GetPathId();
                auto it = Managers.find(p->GetPathId());
                if (it == Managers.end()) {
                    lastDataAccessor = nullptr;
                } else {
                    lastDataAccessor = it->second.get();
                }
            }
            auto it = RequestsByPortion.find(p->GetPortionId());
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
                    portionsToAsk[p->GetPathId()].emplace_back(p);
                    ++countToFlight;
                }
            }
        }
        for (auto&& i : portionsToAsk) {
            auto it = Managers.find(i.first);
            AFL_VERIFY(it != Managers.end());
            auto dataAnalyzed = it->second->AnalyzeData(i.second, "ANALYZE");
            for (auto&& accessor : dataAnalyzed.GetCachedAccessors()) {
                auto it = RequestsByPortion.find(accessor.GetPortionInfo().GetPortionId());
                AFL_VERIFY(it != RequestsByPortion.end());
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddAccessor(accessor);
                    }
                }
                RequestsByPortion.erase(it);
                AFL_VERIFY(countToFlight);
                --countToFlight;
            }
            if (dataAnalyzed.GetPortionsToAsk().size()) {
                it->second->AskData(dataAnalyzed.GetPortionsToAsk(), AccessorCallback, "ANALYZE");
            }
        }
    }
    PortionsAskInFlight += countToFlight;
}

void TLocalManager::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ask_data")("request", request->DebugString());
    for (auto&& pathId : request->GetPathIds()) {
        auto portions = request->StartFetching(pathId);
        for (auto&& [_, i] : portions) {
            auto itRequest = RequestsByPortion.find(i->GetPortionId());
            if (itRequest == RequestsByPortion.end()) {
                AFL_VERIFY(RequestsByPortion.emplace(i->GetPortionId(), std::vector<std::shared_ptr<TDataAccessorsRequest>>({request})).second);
                PortionsAsk.emplace_back(i, request->GetAbortionFlag());
            } else {
                itRequest->second.emplace_back(request);
            }
        }
    }
    DrainQueue();
}

void TLocalManager::DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) {
    if (update) {
        auto it = Managers.find(controller->GetPathId());
        if (it != Managers.end()) {
            it->second = std::move(controller);
        }
    } else {
        AFL_VERIFY(Managers.emplace(controller->GetPathId(), std::move(controller)).second);
    }
}

void TLocalManager::DoAddPortion(const TPortionDataAccessor& accessor) {
    {
        auto it = Managers.find(accessor.GetPortionInfo().GetPathId());
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({ accessor }, {});
    }
    {
        auto it = RequestsByPortion.find(accessor.GetPortionInfo().GetPortionId());
        if (it != RequestsByPortion.end()) {
            for (auto&& i : it->second) {
                i->AddAccessor(accessor);
            }
            AFL_VERIFY(PortionsAskInFlight);
            --PortionsAskInFlight;
        }
        RequestsByPortion.erase(it);
    }
    DrainQueue();
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
