#include "manager.h"

#include <algorithm>

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void TLocalManager::DrainQueue() {
    TPositiveControlInteger countToFlight;

    const auto maxPortionsMetadataAsk = NYDBTest::TControllers::GetColumnShardController()->GetLimitForPortionsMetadataAsk();
    while (PortionsAskInFlight + countToFlight < maxPortionsMetadataAsk && !PortionsAsk.empty()) {
        THashMap<TActorId, THashMap<TInternalPathId, TPortionsByConsumer>> portionsToAsk;

        while (PortionsAskInFlight + countToFlight < 1000 && !PortionsAsk.empty()) {
            auto& portionToAsk = PortionsAsk.front();
            auto p = portionToAsk.ExtractPortion();
            auto consumerId = portionToAsk.GetConsumerId();
            auto& owner = portionToAsk.GetOwner();
            PortionsAsk.pop_front();

            auto ownerInfo = PortionOwners.find(owner);
            if (ownerInfo == PortionOwners.end()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "drain_queue")("can't find owner, ignoring", owner);
                continue;
            }

            auto it = ownerInfo->second.RequestsByPortion.find(std::pair{p->GetPathId(), p->GetPortionId()});
            if (it == ownerInfo->second.RequestsByPortion.end()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "drain_queue")("owner hasn't requested portion", owner)("table", p->GetPathId())("portion", p->GetPortionId());
                continue;
            }

            if (!ownerInfo->second.DataAccessors.contains(p->GetPathId())) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "drain_queue")("owner has no accessor to table", owner)("table", p->GetPathId());
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddError(p->GetPathId(), "path id absent");
                    }
                }
                ownerInfo->second.RequestsByPortion.erase(it);
            } else {
                bool toAsk = false;
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        toAsk = true;
                        break;
                    }
                }

                if (!toAsk) {
                    ownerInfo->second.RequestsByPortion.erase(it);
                } else {
                    portionsToAsk[owner][p->GetPathId()].UpsertConsumer(consumerId).AddPortion(p);
                    ++countToFlight;
                }
            }
        }

        for (auto& [owner, portionsByTable] : portionsToAsk) {
            const auto& ownerInfo = PortionOwners.find(owner);
            AFL_VERIFY(ownerInfo != PortionOwners.end());

            for (auto& [tableId, portions] : portionsByTable) {

                auto dataAccessor = ownerInfo->second.DataAccessors.find(tableId);
                AFL_VERIFY(dataAccessor != ownerInfo->second.DataAccessors.end());

                auto dataAnalyzed = dataAccessor->second->AnalyzeData(portions);

                for (auto&& accessor : dataAnalyzed.GetCachedAccessors()) {
                    auto it = ownerInfo->second.RequestsByPortion.find(std::pair{accessor.GetPortionInfo().GetPathId(), accessor.GetPortionInfo().GetPortionId()});
                    AFL_VERIFY(it != ownerInfo->second.RequestsByPortion.end());
                    for (auto&& i : it->second) {
                        Counters.ResultFromCache->Add(1);
                        if (!i->IsFetched() && !i->IsAborted()) {
                            i->AddAccessor(accessor);
                        }
                    }

                    ownerInfo->second.RequestsByPortion.erase(it);
                    --countToFlight;
                }

                if (!dataAnalyzed.GetPortionsToAsk().IsEmpty()) {
                    THashMap<TInternalPathId, TPortionsByConsumer> portionsToAskImpl;
                    Counters.ResultAskDirectly->Add(dataAnalyzed.GetPortionsToAsk().GetPortionsCount());
                    portionsToAskImpl.emplace(tableId, dataAnalyzed.DetachPortionsToAsk());
                    dataAccessor->second->AskData(std::move(portionsToAskImpl), ownerInfo->second.AccessorCallback);
                }
            }
        }
    }

    PortionsAskInFlight.Add(countToFlight);
    Counters.FetchingCount->Set(PortionsAskInFlight);
    Counters.QueueSize->Set(PortionsAsk.size());
}

void TLocalManager::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request, const TActorId& owner) {
    auto ownerInfo = PortionOwners.find(owner);
    if (ownerInfo == PortionOwners.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "owner_not_found")("owner", owner);
        return;
    }

    for (auto&& tableId : request->GetPathIds()) {
        auto dataAccessor = ownerInfo->second.DataAccessors.find(tableId);
        if (dataAccessor == ownerInfo->second.DataAccessors.end()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "data_accessor_not_fount")("owner", owner)("tableId", tableId);
            continue;
        }

        auto portions = request->StartFetching(tableId);
        for (auto&& [portionId, portion] : portions) {
            auto requestKey = std::pair{tableId, portionId};
            auto itRequest = ownerInfo->second.RequestsByPortion.find(requestKey);

            if (itRequest == ownerInfo->second.RequestsByPortion.end()) {
                ownerInfo->second.RequestsByPortion[requestKey].push_back(request);
                PortionsAsk.emplace_back(portion, request->GetAbortionFlag(), request->GetConsumer(), owner);
                Counters.AskNew->Add(1);
            } else {
                itRequest->second.push_back(request);
                Counters.AskDuplication->Add(1);
            }
        }
    }

    DrainQueue();
}

void TLocalManager::DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update, const TActorId& owner) {
    auto ownerInfo = PortionOwners.find(owner);

    if (update) {
        AFL_VERIFY(ownerInfo != PortionOwners.end());
        auto it = ownerInfo->second.DataAccessors.find(controller->GetPathId());
        AFL_VERIFY(it != ownerInfo->second.DataAccessors.end());
        controller->SetCache(MetadataCache);
        controller->SetOwner(owner);
        ownerInfo->second.DataAccessors[controller->GetPathId()] = std::move(controller);
    } else {
        if (ownerInfo == PortionOwners.end()) {
            ownerInfo = PortionOwners.emplace(std::pair{owner, std::make_shared<TCallbackWrapper>(AccessorCallback, owner)}).first;
        }
        controller->SetCache(MetadataCache);
        controller->SetOwner(owner);
        ownerInfo->second.DataAccessors[controller->GetPathId()] = std::move(controller);
    }

}

void TLocalManager::DoUnregisterController(const TInternalPathId tableId, const TActorId& owner) {
    auto ownerInfo = PortionOwners.find(owner);
    AFL_VERIFY(ownerInfo != PortionOwners.end());
    AFL_VERIFY(ownerInfo->second.DataAccessors.erase(tableId));
}

 void TLocalManager::DoRemovePortion(const TPortionInfo::TConstPtr& portionInfo, const TActorId& owner) {
    auto ownerInfo = PortionOwners.find(owner);
    if (ownerInfo == PortionOwners.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "owner_not_found")("owner", owner);
        return;
    }
    auto it = ownerInfo->second.DataAccessors.find(portionInfo->GetPathId());
    if (it == ownerInfo->second.DataAccessors.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "data_accessor_not_fount")("owner", owner)("tableId", portionInfo->GetPathId());
        return;
    }
    it->second->ModifyPortions({}, {portionInfo->GetPortionId()});
}

void TLocalManager::DoClearCache(const TActorId& owner) {
    for (auto it = MetadataCache->Begin(); it != MetadataCache->End(); ) {
        if (std::get<0>(it.Key()) == owner) {
            auto current = it;
            ++it;
            MetadataCache->Erase(current);
        }
        else {
            ++it;
        }
    }
    if (!PortionOwners.erase(owner)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "clear_cache_owner_not_found")("owner", owner);
    }
}

void TLocalManager::DoAddPortion(const TPortionDataAccessor& accessor, const TActorId& owner) {
    const auto pathId = accessor.GetPortionInfo().GetPathId();
    const auto portionId = accessor.GetPortionInfo().GetPortionId();

    auto ownerInfo = PortionOwners.find(owner);
    if (ownerInfo == PortionOwners.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "owner_not_found")("owner", owner);
        return;
    }

    auto it = ownerInfo->second.DataAccessors.find(pathId);
    if (it == ownerInfo->second.DataAccessors.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_cache", "data_accessor_not_fount")("owner", owner)("tableId", pathId);
        return;
    }

    it->second->ModifyPortions({accessor}, {});


    auto portionKey = std::pair{pathId, portionId};
    auto request = ownerInfo->second.RequestsByPortion.find(portionKey);

    if (request != ownerInfo->second.RequestsByPortion.end()) {
        for (auto&& i : request->second) {
            if (!i->IsFetched() && !i->IsAborted()) {
                i->AddAccessor(accessor);
            }
        }
        --PortionsAskInFlight;
    }
    ownerInfo->second.RequestsByPortion.erase(request);

    DrainQueue();
}

} // namespace NKikimr::NOlap::NDataAccessorControl
