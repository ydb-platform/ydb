#include "collector.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

void TCollector::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    std::vector<TPortionDataAccessor> accessors;
    auto& portions = request->StartFetching(GetPathId());
    for (auto&& i : portions) {
        auto it = Accessors.find(i->GetPortionId());
        AFL_VERIFY(it != Accessors.end());
        accessors.emplace_back(it->second);
    }
    request->AddData(GetPathId(), std::move(accessors));
}

void TCollector::DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
    for (auto&& i : remove) {
        AFL_VERIFY(Accessors.erase(i));
    }
    for (auto&& i : add) {
        AFL_VERIFY(Accessors.emplace(i.GetPortionInfo().GetPortionId(), i).second);
    }
}

}