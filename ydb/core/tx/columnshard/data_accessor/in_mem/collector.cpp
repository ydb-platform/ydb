#include "collector.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

THashMap<ui64, TPortionDataAccessor> TCollector::DoAskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& /*callback*/, const TString& /*consumer*/) {
    THashMap<ui64, TPortionDataAccessor> accessors;
    for (auto&& i : portions) {
        auto it = Accessors.find(i->GetPortionId());
        AFL_VERIFY(it != Accessors.end());
        accessors.emplace(i->GetPortionId(), it->second);
    }
    return accessors;
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