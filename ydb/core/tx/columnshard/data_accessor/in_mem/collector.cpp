#include "collector.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

void TCollector::DoAskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& /*callback*/, const TString& /*consumer*/) {
    AFL_VERIFY(portions.empty());
}

TDataCategorized TCollector::DoAnalyzeData(const std::vector<TPortionInfo::TConstPtr>& portions, const TString& /*consumer*/) {
    TDataCategorized result;
    for (auto&& i : portions) {
        auto it = Accessors.find(i->GetPortionId());
        AFL_VERIFY(it != Accessors.end());
        result.AddFromCache(it->second);
    }
    return result;
}

void TCollector::DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
    for (auto&& i : remove) {
        AFL_VERIFY(Accessors.erase(i));
    }
    for (auto&& i : add) {
        AFL_VERIFY(Accessors.emplace(i.GetPortionInfo().GetPortionId(), i).second);
    }
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
