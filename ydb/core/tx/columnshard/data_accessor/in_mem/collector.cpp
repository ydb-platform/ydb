#include "collector.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

void TCollector::DoAskData(
    THashMap<TInternalPathId, TPortionsByConsumer>&& /*portions*/, const std::shared_ptr<IAccessorCallback>& /*callback*/) {
    AFL_VERIFY(false);
}

TDataCategorized TCollector::DoAnalyzeData(const TPortionsByConsumer& portions) {
    TDataCategorized result;
    for (auto&& c : portions.GetConsumers()) {
        for (auto&& p : c.second.GetPortions()) {
            auto it = Accessors.find(p->GetPortionId());
            AFL_VERIFY(it != Accessors.end());
            result.AddFromCache(it->second);
        }
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

void TCollector::DoResize([[maybe_unused]] ui64 size) {
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
