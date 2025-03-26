#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

void TCollector::DoAskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) {
    if (portions.size()) {
        NActors::TActivationContext::Send(
            TabletActorId, std::make_unique<NDataAccessorControl::TEvAskTabletDataAccessors>(portions, callback, consumer));
    }
}

TDataCategorized TCollector::DoAnalyzeData(const std::vector<TPortionInfo::TConstPtr>& portions, const TString& /*consumer*/) {
    TDataCategorized result;
    for (auto&& p : portions) {
        auto it = AccessorsCache.Find(p->GetPortionId());
        if (it != AccessorsCache.End() && it.Key() == p->GetPortionId()) {
            result.AddFromCache(it.Value());
        } else {
            result.AddToAsk(p);
        }
    }
    return result;
}

void TCollector::DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
    for (auto&& i : remove) {
        TPortionDataAccessor result = TPortionDataAccessor::BuildEmpty();
        AccessorsCache.PickOut(i, &result);
    }
    for (auto&& i : add) {
        AccessorsCache.Insert(i.GetPortionInfo().GetPortionId(), i);
    }
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
