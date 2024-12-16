#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

THashMap<ui64, TPortionDataAccessor> TCollector::DoAskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) {
    THashMap<ui64, TPortionDataAccessor> accessors;
    THashMap<ui64, TPortionInfo::TConstPtr> portionsToDirectAsk;
    for (auto&& p : portions) {
        auto it = AccessorsCache.Find(p->GetPortionId());
        if (it != AccessorsCache.End() && it.Key() == p->GetPortionId()) {
            accessors.emplace(p->GetPortionId(), it.Value());
        } else {
            portionsToDirectAsk.emplace(p->GetPortionId(), p);
        }
    }
    if (portionsToDirectAsk.size()) {
        NActors::TActivationContext::Send(
            TabletActorId, std::make_unique<NDataAccessorControl::TEvAskTabletDataAccessors>(portionsToDirectAsk, callback, consumer));
    }
    return accessors;
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
