#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

void TCollector::DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) {
    NActors::TActivationContext::Send(
        TabletActorId, std::make_unique<NDataAccessorControl::TEvAskTabletDataAccessors>(std::move(portions), callback, GetTabletId()));
}

TDataCategorized TCollector::DoAnalyzeData(const TPortionsByConsumer& portions) {
    TDataCategorized result;
    for (auto&& c : portions.GetConsumers()) {
        TConsumerPortions* cPortions = nullptr;
        for (auto&& p : c.second.GetPortions()) {
            auto it = AccessorsCache.Find(p->GetPortionId());
            if (it != AccessorsCache.End() && it.Key() == p->GetPortionId()) {
                result.AddFromCache(it.Value());
            } else {
                if (!cPortions) {
                    cPortions = &result.MutablePortionsToAsk().UpsertConsumer(c.first);
                }
                cPortions->AddPortion(p);
            }
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

void TCollector::DoResize(ui64 size) {
    AccessorsCache.SetMaxSize(size);
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
