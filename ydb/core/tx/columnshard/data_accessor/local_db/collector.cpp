#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

void TCollector::DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) {
    NActors::TActivationContext::Send(
        TabletActorId, std::make_unique<NDataAccessorControl::TEvAskTabletDataAccessors>(std::move(portions), callback));
}

TDataCategorized TCollector::DoAnalyzeData(const TPortionsByConsumer& /*portions*/) {
    TDataCategorized result;
    AFL_VERIFY(false);
    return result;
}

void TCollector::DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
    for (auto&& i : remove) {
        TPortionDataAccessor result = TPortionDataAccessor::BuildEmpty();
        AccessorsCache->PickOut(std::tuple{Owner, GetPathId(), i}, &result);
    }
    for (auto&& i : add) {
        AccessorsCache->Insert(std::tuple{Owner, GetPathId(), i.GetPortionInfo().GetPortionId()}, i);
    }
}

void TCollector::DoSetCache(std::shared_ptr<TSharedMetadataAccessorCache> cache) {
    AccessorsCache = cache;
}

void TCollector::DoSetOwner(const TActorId& owner) {
    Owner = TActorId(owner);
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
