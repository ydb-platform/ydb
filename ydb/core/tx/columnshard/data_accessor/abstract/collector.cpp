#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void IGranuleDataAccessor::AskData(
    THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) {
    AFL_VERIFY(portions.size());
    DoAskData(std::move(portions), callback);
}

TDataCategorized IGranuleDataAccessor::AnalyzeData(const TPortionsByConsumer& portions) {
    return DoAnalyzeData(portions);
}

void TActorAccessorsCallback::OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors, const TActorId& owner) {
    NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAddPortion>(std::move(accessors), owner));
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
