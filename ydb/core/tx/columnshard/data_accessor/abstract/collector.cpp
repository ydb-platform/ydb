#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {

THashMap<ui64, TPortionDataAccessor> IGranuleDataAccessor::AskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback) {
    AFL_VERIFY(portions.size());
    return DoAskData(portions, callback);
}

void TActorAccessorsCallback::OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) {
    NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAddPortion>(std::move(accessors)));
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
