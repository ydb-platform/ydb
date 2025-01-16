#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/events.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void IGranuleDataAccessor::AskData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) {
    AFL_VERIFY(portions.size());
    DoAskData(portions, callback, consumer);
}

TDataCategorized IGranuleDataAccessor::AnalyzeData(
    const std::vector<TPortionInfo::TConstPtr>& portions, const TString& consumer) {
    return DoAnalyzeData(portions, consumer);
}

void TActorAccessorsCallback::OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) {
    NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAddPortion>(std::move(accessors)));
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
