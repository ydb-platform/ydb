#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void IGranuleDataAccessor::AskData(
    THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) {
    AFL_VERIFY(portions.size());
    DoAskData(std::move(portions), callback);
}

TDataCategorized IGranuleDataAccessor::AnalyzeData(const TPortionsByConsumer& portions) {
    return DoAnalyzeData(portions);
}

std::vector<TPortionInfo::TConstPtr> TConsumerPortions::GetPortions(const TGranuleMeta& granule) const {
    std::vector<TPortionInfo::TConstPtr> result;
    result.reserve(PortionIds.size());
    for (auto&& i : PortionIds) {
        result.emplace_back(granule.GetPortionVerifiedPtr(i, false));
    }
    return result;
}

void TConsumerPortions::AddPortion(const std::shared_ptr<const TPortionInfo>& p) {
    PortionIds.emplace_back(p->GetPortionId());
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
