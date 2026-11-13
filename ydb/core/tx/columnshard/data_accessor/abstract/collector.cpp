#include "collector.h"

#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap::NDataAccessorControl {

std::vector<TPortionInfo::TConstPtr> TConsumerPortions::GetPortions(const TGranuleMeta& granule) const {
    std::vector<TPortionInfo::TConstPtr> result;
    result.reserve(PortionIds.size());
    for (auto&& i : PortionIds) {
        auto portion = granule.GetPortionOptional(i, false);
        if (portion) {
            result.emplace_back(portion);
        }
    }
    return result;
}

void TConsumerPortions::AddPortion(const std::shared_ptr<const TPortionInfo>& p) {
    PortionIds.emplace_back(p->GetPortionId());
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
