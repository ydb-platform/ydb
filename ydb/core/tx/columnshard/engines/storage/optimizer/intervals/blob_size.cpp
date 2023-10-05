#include "blob_size.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges> TBlobsWithSizeLimit::BuildMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const {
    if (PortionsSize > (i64)SizeLimitToMerge || PortionsCount > CountLimitToMerge) {
        i64 currentSum = 0;
        std::vector<std::shared_ptr<TPortionInfo>> portions;
        std::optional<TString> tierName;
        for (auto&& i : Portions) {
            for (auto&& c : i.second) {
                if (busyPortions.contains(c.second->GetAddress())) {
                    continue;
                }
                if (c.second->GetMeta().GetTierName() && (!tierName || *tierName < c.second->GetMeta().GetTierName())) {
                    tierName = c.second->GetMeta().GetTierName();
                }
                currentSum += c.second->GetBlobBytes();
                portions.emplace_back(c.second);
                if (currentSum > (i64)32 * 1024 * 1024) {
                    break;
                }
            }
            if (currentSum > (i64)32 * 1024 * 1024) {
                break;
            }
        }
        if (currentSum > SizeLimitToMerge || PortionsCount > CountLimitToMerge) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_with_small")("portions", portions.size())("current_sum", currentSum);
            TSaverContext saverContext(StoragesManager->GetOperator(tierName.value_or(IStoragesManager::DefaultStorageId)), StoragesManager);
            return std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(limits, granule, portions, saverContext);
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_with_small")("skip", "not_enough_data");
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_with_small")("event", "skip_by_condition");
    }
    return nullptr;
}

} // namespace NKikimr::NOlap
