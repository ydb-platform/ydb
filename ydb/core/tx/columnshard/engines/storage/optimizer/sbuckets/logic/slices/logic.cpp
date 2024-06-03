#include "logic.h"
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

std::vector<std::shared_ptr<NKikimr::NOlap::TPortionInfo>> TTimeSliceLogic::GetPortionsForMerge(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket, std::vector<NArrow::TReplaceKey>* stopPoints, TInstant* stopInstant) const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    {
        ui64 memUsage = 0;
        ui64 txSizeLimit = 0;
        if (stopInstant) {
            *stopInstant = TInstant::Max();
        }
        std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> predictor = NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
        for (auto&& [maxInstant, portions] : bucket.GetSnapshotPortions()) {
            if (now - maxInstant < GetCommonFreshnessCheckDuration()) {
                if (stopInstant) {
                    *stopInstant = maxInstant;
                }
                break;
            }
            for (auto&& [_, p] : portions) {
                if (p->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                    continue;
                }
                memUsage = predictor->AddPortion(*p);
                txSizeLimit += p->GetTxVolume();
                result.emplace_back(p);
            }
            if (txSizeLimit > TGlobalLimits::TxWriteLimitBytes / 2 && result.size() > 1) {
                break;
            }
            if (memUsage > memLimit && result.size() > 1) {
                break;
            }
        }
    }

    if (stopPoints) {
        std::vector<NArrow::TReplaceKey> splitKeys;
        {
            THashMap<ui64, std::shared_ptr<TPortionInfo>> currentCompactedPortions;
            bool compactedFinished = false;
            for (auto&& [pk, portions] : bucket.GetPKPortions()) {
                for (auto&& [_, p] : portions.GetStart()) {
                    if (p->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                        if (currentCompactedPortions.empty() && compactedFinished) {
                            compactedFinished = false;
                            splitKeys.emplace_back(pk);
                        }
                        AFL_VERIFY(currentCompactedPortions.emplace(p->GetPortionId(), p.GetPortionInfo()).second);
                    }
                }

                for (auto&& [_, p] : portions.GetFinish()) {
                    if (p->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                        AFL_VERIFY(currentCompactedPortions.erase(p->GetPortionId()));
                        compactedFinished = currentCompactedPortions.empty();
                    }
                }
            }
        }
        *stopPoints = splitKeys;
    }
    return result;
}

}
