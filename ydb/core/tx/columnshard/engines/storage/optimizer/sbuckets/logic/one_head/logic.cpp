#include "logic.h"
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

std::vector<std::shared_ptr<NKikimr::NOlap::TPortionInfo>> TOneHeadLogic::GetPortionsForMerge(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket, std::vector<NArrow::TReplaceKey>* stopPoints, TInstant* stopInstant) const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    std::vector<NArrow::TReplaceKey> splitKeys;
    ui64 memUsage = 0;
    ui64 txSizeLimit = 0;
    std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> predictor = NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
    {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> currentCompactedPortions;
        bool compactedFinished = false;
        bool finished = false;
        for (auto&& [pk, portions] : bucket.GetPKPortions()) {
            for (auto&& [_, p] : portions.GetStart()) {
                if (p->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                    if (currentCompactedPortions.empty() && compactedFinished) {
                        compactedFinished = false;
                        splitKeys.emplace_back(pk);
                    }
                    AFL_VERIFY(currentCompactedPortions.emplace(p->GetPortionId(), p.GetPortionInfo()).second);
                } else if (now - p->RecordSnapshotMax().GetPlanInstant() < FreshnessCheckDuration) {
                    finished = true;
                    if (stopInstant) {
                        *stopInstant = p->RecordSnapshotMax().GetPlanInstant() + FreshnessCheckDuration;
                    }
                    splitKeys.emplace_back(pk);
                    break;
                }
            }
            if (finished) {
                break;
            }
            for (auto&& [_, p] : portions.GetFinish()) {
                if (p->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                    AFL_VERIFY(currentCompactedPortions.erase(p->GetPortionId()));
                    compactedFinished = currentCompactedPortions.empty();
                } else {
                    result.emplace_back(p.GetPortionInfo());
                    memUsage = predictor->AddPortion(*p.GetPortionInfo());
                    txSizeLimit += p->GetTxVolume();
                }
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
        *stopPoints = splitKeys;
    }
    return result;
}

}
