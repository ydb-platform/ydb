#include "logic.h"
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

static const ui64 compactedDetector = 512 * 1024;

std::vector<std::shared_ptr<NKikimr::NOlap::TPortionInfo>> TTimeSliceLogic::GetPortionsForMerge(const TInstant /*now*/, const ui64 memLimit, 
    const TBucketInfo& bucket) const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    {
        ui64 memUsage = 0;
        ui64 txSizeLimit = 0;
        std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> predictor = NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
        for (auto&& [maxInstant, portions] : bucket.GetSnapshotPortions()) {
            for (auto&& [_, p] : portions) {
                if (p.GetTotalBlobBytes() > compactedDetector) {
                    continue;
                }
                memUsage = predictor->AddPortion(*p.GetPortionInfo());
                txSizeLimit += p->GetTxVolume();
                result.emplace_back(p.GetPortionInfo());
            }
            if (txSizeLimit > TGlobalLimits::TxWriteLimitBytes / 2 && result.size() > 1) {
                break;
            }
            if (memUsage > memLimit && result.size() > 1) {
                break;
            }
        }
    }

    return result;
}

NKikimr::NOlap::NStorageOptimizer::NSBuckets::TCompactionTaskResult TTimeSliceLogic::DoBuildTask(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket) const {
    std::vector<NArrow::TReplaceKey> stopPoints;
    std::vector<std::shared_ptr<TPortionInfo>> portions = GetPortionsForMerge(now, memLimit, bucket);

    std::vector<NArrow::TReplaceKey> splitKeys;
    {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> currentCompactedPortions;
        bool compactedFinished = false;
        for (auto&& [pk, portions] : bucket.GetPKPortions()) {
            for (auto&& [_, p] : portions.GetStart()) {
                if (p.GetTotalBlobBytes() > compactedDetector) {
                    if (currentCompactedPortions.empty() && compactedFinished) {
                        compactedFinished = false;
                        splitKeys.emplace_back(pk);
                    }
                    AFL_VERIFY(currentCompactedPortions.emplace(p->GetPortionId(), p.GetPortionInfo()).second);
                }
            }

            for (auto&& [_, p] : portions.GetFinish()) {
                if (p.GetTotalBlobBytes() > compactedDetector) {
                    AFL_VERIFY(currentCompactedPortions.erase(p->GetPortionId()));
                    compactedFinished = currentCompactedPortions.empty();
                }
            }
        }
    }

    return TCompactionTaskResult(std::move(portions), std::move(splitKeys));
}

NKikimr::NOlap::NStorageOptimizer::NSBuckets::TCalcWeightResult TTimeSliceLogic::DoCalcWeight(const TInstant /*now*/, const TBucketInfo& bucket) const {
    ui64 size = 0;
    ui64 count = 0;
    for (auto&& [maxInstant, portions] : bucket.GetSnapshotPortions()) {
        for (auto&& [_, p] : portions) {
            if (p.GetTotalBlobBytes() > compactedDetector) {
                continue;
            }
            size += p.GetTotalBlobBytes();
            ++count;
        }
    }

    if (size < 32000000 && count < 300) {
        return TCalcWeightResult(0, TInstant::Max());
    }
    const ui64 marker = count * 1000000000;
    if (marker < size || count <= 1) {
        return TCalcWeightResult(0, TInstant::Max());
    } else {
        return TCalcWeightResult(marker - size, TInstant::Max());
    }
}

}
