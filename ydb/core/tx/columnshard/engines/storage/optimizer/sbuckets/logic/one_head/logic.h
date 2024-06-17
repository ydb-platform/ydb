#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract/logic.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TOneHeadLogic: public IOptimizationLogic {
private:
    const TDuration FreshnessCheckDuration = TDuration::Seconds(300);

    std::vector<std::shared_ptr<TPortionInfo>> GetPortionsForMerge(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket,
        std::vector<NArrow::TReplaceKey>* stopPoints, TInstant* stopInstant) const;

    virtual TCalcWeightResult DoCalcWeight(const TInstant now, const TBucketInfo& bucket) const override {
        TInstant nextInstant = TInstant::Max();
        auto actualPortions = GetPortionsForMerge(now, Max<ui64>(), bucket, nullptr, &nextInstant);
        ui64 size = 0;
        for (auto&& i : actualPortions) {
            size += i->GetTotalBlobBytes();
        }
        const ui64 marker = actualPortions.size() * 1000000000;
        if (marker < size || actualPortions.size() <= 1) {
            return TCalcWeightResult(0, nextInstant);
        } else {
            return TCalcWeightResult(marker - size, nextInstant);
        }
    }

    virtual TCompactionTaskResult DoBuildTask(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket) const override {
        std::vector<NArrow::TReplaceKey> stopPoints;
        std::vector<std::shared_ptr<TPortionInfo>> portions = GetPortionsForMerge(now, memLimit, bucket, &stopPoints, nullptr);
        return TCompactionTaskResult(std::move(portions), std::move(stopPoints));
    }
public:
    TOneHeadLogic(const TDuration freshnessCheckDuration)
        : FreshnessCheckDuration(freshnessCheckDuration)
    {

    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
