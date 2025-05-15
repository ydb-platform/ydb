#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TZeroLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    const TLevelCounters LevelCounters;
    const TDuration DurationToDrop;
    const ui64 ExpectedBlobsSize;
    const ui64 PortionsCountAvailable;
    const std::optional<ui64> PortionsCountLimit;
    const std::optional<ui64> PortionsSizeLimit;
    class TOrderedPortion {
    private:
        YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);

    public:
        TOrderedPortion(const TPortionInfo::TConstPtr& portion)
            : Portion(portion) {
        }

        TOrderedPortion(const TPortionInfo::TPtr& portion)
            : Portion(portion) {
        }

        bool operator==(const TOrderedPortion& item) const {
            return item.Portion->GetPathId() == Portion->GetPathId() && item.Portion->GetPortionId() == Portion->GetPortionId();
        }

        bool operator<(const TOrderedPortion& item) const {
            auto cmp = Portion->IndexKeyStart().CompareNotNull(item.Portion->IndexKeyStart());
            if (cmp == std::partial_ordering::equivalent) {
                return Portion->GetPortionId() < item.Portion->GetPortionId();
            } else {
                return cmp == std::partial_ordering::less;
            }
        }
    };
    std::set<TOrderedPortion> Portions;

    virtual bool IsOverloaded() const override {
        if (PortionsCountLimit && Portions.size() > *PortionsCountLimit) {
            return true;
        }
        if (PortionsSizeLimit && (ui64)PortionsInfo.GetBlobBytes() > (ui64)*PortionsSizeLimit) {
            return true;
        }
        return false;
    }

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        return NArrow::NMerger::TIntervalPositions();
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TSimpleRow& /*from*/, const NArrow::TSimpleRow& /*to*/) const override {
        return std::nullopt;
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TSimpleRow& /*from*/, const NArrow::TSimpleRow& /*to*/) const override {
        return 0;
    }

    virtual void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
        const bool constructionFlag = Portions.empty();
        if (constructionFlag) {
            std::vector<TOrderedPortion> ordered;
            ordered.reserve(add.size());
            for (auto&& i : add) {
                ordered.emplace_back(i);
            }
            std::sort(ordered.begin(), ordered.end());
            AFL_VERIFY(std::unique(ordered.begin(), ordered.end()) == ordered.end());
            Portions = std::set<TOrderedPortion>(ordered.begin(), ordered.end());
        }
        for (auto&& i : add) {
            if (!constructionFlag) {
                AFL_VERIFY(Portions.emplace(i).second);
            }
            PortionsInfo.AddPortion(i);
            LevelCounters.Portions->AddPortion(i);
            i->InitRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized, !NextLevel);
        }
        for (auto&& i : remove) {
            AFL_VERIFY(Portions.erase(i));
            LevelCounters.Portions->RemovePortion(i);
            PortionsInfo.RemovePortion(i);
        }
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion(), NDataLocks::ELockCategory::Compaction)) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetWeight() const override;
    virtual TInstant DoGetWeightExpirationInstant() const override;

    virtual TCompactionTaskData DoGetOptimizationTask() const override;

public:
    TZeroLevelPortions(const ui32 levelIdx, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters,
        const TDuration durationToDrop, const ui64 expectedBlobsSize, const ui64 portionsCountAvailable,
        const std::optional<ui64> portionsCountLimit, const std::optional<ui64> portionsSizeLimit);
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
