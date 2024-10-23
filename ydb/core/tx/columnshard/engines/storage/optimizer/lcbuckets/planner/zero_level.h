#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TZeroLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    const TLevelCounters LevelCounters;
    class TOrderedPortion {
    private:
        YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);

    public:
        TOrderedPortion(const std::shared_ptr<TPortionInfo>& portion)
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

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        return NArrow::NMerger::TIntervalPositions();
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        return std::nullopt;
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        return 0;
    }

    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
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
            if (locksManager->IsLocked(*i.GetPortion())) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetWeight() const override;

    virtual TCompactionTaskData DoGetOptimizationTask() const override;

public:
    TZeroLevelPortions(const ui32 levelIdx, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
        : TBase(levelIdx, nextLevel)
        , LevelCounters(levelCounters) {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
