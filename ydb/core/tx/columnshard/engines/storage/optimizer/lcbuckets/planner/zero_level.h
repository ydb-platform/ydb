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

    std::shared_ptr<IPortionsLevel> GetTargetLevelVerified() const {
        auto targetLevel = NextLevel;
        if (PortionsInfo.PredictPackedBlobBytes(GetPackKff()) > (1 << 20) && PortionsInfo.GetRawBytes() < (512 << 20)) {
            targetLevel = targetLevel->GetNextLevel();
        }
        AFL_VERIFY(targetLevel);
        return targetLevel;
    }

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        AFL_VERIFY(false);
        return NArrow::NMerger::TIntervalPositions();
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        AFL_VERIFY(false);
        return std::nullopt;
    }

    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (auto&& i : add) {
            AFL_VERIFY(Portions.emplace(i).second);
            PortionsInfo.AddPortion(i);
            LevelCounters.Portions->AddPortion(i);
            i->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
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
    TZeroLevelPortions(const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
        : TBase(0, nextLevel)
        , LevelCounters(levelCounters) {
        AFL_VERIFY(nextLevel);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
