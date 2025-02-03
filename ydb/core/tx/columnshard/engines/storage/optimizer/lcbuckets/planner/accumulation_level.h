#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TAccumulationLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    const TLevelCounters LevelCounters;

    std::set<TOrderedPortion> Portions;

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        return std::nullopt;
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        return 0;
    }

    virtual ui64 DoGetWeight() const override {
        if (PortionsInfo.GetCount() <= 1) {
            return 0;
        }

        THashSet<ui64> portionIds;
        ui64 affectedRawBytes = 0;
        auto chain =
            NextLevel->GetAffectedPortions(Portions.begin()->GetPortion()->IndexKeyStart(), Portions.rbegin()->GetPortion()->IndexKeyEnd());
        if (chain) {
            auto it = Portions.begin();
            auto itNext = chain->GetPortions().begin();
            while (it != Portions.end() && itNext != chain->GetPortions().end()) {
                const auto& nextLevelPortion = *itNext;
                if (nextLevelPortion->IndexKeyEnd() < it->GetPortion()->IndexKeyStart()) {
                    ++itNext;
                } else if (it->GetPortion()->IndexKeyEnd() < nextLevelPortion->IndexKeyStart()) {
                    ++it;
                } else {
                    if (portionIds.emplace(nextLevelPortion->GetPortionId()).second) {
                        affectedRawBytes += nextLevelPortion->GetTotalRawBytes();
                    }
                    ++itNext;
                }
            }
        }
        const ui64 mb = ((affectedRawBytes + PortionsInfo.GetRawBytes()) >> 20) + 1;
        return 1000.0 * PortionsInfo.GetCount() * PortionsInfo.GetCount() / mb;
    }

    virtual TInstant DoGetWeightExpirationInstant() const override {
        return TInstant::Max();
    }

public:
    TAccumulationLevelPortions(const ui64 levelId, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
        : TBase(levelId, nextLevel)
        , LevelCounters(levelCounters) {
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion(), NDataLocks::ELockCategory::Compaction)) {
                return true;
            }
        }
        return false;
    }

    virtual void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
        for (auto&& i : remove) {
            auto it = Portions.find(i);
            AFL_VERIFY(it != Portions.end());
            AFL_VERIFY(it->GetPortion()->GetPortionId() == i->GetPortionId());
            PortionsInfo.RemovePortion(i);
            Portions.erase(it);
            LevelCounters.Portions->RemovePortion(i);
        }
        for (auto&& i : add) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "add_accum")("portion_id", i->GetPortionId())(
                "blob_size", i->GetTotalBlobBytes());
            AFL_VERIFY(Portions.emplace(i).second);
            PortionsInfo.AddPortion(i);
            LevelCounters.Portions->AddPortion(i);
        }
    }

    virtual TCompactionTaskData DoGetOptimizationTask() const override {
        AFL_VERIFY(Portions.size());
        std::shared_ptr<IPortionsLevel> targetLevel = GetNextLevel();
        AFL_VERIFY(targetLevel);
        TCompactionTaskData result(targetLevel->GetLevelId());
        {
            for (auto&& i : Portions) {
                result.AddCurrentLevelPortion(
                    i.GetPortion(), targetLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd()), true);
                if (!result.CanTakeMore()) {
                    result.SetStopSeparation(i.GetPortion()->IndexKeyStart());
                    break;
                }
            }
        }
        return result;
    }

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        AFL_VERIFY(false);
        NArrow::NMerger::TIntervalPositions result;
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
