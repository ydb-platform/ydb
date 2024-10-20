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

    virtual ui64 DoGetWeight() const override {
        if (PortionsInfo.GetCount() <= 1) {
            return 0;
        }

        THashSet<ui64> portionIds;
        TSimplePortionsGroupInfo portionsInfo;
        for (auto&& i : Portions) {
            auto chain = NextLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd());
            if (chain) {
                for (auto&& p : chain->GetPortions()) {
                    if (portionIds.emplace(p->GetPortionId()).second) {
                        portionsInfo.AddPortion(p);
                    }
                }
            }
        }

        const ui64 mb = (portionsInfo.GetRawBytes() + PortionsInfo.GetRawBytes()) / 1000000 + 1;
        return 1000000000.0 * PortionsInfo.GetCount() * PortionsInfo.GetCount() / mb;
    }

public:
    TAccumulationLevelPortions(const ui64 levelId, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
        : TBase(levelId, nextLevel)
        , LevelCounters(levelCounters) {
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion())) {
                return true;
            }
        }
        return false;
    }

    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
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
